defmodule Postgrex.Protocol do
  @moduledoc false

  alias Postgrex.Types
  alias Postgrex.Query
  import Postgrex.Messages
  import Postgrex.BinaryUtils
  require Logger

  @timeout 5000
  @default_extensions [{Postgrex.Extensions.Binary, nil}, {Postgrex.Extensions.Text, nil}]
  @sock_opts [packet: :raw, mode: :binary, active: false]

  defstruct [sock: nil, backend_key: nil, types: nil, timeout: nil,
             parameters: %{}, postgres: :idle, buffer: nil]

  @type state :: %__MODULE__{sock: {module, any},
                             backend_key: {pos_integer, pos_integer},
                             types: (nil | reference | Postgrex.TypeServer.table),
                             timeout: timeout,
                             parameters: %{binary => binary} | reference,
                             postgres: :idle | :transaction | :naive,
                             buffer: nil | binary | :active_once}
  @type notify :: ((binary, binary) -> any)

  @reserved_prefix "POSTGREX_"
  @reserved_queries ["BEGIN", "COMMIT", "ROLLBACK"]

  @spec connect(Keyword.t) ::
    {:ok, state} | {:error, Postgrex.Error.t}
  def connect(opts) do
    host       = Keyword.fetch!(opts, :hostname) |> to_char_list
    port       = opts[:port] || 5432
    timeout    = opts[:timeout] || @timeout
    sock_opts  = [send_timeout: timeout] ++ (opts[:socket_options] || [])
    custom     = opts[:extensions] || []
    extensions = custom ++ @default_extensions
    ssl?       = opts[:ssl] || false
    types?     = Keyword.fetch!(opts, :types)

    postgres =
      case opts[:transactions] || :naive do
        :naive  -> :naive
        :strict -> :idle
      end

    s = %__MODULE__{timeout: timeout, postgres: postgres}

    types_key = if types?, do: {host, port, Keyword.fetch!(opts, :database), custom}
    status = %{opts: opts, types_key: types_key, types_ref: nil,
               types_table: nil, extensions: extensions, extension_info: nil}
    case connect(host, port, sock_opts ++ @sock_opts, s) do
      {:ok, s} when ssl?  -> s |> ssl(status) |> connected()
      {:ok, s}            -> s |> startup(status) |> connected()
      {:error, _} = error -> error
    end
  end

  defp connected({:error, _} = err), do: err
  defp connected({:ok, %{parameters: parameters} = s}) do
    ref = Postgrex.Parameters.insert(parameters)
    {:ok, %{s | parameters: ref}}
  end
  defp connected({:disconnect, err, s}) do
    disconnect(err, s)
    {:error, err}
  end

  @spec disconnect(Exception.t, state) :: :ok
  def disconnect(err, %{types: ref}) when is_reference(ref) do
    # Don't handle the case where connection failure occurs during bootstrap
    # (hard to test and "unlikely" given auth just succeeded)
    raise err
  end
  def disconnect(_, s) do
    sock_close(s)
    _ = recv_buffer(s)
    delete_parameters(s)
    :ok
  end

  @spec ping(state) ::
    {:ok, state} | {:disconnect, Postgrex.Error.t, state}
  def ping(%{buffer: buffer} = s) do
    status = %{notify: notify([]), sync: :sync}
    sync(%{s | buffer: nil}, status, buffer)
  end

  @spec checkout(state) ::
    {:ok, state} | {:disconnect, Postgrex.Error.t, state}
  def checkout(%{postgres: :transaction} = s) do
    sync_error(s, :transaction)
  end
  def checkout(%{buffer: :active_once} = s) do
    case setopts(s, [active: :false], :active_once) do
      :ok                       -> recv_buffer(s)
      {:disconnect, _, _} = dis -> dis
    end
  end

  @spec checkin(state) ::
  {:ok, state} | {:disconnect, Postgrex.Error.t, state}
  def checkin(%{postgres: :transaction} = s) do
    sync_error(s, :transaction)
  end
  def checkin(%{buffer: buffer} = s) when is_binary(buffer) do
    activate(s, buffer)
  end

  @spec handle_prepare(Postgrex.Query.t, Keyword.t, state) ::
    {:ok, Postgrex.Query.t, state} |
    {:error, ArgumentError.t, state} |
    {:error | :disconnect, Postgrex.Query.t, state}
  def handle_prepare(%Query{name: @reserved_prefix <> _} = query, _, s) do
    reserved_error(query, s)
  end
  def handle_prepare(query, opts, s) do
    case query do
      %Query{param_formats: pfs, encoders: encoders} when is_list(pfs) and is_list(encoders) ->
        handle_prepare(query, :parse, opts, s)
      _ ->
        handle_prepare(query, :parse_describe, opts, s)
    end
  end

  @spec handle_execute(Postgrex.Query.t, list, Keyword.t, state) ::
    {:ok, Postgrex.Result.t, state} |
    {:prepare, state} |
    {:error, ArgumentError.t, state} |
    {:error | :disconnect, Postgrex.Error.t, state}
  def handle_execute(%Query{} = query, params, opts, s) do
    handle_execute(query, params, :sync, opts, s)
  end
  @spec handle_execute(Postgrex.Parameters.t, nil, Keyword.t, state) ::
    {:ok, %{binary => binary}, state} |
    {:error, Postgrex.Errpr.t, state}
  def handle_execute(%Postgrex.Parameters{}, nil, _, s) do
    %{parameters: parameters} = s
    case Postgrex.Parameters.fetch(parameters) do
      {:ok, parameters} ->
        {:ok, parameters, s}
      :error ->
        {:error, %Postgrex.Error{message: "parameters not available"}, s}
    end
  end

  @spec handle_execute_close(Postgrex.Query.t, list, Keyword.t, state) ::
    {:ok, Postgrex.Result.t, state} |
    {:prepare, state} |
    {:error, ArgumentError.t, state} |
    {:error | :disconnect, Postgrex.Error.t, state}
  def handle_execute_close(%Query{name: @reserved_prefix <> _} = query, _, _, s) do
    reserved_error(query, s)
  end
  def handle_execute_close(query, params, opts, s) do
    handle_execute(query, params, :sync_close, opts, s)
  end

  @spec handle_close(Postgrex.Query.t, Keyword.t, state) ::
    {:ok, state} |
    {:error, ArgumentError.t, state} |
    {:error | :disconnect, Postgrex.Error.t, state}
  def handle_close(%Query{name: @reserved_prefix <> _} = query, _, s) do
    reserved_error(query, s)
  end
  def handle_close(query, opts, %{buffer: buffer} = s) do
    status = %{notify: notify(opts)}
    close(%{s | buffer: nil}, status, query, nil, buffer)
  end

  def handle_begin(opts, s) do
    handle_transaction(@reserved_prefix <> "BEGIN", :transaction, opts, s)
  end

  def handle_commit(opts, s) do
    handle_transaction(@reserved_prefix <> "COMMIT", :idle, opts, s)
  end

  def handle_rollback(opts, s) do
    handle_transaction(@reserved_prefix <> "ROLLBACK", :idle, opts, s)
  end

  @spec handle_simple(String.t, Keyword.t, state) ::
    {:ok, Postgrex.Result.t, state} |
    {:error | :disconnect, Postgrex.Error.t, state}
  def handle_simple(statement, opts, %{buffer: buffer} = s) do
    status = %{notify: notify(opts), sync: :sync}
    simple_send(%{s | buffer: nil}, status, statement, buffer)
  end

  @spec handle_info(any, Keyword.t, state) ::
    {:ok, state} | {:error | :disconnect, Postgrex.Error.t}
  def handle_info(msg, opts \\ [], s)

  def handle_info({:tcp, sock, data}, opts, %{sock: {:gen_tcp, sock}} = s) do
    handle_data(s, opts, data)
  end
  def handle_info({:tcp_closed, sock}, _, %{sock: {:gen_tcp, sock}} = s) do
    err = Postgrex.Error.exception(tag: :tcp, action: "async recv", reason: :closed)
    {:disconnect, err, s}
  end
  def handle_info({:tcp_error, sock, reason}, _, %{sock: {:gen_tcp, sock}} = s) do
    err = Postgrex.Error.exception(tag: :tcp, action: "async recv", reason: reason)
    {:disconnect, err, s}
  end
  def handle_info({:ssl, sock, data}, opts, %{sock: {:ssl, sock}} = s) do
    handle_data(s, opts, data)
  end
  def handle_info({:ssl_closed, sock}, _, %{sock: {:ssl, sock}} = s) do
    err = Postgrex.Error.exception(tag: :ssl, action: "async recv", reason: :closed)
    {:disconnect, err, s}
  end
  def handle_info({:ssl_error, sock, reason}, _, %{sock: {:ssl, sock}} = s) do
    err = Postgrex.Error.exception(tag: :ssl, action: "async recv", reason: reason)
    {:disconnect, err, s}
  end
  def handle_info(msg, _, s) do
    Logger.info(fn() -> [inspect(__MODULE__), ?\s, inspect(self()),
      " received unexpected message: " | inspect(msg)]
    end)
    {:ok, s}
  end

  ## connect

  defp connect(host, port, sock_opts, %{timeout: timeout} = s) do
    buffer? = Keyword.has_key?(sock_opts, :buffer)
    case :gen_tcp.connect(host, port, sock_opts ++ @sock_opts, timeout) do
      {:ok, sock} when buffer? ->
        {:ok, %{s | sock: {:gen_tcp, sock}}}
      {:ok, sock} ->
        # A suitable :buffer is only set if :recbuf is included in
        # :socket_options.
        {:ok, [sndbuf: sndbuf, recbuf: recbuf, buffer: buffer]} =
          :inet.getopts(sock, [:sndbuf, :recbuf, :buffer])
        buffer = buffer
          |> max(sndbuf)
          |> max(recbuf)
        :ok = :inet.setopts(sock, [buffer: buffer])
        {:ok, %{s | sock: {:gen_tcp, sock}}}
      {:error, reason} ->
        {:error, Postgrex.Error.exception(tag: :tcp, action: "connect", reason: reason)}
    end
  end

  ## ssl

  defp ssl(s, status) do
    case msg_send(s, msg_ssl_request(), "") do
      :ok              -> ssl_recv(s, status)
      {:error, _} = err -> err
    end
  end

  defp ssl_recv(%{sock: {:gen_tcp, sock}, timeout: timeout} = s, status) do
    case :gen_tcp.recv(sock, 1, timeout) do
      {:ok, <<?S>>} ->
        ssl_connect(s, status)
      {:ok, <<?N>>} ->
        disconnect(s, %Postgrex.Error{message: "ssl not available"}, "")
      {:error, reason} ->
        err = Postgrex.Error.exception(tag: :tcp, action: "recv", reason: reason)
        disconnect(s, err, "")
    end
  end

  defp ssl_connect(%{sock: {:gen_tcp, sock}, timeout: timeout} = s, status) do
    case :ssl.connect(sock, status.opts[:ssl_opts] || [], timeout) do
      {:ok, ssl_sock} ->
        startup(%{s | sock: {:ssl, ssl_sock}}, status)
      {:error, reason} ->
        err = Postgrex.Error.exception(tag: :ssl, action: "connect", reason: reason)
        disconnect(s, err, "")
    end
  end

  ## startup

  defp startup(s, %{opts: opts} = status) do
    params = opts[:parameters] || []
    user = Keyword.fetch!(opts, :username)
    database = Keyword.fetch!(opts, :database)
    msg = msg_startup(params: [user: user, database: database] ++ params)
    case msg_send(s, msg, "") do
      :ok                       -> auth_recv(s, status, <<>>)
      {:disconnect, _, _} = dis -> dis
    end
  end

  ## auth

  defp auth_recv(%{timeout: timeout} = s, status, buffer) do
    case msg_recv(s, timeout, buffer) do
      {:ok, msg_auth(type: :ok), buffer} ->
        init_recv(s, status, buffer)
      {:ok, msg_auth(type: :cleartext), buffer} ->
        auth_cleartext(s, status, buffer)
      {:ok, msg_auth(type: :md5, data: salt), buffer} ->
        auth_md5(s, status, salt, buffer)
      {:ok, msg_error(fields: fields), buffer} ->
        disconnect(s, Postgrex.Error.exception(postgres: fields), buffer)
      {:disconnect, _, _} = dis ->
        dis
    end
  end

  defp auth_cleartext(s, %{opts: opts} = status, buffer) do
    pass = Keyword.fetch!(opts, :password)
    auth_send(s, msg_password(pass: pass), status, buffer)
  end

  defp auth_md5(s, %{opts: opts} = status, salt, buffer) do
    user = Keyword.fetch!(opts, :username)
    pass = Keyword.fetch!(opts, :password)

    digest = :crypto.hash(:md5, [pass, user])
    |> Base.encode16(case: :lower)
    digest = :crypto.hash(:md5, [digest, salt])
    |> Base.encode16(case: :lower)
    auth_send(s, msg_password(pass: ["md5", digest]), status, buffer)
  end

  defp auth_send(s, msg, status, buffer) do
    case msg_send(s, msg, buffer) do
      :ok                       -> auth_recv(s, status, buffer)
      {:disconnect, _, _} = dis -> dis
    end
  end

  ## init

  defp init_recv(%{timeout: timeout} = s, status, buffer) do
    case msg_recv(s, timeout, buffer) do
      {:ok, msg_backend_key(pid: pid, key: key), buffer} ->
        init_recv(%{s | backend_key: {pid, key}}, status, buffer)
      {:ok, msg_ready(), buffer} ->
        bootstrap(s, status, buffer)
      {:ok, msg_error(fields: fields), buffer} ->
        disconnect(s, Postgrex.Error.exception(postgres: fields), buffer)
      {:ok, msg, buffer} ->
        init_recv(handle_msg(s, status, msg), status, buffer)
      {:disconnect, _, _} = dis ->
        dis
    end
  end

  ## bootstrap

  defp bootstrap(s, %{types_key: nil}, buffer) do
    activate(s, buffer)
  end
  defp bootstrap(s, %{types_key: types_key} = status, buffer) do
    case Postgrex.TypeServer.fetch(types_key) do
      {:ok, table} ->
        reserve_send(%{s | types: table}, status, buffer)
      {:lock, ref, table} ->
        status = %{status | types_ref: ref, types_table: table}
        bootstrap_send(%{s | types: ref}, status, buffer)
    end
  end

  defp bootstrap_send(%{parameters: parameters} = s, status, buffer) do
    %{extensions: extensions} = status

    extension_keys = Enum.map(extensions, &elem(&1, 0))
    extension_opts = Types.prepare_extensions(extensions, parameters)
    matchers = Types.extension_matchers(extension_keys, extension_opts)
    version = parameters["server_version"] |> Postgrex.Utils.parse_version
    statement = Types.bootstrap_query(matchers, version)
    msg = msg_query(statement: statement)
    case msg_send(s, msg, buffer) do
      :ok ->
        status = %{status | extension_info: {extension_keys, extension_opts}}
        bootstrap_recv(s, status, buffer)
      {:disconnect, _, _} = dis ->
        dis
    end
  end

  defp bootstrap_recv(%{timeout: timeout} = s, status, buffer) do
    case msg_recv(s, timeout, buffer) do
      {:ok, msg_row_desc(), buffer} ->
        bootstrap_recv(s, status, [], buffer)
      {:ok, msg_error(fields: fields), buffer} ->
        disconnect(s, Postgrex.Error.exception(postgres: fields), buffer)
      {:ok, msg, buffer} ->
        bootstrap_recv(handle_msg(s, status, msg), status, buffer)
      {:disconnect, _, _} = dis ->
        dis
    end
  end

  defp bootstrap_recv(%{timeout: timeout} = s, status, rows, buffer) do
    case msg_recv(s, timeout, buffer) do
      {:ok, msg_data_row(values: values), buffer} ->
        bootstrap_recv(s, status, [values | rows], buffer)
      {:ok, msg_command_complete(), buffer} ->
        bootstrap_types(s, status, rows, buffer)
      {:ok, msg_error(fields: fields), buffer} ->
        disconnect(s, Postgrex.Error.exception(postgres: fields), buffer)
      {:ok, msg, buffer} ->
        bootstrap_recv(handle_msg(s, status, msg), status, rows, buffer)
      {:disconnect, _, _} = dis ->
        dis
    end
  end

  defp bootstrap_types(s, %{types_table: table} = status, rows, buffer) do
    %{types_ref: ref, extension_info: {extension_keys, extension_opts}} = status
    types = Types.build_types(rows)
    Types.associate_extensions_with_types(table, extension_keys, extension_opts, types)
    Postgrex.TypeServer.unlock(ref)
    bootstrap_sync_recv(%{s | types: table}, status, buffer)
  end

  defp bootstrap_sync_recv(%{timeout: timeout} = s, status, buffer) do
    case msg_recv(s, timeout, buffer) do
      {:ok, msg_ready(), buffer} ->
        reserve_send(s, status, buffer)
      {:ok, msg, buffer} ->
        bootstrap_sync_recv(handle_msg(s, status, msg), status, buffer)
      {:disconnect, _, _} = dis ->
        dis
    end
  end

  defp reserve_send(s, status, buffer) do
    case msg_send(s, reserve_msgs() ++ [msg_sync()], buffer) do
      :ok ->
        reserve_recv(s, status, buffer)
      {:disconnect, _, _} = dis ->
        dis
    end
  end

  defp reserve_msgs() do
    for statement <- @reserved_queries do
      name = @reserved_prefix <> statement
      msg_parse(name: name, statement: statement, type_oids: [])
    end
  end

  defp reserve_recv(%{timeout: timeout} = s, status, buffer) do
    case msg_recv(s, timeout, buffer) do
      {:ok, msg_parse_complete(), buffer} ->
        reserve_recv(s, status, buffer)
      {:ok, msg_ready(status: :idle), buffer} ->
        activate(s, buffer)
      {:ok, msg_ready(status: postgres), buffer} ->
        sync_error(s, postgres, buffer)
      {:ok, msg_error(fields: fields), buffer} ->
        disconnect(s, Postgrex.Error.exception(postgres: fields), buffer)
      {:ok, msg, buffer} ->
        reserve_recv(handle_msg(s, status, msg), status, buffer)
      {:disconnect, _, _} = dis ->
        dis
    end
  end

  ## simple

  defp simple_send(s, status, statement, buffer) do
    msg = msg_query(statement: statement)
    case msg_send(s, msg, buffer) do
      :ok                       -> simple_recv(s, status, buffer)
      {:disconnect, _, _} = dis -> dis
    end
  end

  defp simple_recv(%{timeout: timeout} = s, status, buffer) do
    ## simple queries here are only done by Postgrex.Notifications processes
    case msg_recv(s, timeout, buffer) do
      {:ok, msg_command_complete(tag: tag), buffer} ->
        complete(s, status, %Query{}, [], tag, buffer)
      {:ok, msg_error(fields: fields), buffer} ->
        sync_recv(s, status, Postgrex.Error.exception(postgres: fields), buffer)
      {:ok, msg, buffer} ->
        simple_recv(handle_msg(s, status, msg), status, buffer)
      {:disconnect, _, _} = dis ->
        dis
    end
  end

  ## prepare

  defp handle_prepare(query, prepare, opts, %{buffer: buffer} = s) do
    status = %{notify: notify(opts), prepare: prepare, sync: :sync}
    prepare_send(%{s | buffer: nil}, status, query, buffer)
  end

  defp prepare_send(s, %{prepare: prepare} = status, query, buffer) do
    %Query{name: name, statement: statement} = query
    msgs =
     case prepare do
       :parse_describe ->
          [msg_describe(type: :statement, name: name), msg_flush()]
       :parse ->
          [msg_flush()]
      end
    msgs = [msg_parse(name: name, statement: statement, type_oids: []) | msgs]
    case msg_send(s, msgs, buffer) do
      :ok ->
        parse_recv(s, status, query, buffer)
      {:disconnect, _, _} = dis ->
        dis
    end
  end

  defp parse_recv(s, %{prepare: prepare} = status, query, buffer) do
    case msg_recv(s, :infinity, buffer) do
      {:ok, msg_parse_complete(), buffer} when prepare == :parse_describe ->
        describe_recv(s, status, %Query{query | types: s.types}, buffer)
      {:ok, msg_parse_complete(), buffer} when prepare == :parse ->
        ok(s, query, buffer)
      {:ok, msg_error(fields: fields), buffer} ->
        exception = Postgrex.Error.exception(postgres: fields)
        close_sync(s, status, query, exception, buffer)
      {:ok, msg, buffer} ->
        parse_recv(handle_msg(s, status, msg), status, query, buffer)
      {:disconnect, _, _} = dis ->
        dis
    end
  end

  defp describe_recv(s, status, query, buffer) do
    case msg_recv(s, :infinity, buffer) do
      {:ok, msg_no_data(), buffer} ->
        ok(s, query, buffer)
      {:ok, msg_parameter_desc(type_oids: param_oids), buffer} ->
        describe_recv(s, status, %Query{query | encoders: param_oids}, buffer)
      {:ok, msg_row_desc(fields: fields), buffer} ->
        {col_oids, col_names} = columns(fields)
        query = %Query{query | columns: col_names, decoders: col_oids}
        ok(s, query, buffer)
      {:ok, msg_error(fields: fields), buffer} ->
        exception = Postgrex.Error.exception(postgres: fields)
        close_sync(s, status, query, exception, buffer)
      {:ok, msg, buffer} ->
        describe_recv(handle_msg(s, status, msg), status, query, buffer)
      {:disconnect, _, _} = dis ->
        dis
    end
  end

  defp close_sync(s, status, %Query{name: name}, result, buffer) do
    msgs = [
      msg_close(type: :statement, name: name),
      msg_sync() ]
    case msg_send(s, msgs, buffer) do
      :ok                       -> sync_recv(s, status, result, buffer)
      {:disconnect, _, _} = dis -> dis
    end
  end

  ## execute

  defp handle_execute(query, params, sync, opts, s) do
    %{types: types, buffer: buffer} = s
    case query do
      %Query{types: nil} ->
        query_error(s, "query #{inspect query} has not been prepared")
      %Query{param_formats: nil, types: ^types} ->
        query_error(s, "query #{inspect query} has not been described")
      %Query{types: ^types} = query ->
       status = %{notify: notify(opts), sync: sync}
       execute_send(%{s | buffer: nil}, status, query, params, buffer)
      %Query{} = query ->
        query_error(s, "query #{inspect query} has invalid types for the connection")
    end
  end

  defp query_error(s, msg) do
    {:error, ArgumentError.exception(msg), s}
  end

  defp execute_send(s, %{sync: sync} = status, query, params, buffer) do
    %Query{param_formats: pfs, result_formats: rfs, name: name} = query
    msgs =
      case sync do
        :sync_close ->
          [msg_sync(), msg_close(type: :statement, name: name), msg_flush()]
        :sync ->
          [msg_sync()]
      end
    msgs = [
      msg_bind(name_port: "", name_stat: name, param_formats: pfs, params: params, result_formats: rfs),
      msg_execute(name_port: "", max_rows: 0) |
      msgs ]
    case msg_send(s, msgs, buffer) do
      :ok                       -> bind_recv(s, status, query, buffer)
      {:disconnect, _, _} = dis -> dis
    end
  end

  defp bind_recv(s, status, query, buffer) do
    case msg_recv(s, :infinity, buffer) do
      {:ok, msg_bind_complete(), buffer} ->
        execute_recv(s, status, query, buffer)
      {:ok, msg_error(fields: fields), buffer} ->
        bind_error(s, status, fields, buffer)
      {:ok, msg, buffer} ->
        bind_recv(handle_msg(s, status, msg), status, query, buffer)
      {:disconnect, _, _} = dis ->
        dis
    end
  end

  defp bind_error(s, status, fields, buffer) do
    case Postgrex.Error.exception(postgres: fields) do
      %Postgrex.Error{postgres: %{code: :invalid_sql_statement_name}} ->
        sync_recv(s, status, :prepare, buffer)
      err ->
        sync_recv(s, status, err, buffer)
    end
  end

  defp execute_recv(s, status, query, buffer) do
    case msg_recv(s, :infinity, buffer) do
      {:ok, msg_data_row(values: values), buffer} ->
        execute_recv(s, status, query, [values], buffer)
      {:ok, msg_command_complete(tag: tag), buffer} ->
        complete(s, status, query, [], tag, buffer)
      {:ok, msg_empty_query(), buffer} ->
        sync_recv(s, status, %Postgrex.Result{}, buffer)
      {:ok, msg_error(fields: fields), buffer} ->
        sync_recv(s, status, Postgrex.Error.exception(postgres: fields), buffer)
      {:ok, msg, buffer} ->
        execute_recv(handle_msg(s, status, msg), status, query, buffer)
      {:disconnect, _, _} = dis ->
        dis
    end
  end

  defp execute_recv(s, status, query, rows, buffer) do
    case msg_recv(s, :infinity, buffer) do
      {:ok, msg_data_row(values: values), buffer} ->
        execute_recv(s, status, query, [values | rows], buffer)
      {:ok, msg_command_complete(tag: tag), buffer} ->
        complete(s, status, query, rows, tag, buffer)
      {:ok, msg, buffer} ->
        execute_recv(handle_msg(s, status, msg), status, query, buffer)
      {:disconnect, _, _} = dis ->
        dis
    end
  end

  defp complete(s, status, %Query{columns: nil}, [], tag, buffer) do
    {command, nrows} = decode_tag(tag)
    result =  %Postgrex.Result{command: command, num_rows: nrows || 0}
    sync_recv(s, status, result, buffer)
  end
  defp complete(s, status, query, rows, tag, buffer) do
    {command, nrows} = decode_tag(tag)
    %Query{columns: cols} = query
    # Fix for PostgreSQL 8.4 (doesn't include number of selected rows in tag)
    if is_nil(nrows) and command == :select do
      nrows = length(rows)
    end
    result = %Postgrex.Result{command: command, num_rows: nrows || 0,
                              rows: rows, columns: cols}
    sync_recv(s, status, result, buffer)
  end

  ## close

  defp close(s, status, %Query{name: name}, result, buffer) do
    msgs = [
      msg_close(type: :statement, name: name),
      msg_flush() ]
    case msg_send(s, msgs, buffer) do
      :ok               -> close_recv(s, status, result, buffer)
      {:error, _} = err -> err
    end
  end

  defp close_recv(s, status, result, buffer) do
    case msg_recv(s, :infinity, buffer) do
      {:ok, msg_close_complete(), buffer} ->
        ok(s, result, buffer)
      {:ok, msg_error(fields: fields), buffer} ->
        ok(s, Postgrex.Error.exception(postgres: fields), buffer)
      {:ok, msg, buffer} ->
        close_recv(handle_msg(s, status, msg), status, result, buffer)
      {:error, _} = err ->
        err
    end
  end

  ## sync

  defp sync(s, status, buffer) do
    case msg_send(s, msg_sync(), buffer) do
      :ok               -> sync_recv(s, status, nil, buffer)
      {:error, _} = err -> err
    end
  end

  ## transaction

  defp handle_transaction(name, postgres, opts, %{postgres: :naive} = s)
  when postgres != :naive do
    handle_transaction(name, :naive, opts, s)
  end
  defp handle_transaction(name, postgres, opts, %{buffer: buffer} = s) do
    status = %{notify: notify(opts), sync: :sync}
    transaction_send(%{s | buffer: nil}, status, name, postgres, buffer)
  end

  defp transaction_send(s, status, name, postgres, buffer) do
    msgs = [
      msg_bind(name_port: "", name_stat: name, param_formats: [], params: [], result_formats: []),
      msg_execute(name_port: "" , max_rows: 0),
      msg_sync()]
    case msg_send(s, msgs, buffer) do
      :ok ->
        transaction_recv(s, status, postgres, buffer)
      {:disconnect, _, _} = dis ->
        dis
    end
  end

  defp transaction_recv(s, status, postgres, buffer) do
    case msg_recv(s, :infinity, buffer) do
      {:ok, msg_ready(), buffer} when postgres == :naive ->
        ok(s, nil, buffer)
      {:ok, msg_ready(status: ^postgres), buffer} ->
        ok(s, postgres, buffer)
      {:ok, msg_ready(status: postgres), buffer} ->
        sync_error(s, postgres, buffer)
      {:ok, msg_bind_complete(), buffer} ->
        transaction_recv(s, status, postgres, buffer)
      {:ok, msg_command_complete(), buffer} ->
        transaction_recv(s, status, postgres, buffer)
      {:ok, msg_error(fields: fields), buffer} when postgres == :naive ->
        err = Postgrex.Error.exception(postgres: fields)
        sync_recv(s, status, err, buffer)
      {:ok, msg_error(fields: fields), buffer} ->
        err = Postgrex.Error.exception(postgres: fields)
        disconnect(s, err, buffer)
      {:ok, msg, buffer} ->
        transaction_recv(handle_msg(s, status, msg), status, postgres, buffer)
      {:disconnect, _, _} = dis ->
        dis
    end
  end

  ## data

  defp handle_data(s, opts, buffer) do
    data(s, %{notify: notify(opts)}, buffer)
  end

  defp data(%{timeout: timeout} = s, status, buffer) do
    case msg_recv(s, timeout, buffer) do
      {:ok, msg_error(fields: fields), buffer} ->
        disconnect(s, Postgrex.Error.exception(postgres: fields), buffer)
      {:ok, msg, <<>>} ->
        activate(handle_msg(s, status, msg), <<>>)
      {:ok, msg, buffer} ->
        data(handle_msg(s, status, msg), status, buffer)
      {:disconnect, _, _} = dis ->
        dis
    end
  end

  ## helpers

  defp notify(opts) do
    opts[:notify] || fn(_, _) -> :ok end
  end

  defp columns(fields) do
    Enum.map(fields, fn row_field(type_oid: oid, name: name) ->
      {oid, name}
    end) |> :lists.unzip
  end

  defp tag(:gen_tcp), do: :tcp
  defp tag(:ssl), do: :ssl

  defp decode_tag(tag) do
    words = :binary.split(tag, " ", [:global])
    words = Enum.map(words, fn word ->
      case Integer.parse(word) do
        {num, ""} -> num
        :error -> word
      end
    end)

    {command, nums} = Enum.split_while(words, &is_binary(&1))
    command = Enum.join(command, "_") |> String.downcase |> String.to_atom
    {command, List.last(nums)}
  end

  # It is ok to use infinity timeout here if in client process as timer is
  # running.
  defp msg_recv(%{sock: {:gen_tcp, sock}} = s, timeout, :active_once) do
    receive do
      {:tcp, ^sock, buffer} ->
        msg_recv(s, timeout, buffer)
      {:tcp_closed, ^sock} ->
        disconnect(s, :tcp, "async_recv", :closed, :active_once)
      {:tcp_error, ^sock, reason} ->
        disconnect(s, :tcp, "async_recv", reason, :active_once)
    after
      timeout ->
        disconnect(s, :tcp, "async_recv", :timeout, :active_one)
    end
  end
  defp msg_recv(%{sock: {:ssl, sock}} = s, timeout, :active_once) do
    receive do
      {:ssl, ^sock, buffer} ->
        msg_recv(s, timeout, buffer)
      {:ssl_closed, ^sock} ->
        disconnect(s, :ssl, "async_recv", :closed, :active_once)
      {:ssl_error, ^sock, reason} ->
        disconnect(s, :ssl, "async_recv", reason, :active_once)
    after
      timeout ->
        disconnect(s, :ssl, "async_recv", :timeout, :active_once)
    end
  end
  defp msg_recv(s, timeout, buffer) do
    case msg_decode(buffer) do
      {:ok, _, _} = ok -> ok
      {:more, more}    -> msg_recv(s, timeout, buffer, more)
    end
  end

  defp msg_recv(%{sock: {mod, sock}} = s, timeout, buffer, more) do
    case mod.recv(sock, more, timeout) do
      {:ok, data} ->
        msg_recv(s, timeout, buffer <> data)
      {:error, reason} ->
        disconnect(s, tag(mod), "recv", reason, buffer)
    end
  end

  defp msg_decode(bin) when byte_size(bin) < 5 do
    {:more, 0}
  end
  defp msg_decode(<<type :: int8, size :: int32, rest :: binary>>) do
    size = size - 4
    case rest do
      <<body :: binary(size), rest :: binary>> ->
        {:ok, parse(body, type, size), rest}
      _ ->
        {:more, size - byte_size(rest)}
    end
  end

  defp msg_send(s, msgs, buffer) when is_list(msgs) do
    binaries = Enum.reduce(msgs, [], &[&2 | encode_msg(&1)])
    do_send(s, binaries, buffer)
  end

  defp msg_send(s, msg, buffer) do
    do_send(s, encode_msg(msg), buffer)
  end

  defp do_send(%{sock: {mod, sock}} = s, data, buffer) do
    case mod.send(sock, data) do
      :ok ->
        :ok
      {:error, reason} ->
        disconnect(s, tag(mod), "send", reason, buffer)
    end
  end

  defp handle_msg(s, _, msg_parameter(name: name, value: value)) do
    %{parameters: parameters} = s
    # Binaries likely part of much larger binary and only keeping name/value
    # over long term
    name = :binary.copy(name)
    value = :binary.copy(value)
    cond do
      is_reference(parameters) ->
        _ = Postgrex.Parameters.put(parameters, name, value)
        s
      is_map(parameters) ->
        %{s | parameters: Map.put(parameters, name, value)}
    end
  end
  defp handle_msg(s, status, msg_notify(channel: channel, payload: payload)) do
    %{notify: notify} = status
    notify.(channel, payload)
    s
  end
  defp handle_msg(s, _, msg_notice()) do
    # TODO: subscribers
    s
  end

  defp ok(s, %Postgrex.Result{} = res, buffer) do
    {:ok, res, %{s | buffer: buffer}}
  end
  defp ok(s, %Postgrex.Query{} = query, buffer) do
    {:ok, query, %{s | buffer: buffer}}
   end
  defp ok(s, nil, buffer) do
    {:ok, %{s | buffer: buffer}}
  end
  defp ok(s, %Postgrex.Error{} = err, buffer) do
    {:error, err, %{s | buffer: buffer}}
  end
  defp ok(s, :prepare, buffer) do
    {:prepare, %{s | buffer: buffer}}
  end
  defp ok(s, postgres, buffer) when postgres in [:idle, :transaction] do
    {:ok, %{s | postgres: postgres, buffer: buffer}}
  end

  defp disconnect(s, tag, action, reason, buffer) do
    err = Postgrex.Error.exception(tag: tag, action: action, reason: reason)
    disconnect(s, err, buffer)
  end

  defp disconnect(s, err, buffer) do
    {:disconnect, err, %{s | buffer: buffer}}
  end

  defp reserved_error(query, s) do
    err = ArgumentError.exception("query #{inspect query} uses reserved name")
    {:error, err, s}
  end

  # Query has completed so ok to use state timeout as message should either be
  # buffer or in flight. sync_recv/4 used by simple queries so can't use
  # :infinity.
  defp sync_recv(s, %{sync: sync} = status, result, buffer) do
    %{postgres: postgres, timeout: timeout} = s
    case msg_recv(s, timeout, buffer) do
      {:ok, msg_ready(status: :idle), buffer} when postgres == :transaction ->
        sync_error(s, :idle, buffer)
      {:ok, msg_ready(status: :transaction), buffer} when postgres == :idle ->
        sync_error(s, :transaction, buffer)
      {:ok, msg_ready(status: :failed), buffer} when postgres == :idle ->
        sync_error(s, :failed, buffer)
      {:ok, msg_ready(), buffer} when sync == :sync ->
        ok(s, result, buffer)
      {:ok, msg_ready(), buffer} when sync == :sync_close ->
        close_recv(s, status, result, buffer)
      {:ok, msg, buffer} ->
        sync_recv(handle_msg(s, status, msg), status, result, buffer)
      {:disconnect, _, _} = dis ->
        dis
    end
  end

  defp sync_error(s, postgres, buffer) do
    sync_error(%{s | buffer: buffer}, postgres)
  end

  defp sync_error(s, postgres) do
    err = %Postgrex.Error{message: "unexpected postgres status: #{postgres}"}
    {:disconnect, err, s}
  end

  defp recv_buffer(%{sock: {:gen_tcp, sock}} = s) do
    receive do
      {:tcp, ^sock, buffer} ->
        {:ok, %{s | buffer: buffer}}
      {:tcp_closed, ^sock} ->
        disconnect(s, :tcp, "async recv", :closed, "")
      {:tcp_error, ^sock, reason} ->
        disconnect(s, :tcp, "async_recv", reason, "")
    after
      0 ->
        {:ok, %{s | buffer: <<>>}}
    end
  end
  defp recv_buffer(%{sock: {:ssl, sock}} = s) do
    receive do
      {:ssl, ^sock, buffer} ->
        {:ok, %{s | buffer: buffer}}
      {:ssl_closed, ^sock} ->
        disconnect(s, :ssl, "async recv", :closed, "")
      {:ssl_error, ^sock, reason} ->
        disconnect(s, :ssl, "async recv", reason, "")
    after
      0 ->
        {:ok, %{s | buffer: <<>>}}
    end
  end

  ## Fake [active: once] if buffer not empty
  defp activate(s, <<>>) do
    case setopts(s, [active: :once], <<>>) do
      :ok  -> {:ok, %{s | buffer: :active_once}}
      other -> other
    end
  end
  defp activate(%{sock: {mod, sock}} = s, buffer) do
    _ = send(self(), {tag(mod), sock, buffer})
    {:ok, s}
  end

  defp setopts(%{sock: {mod, sock}} = s, opts, buffer) do
    case setopts(mod, sock, opts) do
      :ok ->
        :ok
      {:error, reason} ->
        disconnect(s, tag(mod), "setopts", reason, buffer)
    end
  end

  defp setopts(:gen_tcp, sock, opts), do: :inet.setopts(sock, opts)
  defp setopts(:ssl, sock, opts), do: :ssl.setopts(sock, opts)

  defp sock_close(%{sock: {mod, sock}}), do: mod.close(sock)

  defp delete_parameters(%{parameters: ref}) when is_reference(ref) do
    Postgrex.Parameters.delete(ref)
  end
  defp delete_parameters(_), do: :ok
end
