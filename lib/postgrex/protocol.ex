defmodule Postgrex.Protocol do
  @moduledoc false

  alias Postgrex.Types
  alias Postgrex.Query
  alias Postgrex.Stream
  import Postgrex.Messages
  import Postgrex.BinaryUtils
  require Logger
  @behaviour DBConnection

  @timeout 5000
  @sock_opts [packet: :raw, mode: :binary, active: false]
  @max_packet 64 * 1024 * 1024 # max raw receive length

  defstruct [sock: nil, connection_id: nil, types: nil, null: nil, timeout: nil,
             parameters: %{}, queries: nil, postgres: :idle,
             transactions: :naive, buffer: nil]

  @type state :: %__MODULE__{sock: {module, any},
                             connection_id: pos_integer,
                             types: (nil | reference | Postgrex.TypeServer.table),
                             null: atom,
                             timeout: timeout,
                             parameters: %{binary => binary} | reference,
                             queries: nil | :ets.tid,
                             postgres: :idle | :transaction | :failed,
                             transactions: :strict | :naive,
                             buffer: nil | binary | :active_once}
  @type notify :: ((binary, binary) -> any)

  @reserved_prefix "POSTGREX_"
  @reserved_queries ["BEGIN",
                     "COMMIT",
                     "ROLLBACK",
                     "SAVEPOINT postgrex_savepoint",
                     "RELEASE SAVEPOINT postgrex_savepoint",
                     "ROLLBACK TO SAVEPOINT postgrex_savepoint",
                     "SAVEPOINT postgrex_query",
                     "RELEASE SAVEPOINT postgrex_query",
                     "ROLLBACK TO SAVEPOINT postgrex_query"]

  @spec connect(Keyword.t) ::
    {:ok, state} | {:error, Postgrex.Error.t}
  def connect(opts) do
    host       = Keyword.fetch!(opts, :hostname) |> to_char_list
    port       = opts[:port] || 5432
    timeout    = opts[:timeout] || @timeout
    sock_opts  = [send_timeout: timeout] ++ (opts[:socket_options] || [])
    custom     = opts[:extensions] || []
    decode_bin = opts[:decode_binary] || :copy
    ext_opts   = [decode_binary: decode_bin]
    extensions = custom ++ Postgrex.Utils.default_extensions(ext_opts)
    ssl?       = opts[:ssl] || false
    types?     = Keyword.fetch!(opts, :types)
    null       = opts[:null]

    transactions =
      case opts[:transactions] || :naive do
        :naive  -> :naive
        :strict -> :strict
      end

    prepare =
      case opts[:prepare] || :named do
        :named   -> :named
        :unnamed -> :unnamed
      end

    s = %__MODULE__{timeout: timeout, postgres: :idle,
                    transactions: transactions, null: null}

    types_key = if types?, do: {host, port, Keyword.fetch!(opts, :database), decode_bin, custom}
    status = %{opts: opts, types_key: types_key, types_ref: nil,
               types_table: nil, extensions: extensions, prepare: prepare,
               ssl: ssl?}
    case connect(host, port, sock_opts ++ @sock_opts, s) do
      {:ok, s}            -> handshake(s, status)
      {:error, _} = error -> error
    end
  end

  @spec disconnect(Exception.t, state) :: :ok
  def disconnect(_, s) do
    sock_close(s)
    _ = recv_buffer(s)
    delete_parameters(s)
    queries_delete(s)
    :ok
  end

  @spec ping(state) ::
    {:ok, state} | {:disconnect, Postgrex.Error.t, state}
  def ping(%{postgres: :transaction, transactions: :strict} = s) do
    sync_error(s, :transaction)
  end
  def ping(%{buffer: buffer} = s) do
    status = %{notify: notify([]), mode: :transaction, sync: :sync}
    s = %{s | buffer: nil}
    case buffer do
      :active_once ->
        sync(s, status, :active_once, buffer)
      _ when is_binary(buffer) ->
        sync(s, status, nil, buffer)
    end
  end

  @spec checkout(state) ::
    {:ok, state} | {:disconnect, Postgrex.Error.t, state}
  def checkout(%{postgres: :transaction, transactions: :strict} = s) do
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
  def checkin(%{postgres: :transaction, transactions: :strict} = s) do
    sync_error(s, :transaction)
  end
  def checkin(%{buffer: buffer} = s) when is_binary(buffer) do
    activate(s, buffer)
  end

  @spec handle_prepare(Postgrex.Query.t, Keyword.t, state) ::
    {:ok, Postgrex.Query.t, state} |
    {:error, ArgumentError.t | RuntimeError.t, state} |
    {:error | :disconnect, Postgrex.Query.t, state}
  def handle_prepare(query, _, %{postgres: {_, _}} = s) do
    lock_error(s, :prepare, query)
  end
  def handle_prepare(%Query{name: @reserved_prefix <> _} = query, _, s) do
    reserved_error(query, s)
  end
  def handle_prepare(%Query{types: nil} = query, opts, %{queries: nil, buffer: buffer} = s) do
    status = %{notify: notify(opts), mode: mode(opts), sync: :sync}
    parse_describe_send(%{s | buffer: nil}, status, unnamed(query), buffer)
  end
  def handle_prepare(%Query{types: nil} = query, opts, %{buffer: buffer} = s) do
    case query_member?(s, query) do
      true ->
        status = %{notify: notify(opts), mode: mode(opts), sync: :sync}
        describe_send(%{s | buffer: nil}, status, query, buffer)
      false ->
        status = %{notify: notify(opts), mode: mode(opts), sync: :sync}
        parse_describe_send(%{s | buffer: nil}, status, query, buffer)
    end
  end
  def handle_prepare(%Query{types: types} = query, _, %{types: types} = s) do
    query_error(s, "query #{inspect query} has already been prepared")
  end
  def handle_prepare(%Query{} = query, _, s) do
    query_error(s, "query #{inspect query} has invalid types for the connection")
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

  @spec handle_execute(Postgrex.Stream.t | Postgrex.Query.t, list, Keyword.t, state) ::
    {:ok, Postgrex.Result.t, state} |
    {:error, ArgumentError.t, state} |
    {:error | :disconnect, RuntimeError.t, Postgrex.Error.t, state}
  def handle_execute(req, params, opts, s) do
    %{buffer: buffer} = s
    status = %{notify: notify(opts), mode: mode(opts), sync: :sync}
    action = execute(s, req)
    s = %{s | buffer: nil}
    case action do
      {:bind_execute, query} ->
        bind_execute_send(s, status, query, params, buffer)
      {:bind_copy_in, query} ->
        bind_copy_in_send(s, status, query, params, buffer)
      {:bind_copy_in, stream, query} ->
        bind_copy_in_send(s, status, stream, query, params, buffer)
      {:bind, stream, query} ->
        bind_send(s, status, stream, query, params, buffer)
      {:parse_execute, query} ->
        parse_execute_send(s, status, query, params, buffer)
      {:parse_copy_in, query} ->
        parse_copy_in_send(s, status, query, params, buffer)
      {:parse_copy_in, stream, query} ->
        parse_copy_in_send(s, status, stream, query, params, buffer)
      {:parse_bind, stream, query} ->
        parse_bind_send(s, status, stream, query, params, buffer)
      {:execute, stream} ->
        execute_send(s, status, stream, buffer)
      {:copy_out, stream} ->
        copy_out(s, status, stream, buffer)
      :copy_data ->
        copy_data(s, status, params, buffer)
      {:copy_in_stop, stream, msg} ->
        copy_in_stop(s, status, stream, buffer, msg)
      {kind, _, _} = error when kind in [:error, :disconnect] ->
        error
    end
  end

  @spec handle_close(Postgrex.Query.t | Postgrex.Stream.t, Keyword.t, state) ::
    {:ok, Postgrex.Result.t, state} |
    {:error, ArgumentError.t, state} |
    {:error | :disconnect, RuntimeError.t | Postgrex.Error.t, state}
  def handle_close(%Stream{ref: ref} = stream, _, %{postgres: {_, ref}} = s) do
    msg = "postgresql protocol can not halt copying from database for " <>
      inspect(stream)
    err = RuntimeError.exception(message: msg)
    {:disconnect, err, s}
  end
  def handle_close(query, _, %{postgres: {_, _}} = s) do
    lock_error(s, :close, query)
  end
  def handle_close(%Query{name: @reserved_prefix <> _} = query, _, s) do
    reserved_error(query, s)
  end
  def handle_close(query, opts, s) do
    %{connection_id: connection_id, buffer: buffer} = s
    status = %{notify: notify(opts), mode: mode(opts), sync: :sync}
    res = %Postgrex.Result{command: :close, connection_id: connection_id}
    close(%{s | buffer: nil}, status, query, res, buffer)
  end

  @spec handle_begin(Keyword.t, state) ::
    {:ok, Postgrex.Result.t, state} |
    {:disconnect, RuntimeError.t, state} |
    {:error | :disconnect, Postgrex.Error.t, state}
  def handle_begin(_, %{postgres: {_, _}} = s) do
    lock_error(s, :begin)
  end
  def handle_begin(opts, s) do
    case Keyword.get(opts, :mode, :transaction) do
      :transaction ->
        statement = "BEGIN"
        handle_transaction(statement, :transaction, :begin, opts, s)
      :savepoint   ->
        statement = "SAVEPOINT postgrex_savepoint"
        handle_savepoint([statement, :sync], :savepoint, opts, s)
    end
  end

  @spec handle_commit(Keyword.t, state) ::
    {:ok, Postgrex.Result.t, state} |
    {:disconnect, RuntimeError.t, state} |
    {:error | :disconnect, Postgrex.Error.t, state}
  def handle_commit(_, %{postgres: {_, _}} = s) do
    lock_error(s, :commit)
  end
  def handle_commit(opts, %{postgres: postgres} = s) do
    case Keyword.get(opts, :mode, :transaction) do
      :transaction ->
        statement = "COMMIT"
        handle_transaction(statement, :idle, :commit, opts, s)
      :savepoint when postgres == :failed ->
        handle_rollback(opts, s)
      :savepoint ->
        statement = "RELEASE SAVEPOINT postgrex_savepoint"
        handle_savepoint([statement, :sync], :release, opts, s)
    end
  end

  @spec handle_rollback(Keyword.t, state) ::
    {:ok, Postgrex.Result.t, state} |
    {:disconnect, RuntimeError.t, state} |
    {:error | :disconnect, Postgrex.Error.t, state}
  def handle_rollback(_, %{postgres: {_, _}} = s) do
    lock_error(s, :rollback)
  end
  def handle_rollback(opts, s) do
    case Keyword.get(opts, :mode, :transaction) do
      :transaction ->
        statement = "ROLLBACK"
        handle_transaction(statement, :idle, :rollback, opts, s)
      :savepoint ->
        statements = ["ROLLBACK TO SAVEPOINT postgrex_savepoint",
                      "RELEASE SAVEPOINT postgrex_savepoint",
                      :sync]
        handle_savepoint(statements, [:rollback, :release], opts, s)
    end
  end

  @spec handle_simple(String.t, Keyword.t, state) ::
    {:ok, Postgrex.Result.t, state} |
    {:error | :disconnect, Postgrex.Error.t, state}
  def handle_simple(statement, opts, %{buffer: buffer} = s) do
    status = %{notify: notify(opts), mode: :transaction, sync: :sync}
    simple_send(%{s | buffer: nil}, status, statement, buffer)
  end

  @spec handle_info(any, Keyword.t, state) ::
    {:ok, state} | {:error | :disconnect, Postgrex.Error.t, state}
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

  ## handshake

  defp handshake(%{timeout: timeout, sock: {:gen_tcp, sock}} = s,status) do
    {:ok, timer} = :timer.apply_after(timeout, :gen_tcp, :shutdown,
                                      [sock, :read_write])
    case do_handshake(s, status) do
      {:ok, %{parameters: parameters} = s} ->
        {:ok, _} = :timer.cancel(timer)
        ref = Postgrex.Parameters.insert(parameters)
        {:ok, %{s | parameters: ref}}
      {:disconnect, err, s} ->
        {:ok, _} = :timer.cancel(timer)
        disconnect(err, s)
        {:error, err}
    end
  end

  defp do_handshake(s, %{ssl: true} = status), do: ssl(s, status)
  defp do_handshake(s, %{ssl: false} = status), do: startup(s, status)

  ## ssl

  defp ssl(s, status) do
    case msg_send(s, msg_ssl_request(), "") do
      :ok                       -> ssl_recv(s, status)
      {:disconnect, _, _} = dis -> dis
    end
  end

  defp ssl_recv(%{sock: {:gen_tcp, sock}} = s, status) do
    case :gen_tcp.recv(sock, 1, :infinity) do
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

  defp auth_recv(s, status, buffer) do
    case msg_recv(s, :infinity, buffer) do
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

  defp init_recv(s, status, buffer) do
    case msg_recv(s, :infinity, buffer) do
      {:ok, msg_backend_key(pid: pid), buffer} ->
        init_recv(%{s | connection_id: pid}, status, buffer)
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
      {:lock, ref, table} ->
        bootstrap_send(%{s | types: table}, %{status | types_ref: ref}, buffer)
      {:go, table} ->
        reserve_send(%{s | types: table}, status, buffer)
    end
  end

  defp bootstrap_send(%{parameters: parameters} = s, status, buffer) do
    version = parameters["server_version"] |> Postgrex.Utils.parse_version
    statement = Types.bootstrap_query(version)
    msg = msg_query(statement: statement)
    case msg_send(s, msg, buffer) do
      :ok ->
        bootstrap_recv(s, status, buffer)
      {:disconnect, err, s} ->
        bootstrap_fail(s, err, status)
    end
  end

  defp bootstrap_recv(s, status, buffer) do
    case msg_recv(s, :infinity, buffer) do
      {:ok, msg_row_desc(), buffer} ->
        bootstrap_recv(s, status, [], buffer)
      {:ok, msg_error(fields: fields), buffer} ->
        err = Postgrex.Error.exception(postgres: fields)
        bootstrap_fail(s, err, status, buffer)
      {:ok, msg, buffer} ->
        bootstrap_recv(handle_msg(s, status, msg), status, buffer)
      {:disconnect, err, s} ->
        bootstrap_fail(s, err, status)
    end
  end

  defp bootstrap_recv(s, status, rows, buffer) do
    case msg_recv(s, :infinity, buffer) do
      {:ok, msg_data_row(values: values), buffer} ->
        bootstrap_recv(s, status, [row_decode(values) | rows], buffer)
      {:ok, msg_command_complete(), buffer} ->
        bootstrap_types(s, status, rows, buffer)
      {:ok, msg_error(fields: fields), buffer} ->
        err = Postgrex.Error.exception(postgres: fields)
        bootstrap_fail(s, err, status, buffer)
      {:ok, msg, buffer} ->
        bootstrap_recv(handle_msg(s, status, msg), status, rows, buffer)
      {:disconnect, err, s} ->
        bootstrap_fail(s, err, status)
    end
  end

  defp bootstrap_types(s, status, rows, buffer) do
    %{types: table, parameters: parameters} = s
    %{extensions: extensions, types_ref: ref} = status
    extension_keys = Enum.map(extensions, &elem(&1, 0))
    extension_opts = Types.prepare_extensions(extensions, parameters)
    types = Types.build_types(rows)
    Types.associate_extensions_with_types(table, extension_keys, extension_opts, types)
    Postgrex.TypeServer.unlock(ref)
    bootstrap_sync_recv(s, status, buffer)
  end

  defp bootstrap_sync_recv(s, status, buffer) do
    case msg_recv(s, :infinity, buffer) do
      {:ok, msg_ready(status: :idle), buffer} ->
        reserve_send(s, status, buffer)
      {:ok, msg_ready(status: postgres), buffer} ->
        sync_error(s, postgres, buffer)
      {:ok, msg, buffer} ->
        bootstrap_sync_recv(handle_msg(s, status, msg), status, buffer)
      {:disconnect, _, _} = dis ->
        dis
    end
  end

  defp bootstrap_fail(s, err, %{types_ref: ref}) do
    Postgrex.TypeServer.fail(ref)
    {:disconnect, err, s}
  end

  defp bootstrap_fail(s, err, status, buffer) do
    bootstrap_fail(%{s | buffer: buffer}, err, status)
  end

  defp reserve_send(s, %{prepare: :unnamed}, buffer) do
    activate(s, buffer)
  end
  defp reserve_send(s, %{prepare: :named} = status, buffer) do
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

  defp reserve_recv(s, status, buffer) do
    case msg_recv(s, :infinity, buffer) do
      {:ok, msg_parse_complete(), buffer} ->
        reserve_recv(s, status, buffer)
      {:ok, msg_ready(status: :idle), buffer} ->
        activate(%{s | queries: queries_new()}, buffer)
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
        err = Postgrex.Error.exception(postgres: fields)
        sync_recv(s, status, err, buffer)
      {:ok, msg, buffer} ->
        simple_recv(handle_msg(s, status, msg), status, buffer)
      {:disconnect, _, _} = dis ->
        dis
    end
  end

  ## prepare

  defp parse_describe_send(s, status, query, buffer) do
    %Query{name: name, statement: statement} = query
    msgs =
      [msg_parse(name: name, statement: statement, type_oids: []),
       msg_describe(type: :statement, name: name)]
    describe_recv = &describe_recv/4
    recv = &parse_recv(&1, &2, &3, &4, describe_recv)
    send_and_recv(s, status, query, buffer, msgs, recv)
  end

  defp describe_send(s, status, %Query{name: name} = query, buffer) do
    msgs = [msg_describe(type: :statement, name: name)]
    send_and_recv(s, status, query, buffer, msgs, &describe_recv/4)
  end

  defp parse_recv(s, status, query, buffer, recv \\ &bind_recv/4) do
    case msg_recv(s, :infinity, buffer) do
      {:ok, msg_parse_complete(), buffer} ->
        query_put(s, query)
        recv.(s, status, query, buffer)
      {:ok, msg_error(fields: fields), buffer} ->
        unnamed_query_delete(s, query)
        err = Postgrex.Error.exception(postgres: fields)
        sync_recv(s, status, err, buffer)
      {:ok, msg, buffer} ->
        parse_recv(handle_msg(s, status, msg), status, query, buffer, recv)
      {:disconnect, _, _} = dis ->
        dis
    end
  end

  defp describe_recv(s, status, query, buffer) do
    case msg_recv(s, :infinity, buffer) do
      {:ok, msg_no_data(), buffer} ->
        query = %Query{query | types: s.types, null: s.null}
        sync_recv(s, status, query, buffer)
      {:ok, msg_parameter_desc(type_oids: param_oids), buffer} ->
        describe_recv(s, status, %Query{query | encoders: param_oids}, buffer)
      {:ok, msg_row_desc(fields: fields), buffer} ->
        {col_oids, col_names} = columns(fields)
        query = %Query{query | types: s.types, null: s.null,
                               columns: col_names, decoders: col_oids}
        sync_recv(s, status, query, buffer)
      {:ok, msg_too_many_parameters(len: len, max_len: max), buffer} ->
        msg = "postgresql protocol can not handle #{len} parameters, " <>
          "the maximum is #{max}"
        err = ArgumentError.exception(message: msg)
        {:disconnect, err, %{s | buffer: buffer}}
      {:ok, msg_error(fields: fields), buffer} ->
        sync_recv(s, status, Postgrex.Error.exception(postgres: fields), buffer)
      {:ok, msg, buffer} ->
        describe_recv(handle_msg(s, status, msg), status, query, buffer)
      {:disconnect, _, _} = dis ->
        dis
    end
  end

  ## execute

  defp query_error(s, msg) do
    {:error, ArgumentError.exception(msg), s}
  end

  defp lock_error(s, fun) do
    msg = "connection is locked copying to or from the database and " <>
      "can not #{fun} transaction"
    {:disconnect, RuntimeError.exception(msg), s}
  end

  defp lock_error(s, fun, query) do
    msg = "connection is locked copying to or from the database and " <>
      "can not #{fun} #{inspect query}"
    {:error, RuntimeError.exception(msg), s}
  end

  defp execute(%{postgres: {_, _ref}} = s, %Query{} = query) do
    lock_error(s, :execute, query)
  end
  defp execute(s, %Query{name: @reserved_prefix <> _} = query) do
    reserved_error(query, s)
  end
  defp execute(s, %Query{types: nil} = query) do
    query_error(s, "query #{inspect query} has not been prepared")
  end
  defp execute(%{types: types, queries: nil}, %Query{types: types, copy_data: data?} = query) do
    query = unnamed(query)
    if data? do
      {:parse_copy_in, query}
    else
      {:parse_execute, query}
    end
  end
  defp execute(%{types: types} = s, %Query{types: types, copy_data: data?} = query) do
    prepared? = query_member?(s, query)
    cond do
      prepared? and data? ->
        {:bind_copy_in, query}
      prepared? ->
        {:bind_execute, query}
      data? ->
        {:parse_copy_in, query}
      true ->
        {:parse_execute, query}
    end
  end
  defp execute(s, %Query{} = query) do
    query_error(s, "query #{inspect query} has invalid types for the connection")
  end
  defp execute(%{postgres: {_, ref}}, %Stream{ref: ref, state: state} = stream) do
    case state do
      :copy_out ->
        {:copy_out, stream}
      :copy_done ->
        {:copy_in_stop, stream, msg_copy_done()}
      :copy_fail ->
        msg = "copying to database halted"
        {:copy_in_stop, stream, msg_copy_fail(message: msg)}
    end
  end
  defp execute(s, %Stream{state: state} = stream)
      when state in [:copy_out, :copy_done, :copy_fail] do
    msg = "connection lost lock for copying to or from the database and " <>
      "can not execute #{inspect stream}"
    {:disconnect, RuntimeError.exception(msg), s}
  end
  defp execute(%{postgres: {_, _ref}} = s, %Stream{} = stream) do
    lock_error(s, :execute, stream)
  end
  defp execute(s, %Stream{query: query, state: state} = stream)
      when state in [:out, :suspended] do
    case execute(s, query) do
      {execute, _} when execute in [:bind_execute, :parse_execute] ->
        {:execute, stream}
      {:error, _, _} = error ->
        error
    end
  end
  defp execute(s, %Stream{query: query, state: :bind} = stream) do
    case execute(s, query) do
      {:bind_execute, query} ->
        {:bind, %Stream{stream | query: query}, query}
      {:parse_execute, query} ->
        {:parse_bind, %Stream{stream | query: query}, query}
      {:error, _, _} = error ->
        error
    end
  end
  defp execute(s, %Stream{query: query, state: :copy_in} = stream) do
    case execute(s, query) do
      {:bind_copy_in, query} ->
        {:bind_copy_in, %Stream{stream | query: query}, query}
      {:parse_copy_in, query} ->
        {:parse_copy_in, %Stream{stream | query: query}, query}
      {:error, _, _} = error ->
        error
    end
  end
  defp execute(%{postgres: {_, ref}}, %Postgrex.CopyData{ref: ref}) do
    :copy_data
  end
  defp execute(%{postgres: {_, _ref}} = s, %Postgrex.CopyData{} = copy_data) do
    lock_error(s, :execute, copy_data)
  end

  defp execute_send(s, status, stream, buffer) do
    %Stream{portal: portal, max_rows: max_rows} = stream
    messages = [msg_execute(name_port: portal, max_rows: max_rows)]
    send_and_recv(s, status, stream, buffer, messages, &execute_recv/4)
  end

  defp bind_send(s, status, stream, query, params, buffer) do
    %{connection_id: connection_id} = s
    res = %Postgrex.Result{command: :bind, connection_id: connection_id}
    %Stream{portal: portal} = stream
    %Query{param_formats: pfs, result_formats: rfs, name: name} = query
    messages = [
      msg_bind(name_port: portal, name_stat: name, param_formats: pfs, params: params, result_formats: rfs)]
    sync_recv = &sync_recv/4
    recv = &bind_recv(&1, &2, &3, &4, sync_recv)
    send_and_recv(s, status, res, buffer, messages, recv)
  end

  defp bind_execute_send(s, status, query, params, buffer) do
    %Query{param_formats: pfs, result_formats: rfs, name: name} = query
    msgs = [
      msg_bind(name_port: "", name_stat: name, param_formats: pfs, params: params, result_formats: rfs),
      msg_execute(name_port: "", max_rows: 0)]
    send_and_recv(s, status, query, buffer, msgs, &bind_recv/4)
  end

  defp bind_copy_in_send(s, status, query, params, buffer) do
    {params, [copy_data_msg]} = Enum.split(params, -1)
    %Query{param_formats: pfs, result_formats: rfs, name: name} = query
    msgs = [
      msg_bind(name_port: "", name_stat: name, param_formats: pfs, params: params, result_formats: rfs),
      msg_execute(name_port: "", max_rows: 0),
      copy_data_msg,
      msg_copy_done()]
    copy_in_recv = &copy_in_recv/4
    bind_recv = &bind_recv(&1, &2, &3, &4, copy_in_recv)
    send_and_recv(s, status, query, buffer, msgs, bind_recv)
  end

  defp bind_copy_in_send(s, status, stream, query, params, buffer) do
    %Query{param_formats: pfs, result_formats: rfs, name: name} = query
    msgs = [
      msg_bind(name_port: "", name_stat: name, param_formats: pfs, params: params, result_formats: rfs),
      msg_flush(),
      msg_execute(name_port: "", max_rows: 0)]
    status = %{status | sync: :flush}
    copy_in_ready = &copy_in_ready/4
    bind_recv = &bind_recv(&1, &2, &3, &4, copy_in_ready)
    copy_in_send(s, status, stream, buffer, msgs, bind_recv)
  end

  defp parse_bind_send(s, status, stream, query, params, buffer) do
    %{connection_id: connection_id} = s
    res = %Postgrex.Result{command: :bind, connection_id: connection_id}
    %Stream{portal: portal} = stream
    %Query{param_formats: pfs, result_formats: rfs, name: name, statement: statement} = query
    messages = [
      msg_parse(name: name, statement: statement, type_oids: []),
      msg_bind(name_port: portal, name_stat: name, param_formats: pfs, params: params, result_formats: rfs)]
    sync_recv = &sync_recv/4
    bind_recv = fn(s, status, _query, buffer) ->
      bind_recv(s, status, res, buffer, sync_recv)
    end
    parse_recv = &parse_recv(&1, &2, &3, &4, bind_recv)
    send_and_recv(s, status, query, buffer, messages, parse_recv)
  end

  defp parse_execute_send(s, status, query, params, buffer) do
    %Query{param_formats: pfs, result_formats: rfs, name: name, statement: statement} = query
    msgs = [
      msg_parse(name: name, statement: statement, type_oids: []),
      msg_bind(name_port: "", name_stat: name, param_formats: pfs, params: params, result_formats: rfs),
      msg_execute(name_port: "", max_rows: 0)]
    send_and_recv(s, status, query, buffer, msgs, &parse_recv/4)
  end

  defp parse_copy_in_send(s, status, query, params, buffer) do
    %Query{param_formats: pfs, result_formats: rfs, name: name, statement: statement} = query
    {params, [copy_data_msg]} = Enum.split(params, -1)
    msgs = [
      msg_parse(name: name, statement: statement, type_oids: []),
      msg_bind(name_port: "", name_stat: name, param_formats: pfs, params: params, result_formats: rfs),
      msg_execute(name_port: "", max_rows: 0),
      copy_data_msg,
      msg_copy_done()]
    copy_in_recv = &copy_in_recv/4
    bind_recv = &bind_recv(&1, &2, &3, &4, copy_in_recv)
    parse_recv = &parse_recv(&1, &2, &3, &4, bind_recv)
    send_and_recv(s, status, query, buffer, msgs, parse_recv)
  end

  defp parse_copy_in_send(s, status, stream, query, params, buffer) do
    %Query{param_formats: pfs, result_formats: rfs, name: name, statement: statement} = query
    msgs = [
      msg_parse(name: name, statement: statement, type_oids: []),
      msg_bind(name_port: "", name_stat: name, param_formats: pfs, params: params, result_formats: rfs),
      msg_flush(),
      msg_execute(name_port: "", max_rows: 0)]
    status = %{status | sync: :flush}
    bind_recv = fn(s, status, _query, buffer) ->
      bind_recv(s, status, stream, buffer, &copy_in_ready/4)
    end
    parse_recv = &parse_recv(&1, &2, &3, &4, bind_recv)
    copy_in_send(s, status, query, buffer, msgs, parse_recv)
  end

  defp send_and_recv(s, %{mode: :savepoint} = status, query, buffer, msgs, recv) do
    case msg_send(s, savepoint_msgs(s, msgs), buffer) do
      :ok ->
        savepoint_recv(s, status, query, buffer, recv)
      {:disconnect, _, _} = dis ->
        dis
    end
  end

  defp send_and_recv(s, %{mode: :transaction} = status, query, buffer, msgs, recv) do
    case msg_send(s, msgs ++ [msg_sync()], buffer) do
      :ok ->
        recv.(s, status, query, buffer)
      {:disconnect, _, _} = dis ->
        dis
    end
  end

  defp savepoint_msgs(s, msgs) do
    savepoint = transaction_msgs(s, ["SAVEPOINT postgrex_query"])
    release = transaction_msgs(s, ["RELEASE SAVEPOINT postgrex_query", :sync])
    savepoint ++ msgs ++ release
  end

  defp savepoint_recv(s, status, query, buffer, recv) do
    case msg_recv(s, :infinity, buffer) do
      {:ok, msg_parse_complete(), buffer} ->
        savepoint_recv(s, status, query, buffer, recv)
      {:ok, msg_bind_complete(), buffer} ->
        savepoint_recv(s, status, query, buffer, recv)
      {:ok, msg_command_complete(), buffer} ->
        recv.(s, status, query, buffer)
      {:ok, msg_error(fields: fields), buffer} ->
        err = Postgrex.Error.exception(postgres: fields)
        do_sync_recv(s, status, err, buffer)
      {:ok, msg, buffer} ->
        s = handle_msg(s, status, msg)
        savepoint_recv(s, status, query, buffer, recv)
      {:disconnect, _, _} = dis ->
        dis
    end
  end

  defp savepoint_rollback(s, status, err, buffer) do
    case msg_recv(s, :infinity, buffer) do
      {:ok, msg_ready(status: :failed), buffer} ->
        do_savepoint_rollback(s, status, err, buffer)
      {:ok, msg_ready(status: postgres), buffer} ->
        sync_error(s, postgres, buffer)
      {:ok, msg, buffer} ->
        savepoint_rollback(handle_msg(s, status, msg), status, err, buffer)
      {:disconnect, _, _} = dis ->
        dis
    end
  end

  defp do_savepoint_rollback(s, status, err, buffer) do
    statements = ["ROLLBACK TO SAVEPOINT postgrex_query",
                  "RELEASE SAVEPOINT postgrex_query",
                  :sync]
    messages = transaction_msgs(s, statements)
    case msg_send(s, messages, buffer) do
      :ok ->
        sync_recv = &do_sync_recv/4
        recv = &savepoint_recv(&1, &2, &3, &4, sync_recv)
        savepoint_recv(s, status, err, buffer, recv)
      {:disconnect, _, _} = dis ->
        dis
    end
  end

  defp bind_recv(s, status, query, buffer, recv \\ &execute_recv/4) do
    case msg_recv(s, :infinity, buffer) do
      {:ok, msg_bind_complete(), buffer} ->
        recv.(s, status, query, buffer)
      {:ok, msg_error(fields: fields), buffer} ->
        bind_error(s, status, query, fields, buffer)
      {:ok, msg, buffer} ->
        bind_recv(handle_msg(s, status, msg), status, query, buffer)
      {:disconnect, _, _} = dis ->
        dis
    end
  end

  defp bind_error(s, status, query, fields, buffer) do
    err = Postgrex.Error.exception(postgres: fields)
    _ = if err.postgres.code == :invalid_sql_statement_name do
      Logger.error fn() ->
        [inspect(query) | " is not prepared on backend"]
      end
      query_delete(s, query)
    end
    sync_recv(s, status, err, buffer)
  end

  defp execute_recv(s, status, query, buffer) do
    case msg_recv(s, :infinity, buffer) do
      {:ok, msg_data_row(values: values), buffer} ->
        execute_recv(s, status, query, [values], buffer)
      {:ok, msg_command_complete(tag: tag), buffer} ->
        complete(s, status, query, [], tag, buffer)
      {:ok, msg_empty_query(), buffer} ->
        sync_recv(s, status, %Postgrex.Result{num_rows: 0}, buffer)
      {:ok, msg_error(fields: fields), buffer} ->
        err = Postgrex.Error.exception(postgres: fields)
        sync_recv(s, status, err, buffer)
      {:ok, msg_copy_in_response(), buffer} ->
        msg = "query #{inspect query} is trying to copying but it is not supported"
        err = ArgumentError.exception(msg)
        copy_fail(s, status, err, buffer)
      {:ok, msg_copy_out_response(), buffer} ->
        copy_out(s, status, query, buffer)
      {:ok, msg_copy_both_response(), buffer} ->
        copy_both_disconnect(s, query, buffer)
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
      {:ok, msg_portal_suspend(), buffer} ->
        suspend(s, status, query, rows, buffer)
      {:ok, msg, buffer} ->
        execute_recv(handle_msg(s, status, msg), status, query, rows, buffer)
      {:disconnect, _, _} = dis ->
        dis
    end
  end

  defp complete(s, status, %Query{} = query, rows, tag, buffer) do
    %{connection_id: connection_id} = s
    {command, nrows} = decode_tag(tag)
    %Query{columns: cols} = query
    # Fix for PostgreSQL 8.4 (doesn't include number of selected rows in tag)
    nrows =
      if is_nil(nrows) and command == :select, do: length(rows), else: nrows

    rows =
      if is_nil(cols) and rows == [] and command != :copy, do: nil, else: rows

    result = %Postgrex.Result{command: command, num_rows: nrows || 0,
                              rows: rows, columns: cols, connection_id: connection_id}
    sync_recv(s, status, result, buffer)
  end
  defp complete(s, status, stream, rows, tag, buffer) do
    %Postgrex.Stream{query: query, num_rows: previous_nrows} = stream
    %{connection_id: connection_id} = s
    {command, nrows} = decode_tag(tag)
    %Query{columns: cols} = query
    # Fix for PostgreSQL 8.4 (doesn't include number of selected rows in tag)
    nrows =
      if is_nil(nrows) and command == :select, do: length(rows), else: nrows

    nrows =
      if command == :select, do: nrows + previous_nrows, else: nrows

    rows =
      if is_nil(cols) and rows == [] and command != :copy, do: nil, else: rows

    result = %Postgrex.Result{command: command, num_rows: nrows || 0,
                              rows: rows, columns: cols, connection_id: connection_id}
    sync_recv(s, status, result, buffer)
  end

  defp suspend(s, status, stream, rows, buffer) do
    %{connection_id: connection_id} = s
    %Postgrex.Stream{query: %Query{columns: cols}} = stream

    result = %Postgrex.Result{command: :stream, num_rows: :stream,
                              rows: rows, columns: cols,
                              connection_id: connection_id}
    sync_recv(s, status, result, buffer)
  end

  defp copy_fail(s, %{mode: :transaction} = status, err, buffer) do
    msg = Exception.message(err)
    messages = [msg_copy_fail(message: msg), msg_sync()]
    case msg_send(s, messages, buffer) do
      :ok ->
        copy_fail_recv(s, status, err, buffer)
      {:disconnect, _, _} = dis ->
        dis
    end
  end
  defp copy_fail(s, %{mode: :savepoint} = status, err, buffer) do
    # Releasing savepoint will cause an error so receive that
    copy_fail_recv(s, status, err, buffer)
  end

  defp copy_fail_recv(s, status, err, buffer) do
    case msg_recv(s, :infinity, buffer) do
      {:ok, msg_error(fields: fields), buffer} ->
        err = Postgrex.Error.exception(postgres: fields)
        sync_recv(s, status, err, buffer)
      {:ok, msg, buffer} ->
        copy_fail_recv(handle_msg(s, status, msg), status, err, buffer)
      {:disconnect, _, _} = dis ->
        dis
    end
  end

  defp copy_out(s, status, %Query{} = query, buffer) do
    copy_out_recv(s, status, query, :infinity, [], 0, buffer)
  end
  defp copy_out(s, status, stream, buffer) do
    %Stream{max_rows: max_rows} = stream
    max_rows = if max_rows == 0, do: :infinity, else: max_rows
    copy_out_recv(s, status, stream, max_rows, [], 0, buffer)
  end

  defp copy_out_recv(s, _, stream, max_rows, acc, max_rows, buffer) do
    %Stream{ref: ref} = stream
    %{postgres: postgres, connection_id: connection_id} = s
    result = %Postgrex.Result{command: :copy_stream, num_rows: :copy_stream,
      rows: acc, columns: nil, connection_id: connection_id}
    ok(s, result, {postgres, ref}, buffer)
  end
  defp copy_out_recv(s, status, query, max_rows, acc, nrows, buffer) do
     case msg_recv(s, :infinity, buffer) do
      {:ok, msg_copy_data(data: data), buffer} ->
        copy_out_recv(s, status, query, max_rows, [data | acc], nrows+1, buffer)
      {:ok, msg_copy_done(), buffer} ->
        copy_out_done(s, status, query, acc, buffer)
      {:ok, msg_error(fields: fields), buffer} ->
        err = Postgrex.Error.exception(postgres: fields)
        sync_recv(s, status, err, buffer)
      {:ok, msg, buffer} ->
        s = handle_msg(s, status, msg)
        copy_out_recv(s, status, query, max_rows, acc, nrows, buffer)
      {:disconnect, _, _} = dis ->
        dis
    end
  end

  defp copy_out_done(s, status, query, acc, buffer) do
    case msg_recv(s, :infinity, buffer) do
      {:ok, msg_command_complete(tag: tag), buffer} ->
        complete(s, status, query, acc, tag, buffer)
      {:ok, msg_error(fields: fields), buffer} ->
        err = Postgrex.Error.exception(postgres: fields)
        sync_recv(s, status, err, buffer)
      {:ok, msg, buffer} ->
        s = handle_msg(s, status, msg)
        copy_out_done(s, status, query, acc, buffer)
      {:disconnect, _, _} = dis ->
        dis
    end
  end

  defp copy_in_recv(s, status, query, buffer) do
    case msg_recv(s, :infinity, buffer) do
      {:ok, msg_copy_in_response(), buffer} ->
        copy_in_done(s, status, query, buffer)
      {:ok, msg_command_complete(tag: tag), buffer} ->
        complete(s, status, query, [], tag, buffer)
      {:ok, msg_data_row(values: values), buffer} ->
        execute_recv(s, status, query, [values], buffer)
      {:ok, msg_empty_query(), buffer} ->
        sync_recv(s, status, %Postgrex.Result{num_rows: 0}, buffer)
      {:ok, msg_error(fields: fields), buffer} ->
        err = Postgrex.Error.exception(postgres: fields)
        sync_recv(s, status, err, buffer)
      {:ok, msg_copy_out_response(), buffer} ->
        copy_out(s, status, query, buffer)
      {:ok, msg_copy_both_response(), buffer} ->
        copy_both_disconnect(s, query, buffer)
      {:ok, msg, buffer} ->
        copy_in_recv(handle_msg(s, status, msg), status, query, buffer)
      {:disconnect, _, _} = dis ->
        dis
    end
  end

  defp copy_in_done(s, status, query, buffer) do
    case msg_recv(s, :infinity, buffer) do
      {:ok, msg_command_complete(tag: tag), buffer} ->
        complete(s, status, query, nil, tag, buffer)
      {:ok, msg_error(fields: fields), buffer} ->
        err = Postgrex.Error.exception(postgres: fields)
        sync_recv(s, status, err, buffer)
      {:ok, msg, buffer} ->
        copy_in_done(handle_msg(s, status, msg), status, query, buffer)
      {:disconnect, _, _} = dis ->
        dis
    end
  end

  defp copy_in_send(s, %{mode: :transaction} = status, stream, buffer, msgs, recv) do
    case msg_send(s, msgs, buffer) do
      :ok ->
        recv.(s, status, stream, buffer)
      {:disconnect, _, _} = dis ->
        dis
    end
  end

  defp copy_in_send(s, %{mode: :savepoint} = status, stream, buffer, msgs, recv) do
    savepoint_msgs = transaction_msgs(s, ["SAVEPOINT postgrex_query"])
    case msg_send(s, savepoint_msgs ++ msgs, buffer) do
      :ok ->
        savepoint_recv(s, status, stream, buffer, recv)
      {:disconnect, _, _} = dis ->
        dis
    end
  end

  defp copy_in_ready(s, _status, stream, buffer) do
    %{connection_id: connection_id, postgres: postgres} = s
    result = %Postgrex.Result{connection_id: connection_id, command: :copy_stream,
                              rows: nil, num_rows: :copy_stream}
    %Stream{ref: ref} = stream
    ok(s, result, {postgres, ref}, buffer)
  end

  defp copy_data(s, _status, data, buffer) do
    case do_send(s, data, buffer) do
      :ok ->
        %{connection_id: connection_id, postgres: postgres} = s
        result = %Postgrex.Result{connection_id: connection_id,
                                  command: :copy_stream, rows: nil,
                                  num_rows: :copy_stream}
        ok(s, result, postgres, buffer)
      {:disconnect, _, _} = dis ->
        dis
    end
  end

  defp copy_in_stop(s, %{mode: :transaction} = status, stream, buffer, msg) do
    msgs = [msg, msg_sync()]
    copy_in_stop_send(s, status, stream, buffer, msgs)
  end

  defp copy_in_stop(s, %{mode: :savepoint} = status, stream, buffer, msg) do
    release = transaction_msgs(s, ["RELEASE SAVEPOINT postgrex_query", :sync])
    msgs = [msg | release]
    copy_in_stop_send(s, status, stream, buffer, msgs)
  end

  defp copy_in_stop_send(s, status, stream, buffer, msgs) do
    case msg_send(s, msgs, buffer) do
      :ok ->
        copy_in_recv(s, status, stream, buffer)
      {:disconnect, _, _} = dis ->
        dis
    end
  end

  defp copy_both_disconnect(s, query, buffer) do
    msg = "query #{inspect query} is trying to copy both ways but it is not supported"
    err = ArgumentError.exception(msg)
    {:disconnect, err, %{s | buffer: buffer}}
  end

  ## close
  defp close(s, status, %Query{name: name} = query, result, buffer) do
    messages = [msg_close(type: :statement, name: name)]
    close(s, status, query, buffer, result, messages)
  end
  defp close(s, status, %Stream{portal: portal} = stream, result, buffer) do
    messages = [msg_close(type: :portal, name: portal)]
    close(s, status, stream, buffer, result, messages)
  end

  defp close(s, status, query, buffer, result, messages) do
    recv = &close_recv(&1, &2, query, &3, &4)
    send_and_recv(s, status, result, buffer, messages, recv)
  end

  defp close_recv(s, status, query, result, buffer) do
    case msg_recv(s, :infinity, buffer) do
      {:ok, msg_close_complete(), buffer} ->
        statement_query_delete(s, query)
        sync_recv(s, status, result, buffer)
      {:ok, msg_error(fields: fields), buffer} ->
        sync_recv(s, status, Postgrex.Error.exception(postgres: fields), buffer)
      {:ok, msg, buffer} ->
        close_recv(handle_msg(s, status, msg), status, query, result, buffer)
      {:disconnect, _, _} = dis ->
        dis
    end
  end

  ## sync
  defp sync(s, status, result, buffer) do
    case msg_send(s, msg_sync(), buffer) do
      :ok                       -> sync_recv(s, status, result, buffer)
      {:disconnect, _, _} = dis -> dis
    end
  end

  ## transaction

  defp handle_transaction(name, next_postgres, cmd, opts, s) do
    %{connection_id: connection_id, buffer: buffer} = s
    status = %{notify: notify(opts), mode: :transaction, sync: :sync}
    res = %Postgrex.Result{command: cmd, connection_id: connection_id}
    transaction_send(%{s | buffer: nil}, status, name, next_postgres, res, buffer)
  end

  defp transaction_send(s, status, statement, next_postgres, res, buffer) do
    msgs = transaction_msgs(s, [statement, :sync])
    case msg_send(s, msgs, buffer) do
      :ok ->
        transaction_recv(s, status, next_postgres, res, buffer)
      {:disconnect, _, _} = dis ->
        dis
    end
  end

  defp transaction_msgs(_, []) do
    []
  end
  defp transaction_msgs(_, [:sync]) do
     [msg_sync()]
  end
  defp transaction_msgs(%{queries: nil} = s, [statement | statements]) do
    [msg_parse(name: "", statement: statement, type_oids: []),
     msg_bind(name_port: "", name_stat: "", param_formats: [], params: [], result_formats: []),
     msg_execute(name_port: "" , max_rows: 0) |
     transaction_msgs(s, statements)]
  end
  defp transaction_msgs(s, [name | names]) do
    name = [@reserved_prefix | name]
    [msg_bind(name_port: "", name_stat: name, param_formats: [], params: [], result_formats: []),
     msg_execute(name_port: "" , max_rows: 0) |
     transaction_msgs(s, names)]
  end

  defp transaction_recv(s, status, next_postgres, res, buffer) do
    %{transactions: transactions} = s
    case msg_recv(s, :infinity, buffer) do
      {:ok, msg_ready(status: postgres), buffer} when transactions == :naive ->
        ok(s, res, postgres, buffer)
      {:ok, msg_ready(status: ^next_postgres), buffer} ->
        ok(s, res, next_postgres, buffer)
      {:ok, msg_ready(status: postgres), buffer} ->
        sync_error(s, postgres, buffer)
      {:ok, msg_parse_complete(), buffer} ->
        transaction_recv(s, status, next_postgres, res, buffer)
      {:ok, msg_bind_complete(), buffer} ->
        transaction_recv(s, status, next_postgres, res, buffer)
      {:ok, msg_command_complete(), buffer} ->
        transaction_recv(s, status, next_postgres, res, buffer)
      {:ok, msg_error(fields: fields), buffer} when transactions == :naive ->
        err = Postgrex.Error.exception(postgres: fields)
        sync_recv(s, status, err, buffer)
      {:ok, msg_error(fields: fields), buffer} ->
        err = Postgrex.Error.exception(postgres: fields)
        disconnect(s, err, buffer)
      {:ok, msg, buffer} ->
        s = handle_msg(s, status, msg)
        transaction_recv(s, status, next_postgres, res, buffer)
      {:disconnect, _, _} = dis ->
        dis
    end
  end

  defp handle_savepoint(names, cmd, opts, s) do
   %{connection_id: connection_id, buffer: buffer} = s
    status = %{notify: notify(opts), mode: :transaction, sync: :sync}
    res = %Postgrex.Result{command: cmd, connection_id: connection_id}
    savepoint_send(%{s | buffer: nil}, status, names, res, buffer)
  end

  defp savepoint_send(s, status, statements, res, buffer) do
    msgs = transaction_msgs(s, statements)
    case msg_send(s, msgs, buffer) do
      :ok ->
        savepoint_recv(s, status, res, buffer)
      {:disconnect, _, _} = dis ->
        dis
    end
  end

  defp savepoint_recv(s, status, res, buffer) do
    %{postgres: postgres, transactions: transactions} = s
    case msg_recv(s, :infinity, buffer) do
      {:ok, msg_parse_complete(), buffer} ->
        savepoint_recv(s, status, res, buffer)
      {:ok, msg_bind_complete(), buffer} ->
        savepoint_recv(s, status, res, buffer)
      {:ok, msg_command_complete(), buffer} ->
        savepoint_recv(s, status, res, buffer)
      {:ok, msg_ready(status: :idle), buffer}
      when postgres == :transaction and transactions == :strict ->
        sync_error(s, :idle, buffer)
      {:ok, msg_ready(status: :transaction), buffer}
      when postgres == :idle and transactions == :strict ->
        sync_error(s, :transaction, buffer)
      {:ok, msg_ready(status: :failed), buffer}
      when postgres == :idle and transactions == :strict ->
        sync_error(s, :failed, buffer)
      {:ok, msg_ready(status: postgres), buffer} ->
        ok(s, res, postgres, buffer)
      {:ok, msg_error(fields: fields), buffer} ->
        err = Postgrex.Error.exception(postgres: fields)
        do_sync_recv(s, status, err, buffer)
      {:ok, msg, buffer} ->
        s = handle_msg(s, status, msg)
        savepoint_recv(s, status, res, buffer)
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

  defp mode(opts) do
    case opts[:mode] || :transaction do
      :transaction -> :transaction
      :savepoint   -> :savepoint
    end
  end

  defp columns(fields) do
    Enum.map(fields, fn row_field(type_oid: oid, name: name) ->
      {oid, name}
    end) |> :lists.unzip
  end

  defp tag(:gen_tcp), do: :tcp
  defp tag(:ssl), do: :ssl

  defp decode_tag("INSERT " <> rest) do
    [_oid, nrows] = :binary.split(rest, " ")
    {:insert, String.to_integer(nrows)}
  end
  defp decode_tag("SELECT " <> int),
    do: {:select, String.to_integer(int)}
  defp decode_tag("UPDATE " <> int),
    do: {:update, String.to_integer(int)}
  defp decode_tag("DELETE " <> int),
    do: {:delete, String.to_integer(int)}
  defp decode_tag("FETCH " <> int),
    do: {:fetch, String.to_integer(int)}
  defp decode_tag("MOVE " <> int),
    do: {:move, String.to_integer(int)}
  defp decode_tag("COPY " <> int),
    do: {:copy, String.to_integer(int)}
  defp decode_tag("BEGIN"),
    do: {:commit, nil}
  defp decode_tag("COMMIT"),
    do: {:commit, nil}
  defp decode_tag("ROLLBACK"),
    do: {:rollback, nil}
  defp decode_tag(tag),
    do: decode_tag(tag, "")

  defp decode_tag(<<>>, acc),
    do: {String.to_atom(acc), nil}
  defp decode_tag(<<?\s, t::binary>>, acc),
    do: decode_tag(t, <<acc::binary, ?_>>)
  defp decode_tag(<<h, t::binary>>, acc) when h in ?A..?Z,
    do: decode_tag(t, <<acc::binary, h+32>>)
  defp decode_tag(<<h, t::binary>>, acc),
    do: decode_tag(t, <<acc::binary, h>>)

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
    case mod.recv(sock, min(more, @max_packet), timeout) do
      {:ok, data} when byte_size(data) < more ->
        msg_recv(s, timeout, [buffer | data], more - byte_size(data))
      {:ok, data} when is_binary(buffer) ->
        msg_recv(s, timeout, buffer <> data)
      {:ok, data} when is_list(buffer) ->
        msg_recv(s, timeout, IO.iodata_to_binary([buffer | data]))
      {:error, reason} ->
        disconnect(s, tag(mod), "recv", reason, IO.iodata_to_binary(buffer))
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

  defp row_decode(<<>>), do: []
  defp row_decode(<<-1::int32, rest::binary>>) do
    [nil | row_decode(rest)]
  end
  defp row_decode(<<len::uint32, value::binary(len), rest::binary>>) do
    [value | row_decode(rest)]
  end

  defp msg_send(s, msgs, buffer) when is_list(msgs) do
    binaries = Enum.reduce(msgs, [], &[&2 | maybe_encode_msg(&1)])
    do_send(s, binaries, buffer)
  end

  defp msg_send(s, msg, buffer) do
    do_send(s, encode_msg(msg), buffer)
  end

  defp maybe_encode_msg(msg) when is_tuple(msg), do: encode_msg(msg)
  defp maybe_encode_msg(msg) when is_binary(msg) or is_list(msg), do: msg

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

  defp ok(s, %Postgrex.Result{} = res, postgres, buffer) do
    {:ok, res, %{s | postgres: postgres, buffer: buffer}}
  end
  defp ok(s, %Postgrex.Query{} = query, postgres, buffer) do
    {:ok, query, %{s | postgres: postgres, buffer: buffer}}
   end
  defp ok(s, %Postgrex.Error{} = err, postgres, buffer) do
    %{connection_id: connection_id} = s
    err = %{err | connection_id: connection_id}
    {:error, err, %{s | postgres: postgres, buffer: buffer}}
  end
  defp ok(s, :active_once, postgres, buffer) do
    activate(%{s | postgres: postgres}, buffer)
  end
  defp ok(s, nil, postgres, buffer) do
    {:ok, %{s | postgres: postgres, buffer: buffer}}
  end

  defp disconnect(s, tag, action, reason, buffer) do
    err = Postgrex.Error.exception(tag: tag, action: action, reason: reason)
    disconnect(s, err, buffer)
  end

  defp disconnect(%{connection_id: connection_id} = s, %Postgrex.Error{} = err, buffer) do
    {:disconnect, %{err | connection_id: connection_id}, %{s | buffer: buffer}}
  end

  defp reserved_error(query, s) do
    err = ArgumentError.exception("query #{inspect query} uses reserved name")
    {:error, err, s}
  end

  # Query has completed so ok to use state timeout as message should either be
  # buffer or in flight. sync_recv/4 used by simple queries so can't use
  # :infinity.
  defp sync_recv(s, %{sync: :flush} = status, res, buffer) do
    case msg_send(s, msg_sync(), buffer) do
      :ok ->
        sync_recv(s, %{status | sync: :sync}, res, buffer)
      {:disconnect, _, _} = dis ->
        dis
    end
  end
  defp sync_recv(s, %{mode: :savepoint} = status, res, buffer) do
    case res do
      %Postgrex.Error{} ->
        savepoint_rollback(s, status, res, buffer)
      _ ->
        savepoint_recv(s, status, res, buffer, &do_sync_recv/4)
    end
  end
  defp sync_recv(s, %{mode: :transaction} = status, res, buffer) do
    do_sync_recv(s, status, res, buffer)
  end

  defp do_sync_recv(s, status, res, buffer) do
    %{postgres: postgres, transactions: transactions, timeout: timeout} = s
    case msg_recv(s, timeout, buffer) do
      {:ok, msg_ready(status: :idle), buffer}
      when postgres == :transaction and transactions == :strict ->
        sync_error(s, :idle, buffer)
      {:ok, msg_ready(status: :transaction), buffer}
      when postgres == :idle and transactions == :strict ->
        sync_error(s, :transaction, buffer)
      {:ok, msg_ready(status: :failed), buffer}
      when postgres == :idle and transactions == :strict ->
        sync_error(s, :failed, buffer)
      {:ok, msg_ready(status: postgres), buffer} ->
        ok(s, res, postgres, buffer)
      {:ok, msg, buffer} ->
        do_sync_recv(handle_msg(s, status, msg), status, res, buffer)
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

  defp queries_new(), do: :ets.new(__MODULE__, [:set, :public])

  defp queries_delete(%{queries: nil}), do: true
  defp queries_delete(%{queries: queries}), do: :ets.delete(queries)

  defp query_put(%{queries: nil}, _), do: :ok
  defp query_put(s, %Stream{query: query}), do: query_put(s, query)
  defp query_put(%{queries: queries}, query) do
    %Query{name: name, statement: statement} = query
    try do
      :ets.insert(queries, {name, statement})
    rescue
      ArgumentError ->
        # ets table deleted, socket will be closed, rescue here and get nice
        # error when trying to recv on socket.
        :ok
    else
      true ->
        :ok
    end
  end

  defp unnamed(%Query{name: ""} = query), do: query
  defp unnamed(query), do: %Query{query | name: ""}

  defp unnamed_query_delete(s, %Query{name: ""} = query) do
    query_delete(s, query)
  end
  defp unnamed_query_delete(s, %Stream{query: %Query{name: ""} = query}) do
    query_delete(s, query)
  end
  defp unnamed_query_delete(_, _), do: :ok

  defp statement_query_delete(s, %Query{} = query), do: query_delete(s, query)
  defp statement_query_delete(_, %Stream{}), do: :ok

  defp query_delete(%{queries: nil}, _), do: :ok
  defp query_delete(s, %Stream{query: query}), do: query_delete(s, query)
  defp query_delete(%{queries: queries}, %Query{name: name}) do
    try do
      :ets.delete(queries, name)
    rescue
      ArgumentError ->
        :ok
    else
      true ->
        :ok
    end
  end

  defp query_member?(%{queries: queries}, query) when queries != nil do
    %Query{name: name, statement: statement} = query
    try do
      :ets.lookup(queries, name)
    rescue
      ArgumentError ->
        false
    else
      [{_, ^statement}] ->
        true
      _ ->
        false
    end
  end
end
