defmodule Postgrex.Protocol do
  @moduledoc false

  alias Postgrex.Types
  alias Postgrex.Query
  import Postgrex.Messages
  import Postgrex.BinaryUtils

  @timeout 5000
  @default_extensions [{Postgrex.Extensions.Binary, nil}, {Postgrex.Extensions.Text, nil}]
  @sock_opts [packet: :raw, mode: :binary, active: false]

  ## TODO: Use struct for state
  @type state :: %{}
  @typep parameters :: %{binary => binary}
  @typep notifications :: [{binary, binary}]

  @spec connect(Keyword.t) ::
    {:ok, state, parameters, notifications} | {:error, Postgrex.Error.t}
  def connect(opts) do
    host       = Keyword.fetch!(opts, :hostname) |> to_char_list
    port       = opts[:port] || 5432
    timeout    = opts[:timeout] || @timeout
    sock_opts  = [send_timeout: timeout] ++ (opts[:socket_options] || [])
    custom     = opts[:extensions] || []
    extensions = custom ++ @default_extensions
    ssl?       = opts[:ssl] || false
    types?     = opts[:types] || true

    s = %{sock: nil, backend_key: nil, types: nil, timeout: timeout}

    types_key = if types?, do: {host, port, Keyword.fetch!(opts, :database), custom}
    status = %{opts: opts, parameters: %{}, notifications: [],
               types_key: types_key, types_ref: nil, extensions: extensions,
               extension_info: nil}
    case connect(host, port, sock_opts ++ @sock_opts, s) do
      {:ok, s} when ssl?  -> ssl(s, status)
      {:ok, s}            -> startup(s, status)
      {:error, _} = error -> error
    end
  end

  @spec checkout(state, binary | :active_once) ::
    {:ok, binary} | {:error, Postgrex.Error.t}
  def checkout(%{sock: sock} = s, :active_once) do
    case setopts(sock, [active: :false]) do
      :ok               -> recv_buffer(s)
      {:error, _} = err -> err
    end
  end
  def checkout(_, buffer), do: {:ok, buffer}

  @spec checkin(state, binary) :: :ok | {:error, Postgrex.Error.t}
  def checkin(%{sock: sock}, buffer) do
    activate(sock, buffer)
  end

  @spec simple_query(state, String.t, binary | :active_once) ::
    {:ok, Postgrex.Result.t | Postgrex.Error.t, parameters, notifications,
      binary} |
    {:error, Postgrex.Error.t}
  def simple_query(s, statement, buffer) do
    status = %{parameters: %{}, notifications: [], ok: :result, sync: :sync}
    simple_send(s, status, statement, buffer)
  end

  @spec query(state, String.t, [any], binary | :active_once) ::
    {:ok, Postgrex.Result.t | Postgrex.Error.t |
      {:error | :throw | :exit, any, list}, parameters, notifications, binary} |
    {:error, Postgrex.Error.t}
  def query(s, statement, params, buffer) do
    status = %{parameters: %{}, notifications: [], ok: :status,
               sync: :close_sync}
    case prepare_send(s, status, "", statement, buffer) do
      {:ok, %Query{} = query, status, buffer} ->
        query_encode(s, %{status | ok: :result}, query, params, buffer)
      {:ok, result, status, buffer} ->
        ok(result, %{status | ok: :result}, buffer)
      result ->
        result
    end
  end

  @spec prepare(state, iodata, iodata, binary | :active_once) ::
    {:ok, Postgrex.Query.t | Postgrex.Error.t |
      {:error | :throw | :exit, any, list}, parameters, notifications, binary} |
    {:error, Postgrex.Error.t}
  def prepare(s, name, statement, buffer) do
    status = %{parameters: %{}, notifications: [], ok: :result}
    prepare_send(s, status, name, statement, buffer)
  end

  @spec execute(state, Postgrex.Query.t, binary | :active_once) ::
    {:ok, Postgrex.Result.t | Postgrex.Error.t |
      {:error | :throw | :exit, any, list}, parameters, notifications, binary} |
    {:error, Postgrex.Error.t}
  def execute(s, %Query{encoders: nil} = query, buffer) do
    status = %{parameters: %{}, notifications: [], ok: :result, sync: :sync}
    execute_send(s, status, query, buffer)
  end

  @spec close(state, Postgrex.Query.t, binary | :active_once) ::
    {:ok, parameters, notifications, binary} | {:error, Postgrex.Error.t}
  def close(s, query, buffer) do
    status = %{parameters: %{}, notifications: [], ok: :no_result}
    close(s, status, query, nil, buffer)
  end

  @spec message(state, any) ::
    {:ok, parameters, notifications} | {:error, Postgrex.Error.t} | :unknown
  def message(%{sock: {:gen_tcp, sock}} = s, {:tcp, sock, data}) do
    data(s, data)
  end
  def message(%{sock: {:gen_tcp, sock}}, {:tcp_closed, sock}) do
    {:error, Postgrex.Error.exception(tag: :tcp, action: "async recv", reason: :closed)}
  end
  def message(%{sock: {:gen_tcp, sock}}, {:tcp_error, sock, reason}) do
    {:error, Postgrex.Error.exception(tag: :tcp, action: "async recv", reason: reason)}
  end
  def message(%{sock: {:ssl, sock}} = s, {:ssl, sock, data}) do
    data(s, data)
  end
  def message(%{sock: {:ssl, sock}}, {:ssl_closed, sock}) do
    {:error, Postgrex.Error.exception(tag: :ssl, action: "async recv", reason: :closed)}
  end
  def message(%{sock: {:ssl, sock}}, {:ssl_error, sock, reason}) do
    {:error, Postgrex.Error.exception(tag: :ssl, action: "async recv", reason: reason)}
  end
  def message(_, _) do
    :unknown
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

  defp ssl(%{sock: sock} = s, status) do
    case msg_send(msg_ssl_request(), sock) do
      :ok              -> ssl_recv(s, status)
      {:error, _} = err -> err
    end
  end

  defp ssl_recv(%{sock: {:gen_tcp, sock}, timeout: timeout} = s, status) do
    case :gen_tcp.recv(sock, 1, timeout) do
      {:ok, <<?S>>} ->
        ssl_connect(s, status)
      {:ok, <<?N>>} ->
        {:error, %Postgrex.Error{message: "ssl not available"}}
      {:error, reason} ->
        {:error, Postgrex.Error.exception(tag: :tcp, action: "recv", reason: reason)}
    end
  end

  defp ssl_connect(%{sock: {:gen_tcp, sock}, timeout: timeout} = s, status) do
    case :ssl.connect(sock, status.opts[:ssl_opts] || [], timeout) do
      {:ok, ssl_sock} ->
        startup(%{s | sock: {:ssl, ssl_sock}}, status)
      {:error, reason} ->
        {:error, Postgrex.Error.exception(tag: :ssl, action: "connect", reason: reason)}
    end
  end

  ## startup

  defp startup(%{sock: sock} = s, %{opts: opts} = status) do
    params = opts[:parameters] || []
    user = Keyword.fetch!(opts, :username)
    database = Keyword.fetch!(opts, :database)
    msg = msg_startup(params: [user: user, database: database] ++ params)
    case msg_send(msg, sock) do
      :ok               -> auth_recv(s, status, <<>>)
      {:error, _} = err -> err
    end
  end

  ## auth

  defp auth_recv(%{sock: sock, timeout: timeout} = s, status, buffer) do
    case msg_recv(sock, buffer, timeout) do
      {:ok, msg_auth(type: :ok), buffer} ->
        init_recv(s, status, buffer)
      {:ok, msg_auth(type: :cleartext), buffer} ->
        auth_cleartext(s, status, buffer)
      {:ok, msg_auth(type: :md5, data: salt), buffer} ->
        auth_md5(s, status, salt, buffer)
      {:ok, msg_error(fields: fields), _} ->
        {:error, Postgrex.Error.exception(postgres: fields)}
      {:error, _} = err->
        err
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

  defp auth_send(%{sock: sock} = s, msg, status, buffer) do
    case msg_send(msg, sock) do
      :ok               -> auth_recv(s, status, buffer)
      {:error, _} = err -> err
    end
  end

  ## init

  defp init_recv(%{sock: sock, timeout: timeout} = s, status, buffer) do
    case msg_recv(sock, buffer, timeout) do
      {:ok, msg_backend_key(pid: pid, key: key), buffer} ->
        init_recv(%{s | backend_key: {pid, key}}, status, buffer)
      {:ok, msg_ready(), buffer} ->
        bootstrap(s, status, buffer)
      {:ok, msg_error(fields: fields), _} ->
        {:error, Postgrex.Error.exception(postgres: fields)}
      {:ok, msg, buffer} ->
        init_recv(s, handle_msg(status, msg), buffer)
      {:error, _} = err ->
        err
    end
  end

  ## bootstrap

  defp bootstrap(s, %{types_key: nil} = status, buffer) do
    bootstrap_ready(s, status, buffer)
  end
  defp bootstrap(s, %{types_key: types_key} = status, buffer) do
    case Postgrex.TypeServer.fetch(types_key) do
      {:ok, table} ->
        bootstrap_ready(%{s | types: table}, status, buffer)
      {:lock, ref, table} ->
        status = %{status | types_ref: ref}
        bootstrap_send(%{s | types: table}, status, buffer)
    end
  end

  defp bootstrap_send(%{sock: sock} = s, status, buffer) do
    %{parameters: parameters, extensions: extensions} = status

    extension_keys = Enum.map(extensions, &elem(&1, 0))
    extension_opts = Types.prepare_extensions(extensions, parameters)
    matchers = Types.extension_matchers(extension_keys, extension_opts)
    version = parameters["server_version"] |> Postgrex.Utils.parse_version
    statement = Types.bootstrap_query(matchers, version)
    msg = msg_query(statement: statement)
    case msg_send(msg, sock) do
      :ok ->
        status = %{status | extension_info: {extension_keys, extension_opts}}
        bootstrap_recv(s, status, buffer)
      {:error, } = err ->
        err
    end
  end

  defp bootstrap_recv(s, status, buffer) do
    %{sock: sock, timeout: timeout} = s
    case msg_recv(sock, buffer, timeout) do
      {:ok, msg_row_desc(), buffer} ->
        bootstrap_recv(s, status, [], buffer)
      {:ok, msg_error(fields: fields), _} ->
        {:error, Postgrex.Error.exception(postgres: fields)}
      {:ok, msg, buffer} ->
        bootstrap_recv(s, handle_msg(status, msg), buffer)
      {:error, _} = err ->
        err
    end
  end

  defp bootstrap_recv(s, status, rows, buffer) do
    %{sock: sock, timeout: timeout} = s
    case msg_recv(sock, buffer, timeout) do
      {:ok, msg_data_row(values: values), buffer} ->
        bootstrap_recv(s, status, [values | rows], buffer)
      {:ok, msg_command_complete(), buffer} ->
        bootstrap_types(s, status, rows, buffer)
      {:ok, msg_error(fields: fields), _} ->
        {:error, Postgrex.Error.exception(postgres: fields)}
      {:ok, msg, buffer} ->
        bootstrap_recv(s, handle_msg(status, msg), rows, buffer)
      {:error, _} = err ->
        err
    end
  end

  defp bootstrap_types(%{types: table} = s, status, rows, buffer) do
    %{types_ref: ref, extension_info: {extension_keys, extension_opts}} = status
    types = Types.build_types(rows)
    Types.associate_extensions_with_types(table, extension_keys, extension_opts, types)
    Postgrex.TypeServer.unlock(ref)
    bootstrap_sync_recv(s, status, buffer)
  end

  defp bootstrap_sync_recv(s, status, buffer) do
    %{sock: sock, timeout: timeout} = s
    case msg_recv(sock, buffer, timeout) do
      {:ok, msg_ready(), buffer} ->
        bootstrap_ready(s, status, buffer)
      {:ok, msg, buffer} ->
        bootstrap_sync_recv(s, handle_msg(status, msg), buffer)
      {:error, _} = err ->
        err
    end
  end

  defp bootstrap_ready(%{sock: sock} = s, status, buffer) do
    %{parameters: parameters, notifications: notifications} = status
    case activate(sock, buffer) do
      :ok ->
        {:ok, s, parameters, Enum.reverse(notifications)}
      {:error, _} = err ->
        err
    end
  end

  ## simple

  defp simple_send(%{sock: sock} = s, status, statement, buffer) do
    msg = msg_query(statement: statement)
    case msg_send(msg, sock) do
      :ok               -> simple_recv(s, status, buffer)
      {:error, _} = err -> err
    end
  end

  defp simple_recv(s, status, buffer) do
    %{sock: sock, timeout: timeout} = s
    case msg_recv(sock, buffer, timeout) do
      {:ok, msg_command_complete(tag: tag), buffer} ->
        complete(s, status, %Query{}, [], tag, buffer)
      {:ok, msg_error(fields: fields), buffer} ->
        sync_recv(s, status, Postgrex.Error.exception(postgres: fields), buffer)
      {:ok, msg, buffer} ->
        simple_recv(s, handle_msg(status, msg), buffer)
      {:error, _} = err ->
        err
    end
  end

  ## query

  defp query_encode(s, status, query, params, buffer) do
    try do
      Query.encode(%Query{query | params: params})
    catch
      kind, reason ->
        close(s, status, query, {kind, reason, System.stacktrace}, buffer)
    else
      query ->
        execute_send(s, status, query, buffer)
    end
  end

  ## prepare

  defp prepare_send(s, status, name, statement, buffer) do
    msgs = [
      msg_parse(name: name, statement: statement, type_oids: []),
      msg_describe(type: :statement, name: name),
      msg_flush() ]
    case msg_send(msgs, s) do
      :ok ->
        parse_recv(s, status, %Query{name: name, statement: statement}, buffer)
      {:error, _} = err -> err
    end
  end

  defp parse_recv(s, status, query, buffer) do
    %{sock: sock, timeout: timeout} = s
    case msg_recv(sock, buffer, timeout) do
      {:ok, msg_parse_complete(), buffer} ->
        describe_recv(s, status, query, buffer)
      {:ok, msg_error(fields: fields), buffer} ->
        exception = Postgrex.Error.exception(postgres: fields)
        sync_close(s, status, query, exception, buffer)
      {:ok, msg, buffer} ->
        parse_recv(s, handle_msg(status, msg), query, buffer)
      {:error, _} = err ->
        err
    end
  end

  defp describe_recv(s, status, query, buffer) do
    %{sock: sock, timeout: timeout} = s
    case msg_recv(sock, buffer, timeout) do
      {:ok, msg_no_data(), buffer} ->
        ok(query, status, buffer)
      {:ok, msg_parameter_desc(type_oids: []), buffer} ->
        describe_recv(s, status, query, buffer)
      {:ok, msg_parameter_desc(type_oids: param_oids), buffer} ->
        describe_params(s, status, query, param_oids, buffer)
      {:ok, msg_row_desc(fields: fields), buffer} ->
        describe_results(s, status, query, fields, buffer)
      {:ok, msg_error(fields: fields), buffer} ->
        exception = Postgrex.Error.exception(postgres: fields)
        sync_close(s, status, query, exception, buffer)
      {:ok, msg, buffer} ->
        describe_recv(s, handle_msg(status, msg), query, buffer)
      {:error, _} = err ->
        err
    end
  end

  defp describe_params(s, status, query, param_oids, buffer) do
    try do
      encoders(param_oids, s.types)
    catch
      kind, reason ->
        close(s, status, query, {kind, reason, System.stacktrace}, buffer)
    else
      {pfs, encoders} ->
        query = %Query{query | param_formats: pfs, encoders: encoders}
        describe_recv(s, status, query, buffer)
    end
  end

  defp describe_results(s, status, query, fields, buffer) do
    {col_oids, col_names} = columns(fields)
    try do
      decoders(col_oids, s.types)
    catch
      kind, reason ->
        close(s, status, query, {kind, reason, System.stacktrace}, buffer)
    else
      {rfs, decoders} ->
        query = %Query{query | result_formats: rfs, decoders: decoders,
          columns: col_names}
        ok(query, status, buffer)
    end
  end

  defp sync_close(s, status, %Query{name: name}, result, buffer) do
    msgs = [
      msg_close(type: :statement, name: name),
      msg_sync() ]
    case msg_send(msgs, s) do
      :ok               -> sync_recv(s, status, result, buffer)
      {:error, _} = err -> err
    end
  end

  ## execute

  defp execute_send(s, %{sync: sync} = status, query, buffer) do
    %Query{param_formats: pfs, params: params, result_formats: rfs,
           name: name} = query
    pfs = pfs || []
    params = params || []
    rfs = rfs || []
    msgs =
      case sync do
        :close_sync -> [msg_close(type: :statement, name: name), msg_sync()]
        :sync       -> [msg_sync()]
      end
    msgs = [
      msg_bind(name_port: "", name_stat: name, param_formats: pfs, params: params, result_formats: rfs),
      msg_execute(name_port: "", max_rows: 0) |
      msgs ]
    case msg_send(msgs, s) do
      :ok               -> bind_recv(s, status, query, buffer)
      {:error, _} = err -> err
    end
  end

  defp bind_recv(s, status, query, buffer) do
    %{sock: sock, timeout: timeout} = s
    case msg_recv(sock, buffer, timeout) do
      {:ok, msg_bind_complete(), buffer} ->
        execute_recv(s, status, query, buffer)
      {:ok, msg_error(fields: fields), buffer} ->
        sync_recv(s, status, Postgrex.Error.exception(postgres: fields), buffer)
      {:ok, msg, buffer} ->
        bind_recv(s, handle_msg(status, msg), query, buffer)
      {:error, _} = err ->
        err
    end
  end

  defp execute_recv(s, status, query, buffer) do
    %{sock: sock, timeout: timeout} = s
    case msg_recv(sock, buffer, timeout) do
      {:ok, msg_data_row(values: values), buffer} ->
        execute_recv(s, status, query, [values], buffer)
      {:ok, msg_command_complete(tag: tag), buffer} ->
        complete(s, status, query, [], tag, buffer)
      {:ok, msg_empty_query(), buffer} ->
        sync_recv(s, status, %Postgrex.Result{}, buffer)
      {:ok, msg_error(fields: fields), buffer} ->
        sync_recv(s, status, Postgrex.Error.exception(postgres: fields), buffer)
      {:ok, msg, buffer} ->
        execute_recv(s, handle_msg(status, msg), query, buffer)
      {:error, _} = err ->
        err
    end
  end

  defp execute_recv(s, status, query, rows, buffer) do
    %{sock: sock, timeout: timeout} = s
    case msg_recv(sock, buffer, timeout) do
      {:ok, msg_data_row(values: values), buffer} ->
        execute_recv(s, status, query, [values | rows], buffer)
      {:ok, msg_command_complete(tag: tag), buffer} ->
        complete(s, status, query, rows, tag, buffer)
      {:ok, msg, buffer} ->
        execute_recv(s, handle_msg(status, msg), query, buffer)
      {:error, _} = err ->
        err
    end
  end

  defp complete(s, status, %Query{columns: nil}, [], tag, buffer) do
    {command, nrows} = decode_tag(tag)
    result =  %Postgrex.Result{command: command, num_rows: nrows || 0}
    sync_recv(s, status, result, buffer)
  end
  defp complete(s, status, query, rows, tag, buffer) do
    {command, nrows} = decode_tag(tag)
    %Query{decoders: decoders, columns: cols} = query
    # Fix for PostgreSQL 8.4 (doesn't include number of selected rows in tag)
    if is_nil(nrows) and command == :select do
      nrows = length(rows)
    end
    result = %Postgrex.Result{command: command, num_rows: nrows || 0,
                              rows: rows, columns: cols,
                              decoders: decoders}
    sync_recv(s, status, result, buffer)
  end

  ## close

  defp close(s, status, %Query{name: name}, result, buffer) do
    msgs = [
      msg_close(type: :statement, name: name),
      msg_flush() ]
    case msg_send(msgs, s) do
      :ok               -> close_recv(s, status, result, buffer)
      {:error, _} = err -> err
    end
  end

  defp close_recv(s, status, result, buffer) do
    %{sock: sock, timeout: timeout} = s
    case msg_recv(sock, buffer, timeout) do
      {:ok, msg_close_complete(), buffer} ->
        ok(result, status, buffer)
      {:ok, msg_error(fields: fields), buffer} ->
        ok(Postgrex.Error.exception(postgres: fields), status, buffer)
      {:ok, msg, buffer} ->
        close_recv(s, handle_msg(status, msg), result, buffer)
      {:error, _} = err ->
        err
    end
  end

  ## data

  defp data(s, status \\ %{parameters: %{}, notifications: []}, buffer) do
    %{sock: sock, timeout: timeout} = s
    case msg_recv(sock, buffer, timeout) do
      {:ok, msg_error(fields: fields), _} ->
        {:error, Postgrex.Error.exception(postgres: fields)}
      {:ok, msg, <<>>} ->
        data_ready(s, handle_msg(status, msg))
      {:ok, msg, buffer} ->
        data(s, handle_msg(status, msg), buffer)
      {:error, _} = err ->
        err
    end
  end

  defp data_ready(%{sock: sock}, status) do
    case activate(sock, <<>>) do
      :ok ->
        %{parameters: parameters, notifications: notifications} = status
        {:ok, parameters, Enum.reverse(notifications)}
      {:error, _} = err ->
        err
    end
  end

  ## helpers

  defp encoders(oids, types) do
    oids
    |> Enum.map(&Types.encoder(&1, types))
    |> :lists.unzip()
  end

  defp columns(fields) do
    Enum.map(fields, fn row_field(type_oid: oid, name: name) ->
      {oid, name}
    end) |> :lists.unzip
  end

  defp decoders(oids, types) do
    oids
    |> Enum.map(&Types.decoder(&1, types))
    |> :lists.unzip()
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

  defp msg_recv({:gen_tcp, sock}, :active_once, timeout) do
    receive do
      {:tcp, ^sock, buffer} ->
        msg_recv(sock, buffer, timeout)
      {:tcp_closed, ^sock} ->
        {:error, :closed}
      {:tcp_error, ^sock, reason} ->
        {:error, reason}
    after
      timeout ->
        {:error, timeout}
    end
  end
  defp msg_recv({:ssl, sock}, :active_once, timeout) do
    receive do
      {:ssl, ^sock, buffer} ->
        msg_recv(sock, buffer, timeout)
      {:ssl_closed, ^sock} ->
        {:error, :closed}
      {:ssl_error, ^sock, reason} ->
        {:error, reason}
    after
      timeout ->
        {:error, timeout}
    end
  end
  defp msg_recv(sock, buffer, timeout) do
    case msg_decode(buffer) do
      {:ok, _, _} = ok -> ok
      {:more, more}    -> msg_recv(sock, buffer, more, timeout)
    end
  end

  defp msg_recv({mod, sock} = sock_info, buffer, more, timeout) do
    case mod.recv(sock, more, timeout) do
      {:ok, data} ->
        msg_recv(sock_info, buffer <> data, timeout)
      {:error, reason} ->
        {:error, Postgrex.Error.exception(tag: tag(mod), action: "recv", reason: reason)}
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

  defp msg_send(msg, %{sock: sock}), do: msg_send(msg, sock)

  defp msg_send(msgs, sock) when is_list(msgs) do
    binaries = Enum.reduce(msgs, [], &[&2 | encode_msg(&1)])
    do_send(sock, binaries)
  end

  defp msg_send(msg, sock) do
    do_send(sock, encode_msg(msg))
  end

  defp do_send({mod, sock}, data) do
    case mod.send(sock, data) do
      :ok ->
        :ok
      {:error, reason} ->
        {:error, Postgrex.Error.exception(tag: tag(mod), action: "send", reason: reason)}
    end
  end

  ## TODO: See if :binary.copy/1 of parameters/notifications reduces memory
  defp handle_msg(status, msg_parameter(name: name, value: value)) do
    update_in(status.parameters, &Map.put(&1, name, value))
  end
  defp handle_msg(status, msg_notify(channel: channel, payload: payload)) do
    update_in(status.notifications, &[{channel, payload} | &1])
  end
  defp handle_msg(status, msg_notice()) do
    # TODO: subscribers
    status
  end

  defp ok(result, %{ok: :status} = status, buffer) do
    {:ok, result, status, buffer}
  end
  defp ok(result, %{ok: :result} = status, buffer) do
    %{parameters: parameters, notifications: notifications} = status
    {:ok, result, parameters, Enum.reverse(notifications), buffer}
  end
  defp ok(nil, %{ok: :no_result} = status, buffer) do
    %{parameters: parameters, notifications: notifications} = status
    {:ok, parameters, Enum.reverse(notifications), buffer}
  end

  defp sync_recv(s, status, result, buffer) do
    %{sock: sock, timeout: timeout} = s
    case msg_recv(sock, buffer, timeout) do
      {:ok, msg_ready(), buffer} ->
        ok(result, status, buffer)
      {:ok, msg_close_complete(), buffer} ->
        sync_recv(s, status, result, buffer)
      {:ok, msg, buffer} ->
        sync_recv(s, handle_msg(status, msg), result, buffer)
      {:error, _} = err ->
        err
    end
  end

  defp recv_buffer(%{sock: {:gen_tcp, sock}}) do
    receive do
      {:tcp, ^sock, buffer} ->
        {:ok, buffer}
      {:tcp_closed, ^sock} ->
        {:error, Postgrex.Error.exception(tag: :tcp, action: "async recv", reason: :closed)}
      {:tcp_error, ^sock, reason} ->
        {:error, Postgrex.Error.exception(tag: :tcp, action: "async recv", reason: reason)}
    after
      0 ->
        {:ok, <<>>}
    end
  end
  defp recv_buffer(%{sock: {:ssl, sock}}) do
    receive do
      {:ssl, ^sock, buffer} ->
        {:ok, buffer}
      {:ssl_closed, ^sock} ->
        {:error, Postgrex.Error.exception(tag: :ssl, action: "async recv", reason: :closed)}
      {:ssl_error, ^sock, reason} ->
        {:error, Postgrex.Error.exception(tag: :ssl, action: "async recv", reason: reason)}
    after
      0 ->
        {:ok, <<>>}
    end
  end

  ## Fake [active: once] if buffer not empty
  defp activate(sock, <<>>) do
    setopts(sock, [active: :once])
  end
  defp activate({mod, sock}, buffer) do
    _ = send(self(), {tag(mod), sock, buffer})
    :ok
  end

  defp setopts({mod, sock}, opts) do
    case setopts(mod, sock, opts) do
      :ok ->
        :ok
      {:error, reason} ->
        {:error, Postgrex.Error.exception(tag: tag(mod), action: "setopts", reason: reason)}
    end
  end

  defp setopts(:gen_tcp, sock, opts), do: :inet.setopts(sock, opts)
  defp setopts(:ssl, sock, opts), do: :ssl.setopts(sock, opts)
end
