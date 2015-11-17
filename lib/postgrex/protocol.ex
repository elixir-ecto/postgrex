defmodule Postgrex.Protocol do
  @moduledoc false

  alias Postgrex.Types
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

    s = %{sock: nil, backend_key: nil, types: nil, timeout: timeout}

    types_key = {host, port, Keyword.fetch!(opts, :database), custom}
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
  def checkout(%{sock: {:gen_tcp, sock}} = s, :active_once) do
    case :inet.setopts(sock, [active: :false]) do
      :ok ->
        recv_buffer(s)
      {:error, reason} ->
        {:error, Postgrex.Error.exception(tag: :tcp, action: "setopts", reason: reason)}
    end
  end
  def checkout(%{sock: {:ssl, sock}} = s, :active_once) do
    case :ssl.setopts(sock, [active: :false]) do
      :ok ->
        recv_buffer(s)
      {:error, reason} ->
        {:error, Postgrex.Error.exception(tag: :ssl, action: "setopts", reason: reason)}
    end
  end
  def checkout(_, buffer), do: {:ok, buffer}

  @spec checkin(state, binary) ::
    {:ok, parameters, notifications} | {:error, Postgrex.Error.t}
  def checkin(s, buffer) do
    checkin_recv(s, %{parameters: %{}, notifications: []}, buffer)
  end

  @spec await(state, binary) ::
    {:ok, parameters, notifications, binary} | {:error, Postgrex.Error.t}
  def await(s, buffer) do
    await_recv(s, %{parameters: %{}, notifications: []}, buffer)
  end

  @spec query(state, String.t, [any], binary | :active_once) ::
    {:ok, Postgrex.Result.t | Postgrex.Error.t |
      {:error | :throw | :exit, any, list}, parameters, notifications, binary} |
    {:error, Postgrex.Error.t}
  def query(s, statement, params, buffer) do
    status = %{parameters: %{}, notifications: [], portal: nil,
               decoders: nil, columns: nil}
    case describe(s, status, statement, buffer) do
      {:ok, %Postgrex.Error{}, _, _, _} = ok ->
        ok
      {:execute, rfs, status, buffer} ->
        execute(s, status, params, rfs, buffer)
      {:error, _} = error ->
        error
    end
  end

  @spec message(state, any) ::
    {:ok, parameters, notifications} | {:error, Postgrex.Error.t} | :unknown
  def message(%{sock: {:gen_tcp, sock}} = s, {:tcp, sock, data}) do
    data(s, data)
  end
  def message(%{sock: {:gen_tcp, sock}} = s, {:tcp_closed, sock}) do
    error(Postgrex.Error.exception(tag: :tcp, action: "async recv", reason: :closed), s)
  end
  def message(%{sock: {:gen_tcp, sock}} = s, {:tcp_error, sock, reason}) do
    error(Postgrex.Error.exception(tag: :tcp, action: "async recv", reason: reason), s)
  end
  def message(%{sock: {:ssl, sock}} = s, {:ssl, sock, data}) do
    data(s, data)
  end
  def message(%{sock: {:ssl, sock}} = s, {:ssl_closed, sock}) do
    error(Postgrex.Error.exception(tag: :ssl, action: "async recv", reason: :closed), s)
  end
  def message(%{sock: {:ssl, sock}} = s, {:ssl_error, sock, reason}) do
    error(Postgrex.Error.exception(tag: :ssl, action: "async recv", reason: reason), s)
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
        error(Postgrex.Error.exception(tag: :tcp, action: "connect", reason: reason),s)
    end
  end

  ## ssl

  defp ssl(%{sock: sock} = s, status) do
    case msg_send(msg_ssl_request(), sock) do
      :ok              -> ssl_recv(s, status)
      {:error, exception} -> error(exception, s)
    end
  end

  defp ssl_recv(%{sock: {:gen_tcp, sock}, timeout: timeout} = s, status) do
    case :gen_tcp.recv(sock, 1, timeout) do
      {:ok, <<?S>>} ->
        ssl_connect(s, status)
      {:ok, <<?N>>} ->
        error(%Postgrex.Error{message: "ssl not available"}, s)
      {:error, reason} ->
        error(Postgrex.Error.exception(tag: :tcp, action: "recv", reason: reason),s)
    end
  end

  defp ssl_connect(%{sock: {:gen_tcp, sock}, timeout: timeout} = s, status) do
    case :ssl.connect(sock, status.opts[:ssl_opts] || [], timeout) do
      {:ok, ssl_sock} ->
        startup(%{s | sock: {:ssl, ssl_sock}}, status)
      {:error, reason} ->
        error(Postgrex.Error.exception(tag: :ssl, action: "connect", reason: reason),s)
    end
  end

  ## startup

  defp startup(%{sock: sock} = s, %{opts: opts} = status) do
    params = opts[:parameters] || []
    user = Keyword.fetch!(opts, :username)
    database = Keyword.fetch!(opts, :database)
    msg = msg_startup(params: [user: user, database: database] ++ params)
    case msg_send(msg, sock) do
      :ok ->
        auth_recv(s, status, <<>>)
      {:error, exception} ->
        error(exception, s)
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
        error(Postgrex.Error.exception(postgres: fields), s)
      {:error, exception} ->
        error(exception, s)
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
      :ok ->
        auth_recv(s, status, buffer)
      {:error, exception} ->
        error(exception, s)
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
        error(Postgrex.Error.exception(postgres: fields), s)
      {:ok, msg, buffer} ->
        init_recv(s, handle_msg(status, msg), buffer)
      {:error, exception} ->
        error(exception, s)
    end
  end

  ## bootstrap

  defp bootstrap(s, %{types_key: types_key} = status, buffer) do
    case Postgrex.TypeServer.fetch(types_key) do
      {:ok, table} ->
        s = %{s | types: table}
        {:ok, parameters, notifications} = activate(s, status, buffer)
        {:ok, s, parameters, notifications}
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
    query = Types.bootstrap_query(matchers, version)
    msg = msg_query(query: query)
    case msg_send(msg, sock) do
      :ok ->
        status = %{status | extension_info: {extension_keys, extension_opts}}
        bootstrap_recv(s, status, buffer)
      {:error, exception} ->
        error(exception, s)
    end
  end

  defp bootstrap_recv(s, status, buffer) do
    %{sock: sock, timeout: timeout} = s
    case msg_recv(sock, buffer, timeout) do
      {:ok, msg_row_desc(), buffer} ->
        bootstrap_recv(s, status, [], buffer)
      {:ok, msg_error(fields: fields), _} ->
        error(Postgrex.Error.exception(postgres: fields), s)
      {:ok, msg, buffer} ->
        bootstrap_recv(s, handle_msg(status, msg), buffer)
      {:error, exception} ->
        error(exception, s)
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
        error(Postgrex.Error.exception(postgres: fields), s)
      {:ok, msg, buffer} ->
        bootstrap_recv(s, handle_msg(status, msg), rows, buffer)
      {:error, exception} ->
        error(exception, s)
    end
  end

  defp bootstrap_types(%{types: table} = s, status, rows, buffer) do
    %{types_ref: ref, extension_info: {extension_keys, extension_opts}} = status
    types = Types.build_types(rows)
    Types.associate_extensions_with_types(table, extension_keys, extension_opts, types)
    Postgrex.TypeServer.unlock(ref)
    bootstrap_await(s, status, buffer)
  end

  defp bootstrap_await(s, status, buffer) do
    case checkin_recv(s, status, buffer) do
      {:ok, parameters, notifications} ->
        {:ok, s, parameters, notifications}
      {:error, _} = error ->
        error
    end
  end

  ## describe

  defp describe(s, status, statement, buffer) do
    msgs = [
      msg_parse(name: "", query: statement, type_oids: []),
      msg_describe(type: :statement, name: ""),
      msg_flush() ]
    case msg_send(msgs, s) do
      :ok ->
        parse_recv(s, status, buffer)
      {:error, exception} ->
        error(exception, s)
    end
  end

  defp parse_recv(s, status, buffer) do
    %{sock: sock, timeout: timeout} = s
    case msg_recv(sock, buffer, timeout) do
      {:ok, msg_parse_complete(), buffer} ->
        describe_recv(s, status, buffer)
      {:ok, msg_error(fields: fields), buffer} ->
        error(Postgrex.Error.exception(postgres: fields), s, status, buffer)
      {:ok, msg, buffer} ->
        parse_recv(s, handle_msg(status, msg), buffer)
      {:error, exception} ->
        error(exception, s)
    end
  end

  defp describe_recv(s, status, buffer) do
    %{sock: sock, timeout: timeout} = s
    case msg_recv(sock, buffer, timeout) do
      {:ok, msg_no_data(), buffer} ->
        {:execute, [], status, buffer}
      {:ok, msg_parameter_desc(type_oids: param_oids), buffer} ->
        describe_recv(s, %{status | portal: param_oids}, buffer)
      {:ok, msg_row_desc(fields: fields), buffer} ->
        describe_fields(s, status, fields, buffer)
      {:ok, msg_error(fields: fields), buffer} ->
        error(Postgrex.Error.exception(postgres: fields), s, status, buffer)
      {:ok, msg, buffer} ->
        describe_recv(s, handle_msg(status, msg), buffer)
      {:error, exception} ->
        error(exception, s)
    end
  end

  defp describe_fields(s, status, fields, buffer) do
    {col_oids, col_names} = columns(fields)
    try do
      decoders(col_oids, s.types)
    catch
      kind, reason ->
        sync_error(kind, reason, System.stacktrace, s, status, buffer)
    else
      {formats, decoders} ->
        status = %{status | columns: col_names, decoders: decoders}
        {:execute, formats, status, buffer}
    end
  end

  ## execute

  defp execute(s, status, params, rfs, buffer) do
    try do
      encode_params(s, status, params)
    catch
      kind, reason ->
        sync_error(kind, reason, System.stacktrace, s, status, buffer)
    else
      {pfs, params} ->
        execute(s, status, pfs, params, rfs, buffer)
    end
  end

  defp execute(s, status, pfs, params, rfs, buffer) do
    msgs = [
      msg_bind(name_port: "", name_stat: "", param_formats: pfs, params: params, result_formats: rfs),
      msg_execute(name_port: "", max_rows: 0),
      msg_sync() ]
    case msg_send(msgs, s) do
      :ok ->
        bind_recv(s, status, buffer)
      {:error, exception} ->
        error(exception, s)
    end
  end

  defp bind_recv(s, status, buffer) do
    %{sock: sock, timeout: timeout} = s
    case msg_recv(sock, buffer, timeout) do
      {:ok, msg_bind_complete(), buffer} ->
        execute_recv(s, status, buffer)
      {:ok, msg_error(fields: fields), _} ->
        error(Postgrex.Error.exception(postgres: fields), s)
      {:ok, msg, buffer} ->
        bind_recv(s, handle_msg(status, msg), buffer)
      {:error, exception} ->
        error(exception, s)
    end
  end

  defp execute_recv(s, status, buffer) do
    %{sock: sock, timeout: timeout} = s
    case msg_recv(sock, buffer, timeout) do
      {:ok, msg_data_row(values: values), buffer} ->
        execute_recv(s, status, [values], buffer)
      {:ok, msg_command_complete(tag: tag), buffer} ->
        complete(s, status, [], tag, buffer)
      {:ok, msg_empty_query(), buffer} ->
        ok(%Postgrex.Result{}, s, status, buffer)
      {:ok, msg_error(fields: fields), buffer} ->
        error(Postgrex.Error.exception(postgres: fields), s, status, buffer)
      {:ok, msg, buffer} ->
        execute_recv(s, handle_msg(status, msg), buffer)
      {:error, exception} ->
        error(exception, s)
    end
  end

  defp execute_recv(s, status, rows, buffer) do
    %{sock: sock, timeout: timeout} = s
    case msg_recv(sock, buffer, timeout) do
      {:ok, msg_data_row(values: values), buffer} ->
        execute_recv(s, status, [values | rows], buffer)
      {:ok, msg_command_complete(tag: tag), buffer} ->
        complete(s, status, rows, tag, buffer)
      {:ok, msg_error(fields: fields), _} ->
        error(Postgrex.Error.exception(postgres: fields), s)
      {:ok, msg, buffer} ->
        execute_recv(s, handle_msg(status, msg), buffer)
      {:error, exception} ->
        error(exception, s)
    end
  end

  defp complete(s, %{columns: nil} = status, [], tag, buffer) do
    {command, nrows} = decode_tag(tag)
    result =  %Postgrex.Result{command: command, num_rows: nrows || 0}
    ok(result, s, status, buffer)
  end
  defp complete(s, status, rows, tag, buffer) do
    {command, nrows} = decode_tag(tag)
    %{decoders: decoders, columns: cols} = status
    # Fix for PostgreSQL 8.4 (doesn't include number of selected rows in tag)
    if is_nil(nrows) and command == :select do
      nrows = length(rows)
    end
    result = %Postgrex.Result{command: command, num_rows: nrows || 0,
                              rows: rows, columns: cols,
                              decoders: decoders}
    ok(result, s, status, buffer)
  end

  ## checkin

  defp checkin_recv(%{sock: sock, timeout: timeout} = s, status, buffer) do
    case msg_recv(sock, buffer, timeout) do
      {:ok, msg_ready(), buffer} ->
        activate(s, status, buffer)
      {:ok, msg_error(fields: fields), _} ->
        error(Postgrex.Error.exception(postgres: fields), s)
      {:ok, msg, buffer} ->
        checkin_recv(s, handle_msg(status, msg), buffer)
      {:error, exception} ->
        error(exception, s)
    end
  end

  ## await

  defp await_recv(%{sock: sock, timeout: timeout} = s, status, buffer) do
    case msg_recv(sock, buffer, timeout) do
      {:ok, msg_ready(), buffer} ->
        ok(s, status, buffer)
      {:ok, msg_error(fields: fields), _} ->
        error(Postgrex.Error.exception(postgres: fields), s)
      {:ok, msg, buffer} ->
        await_recv(s, handle_msg(status, msg), buffer)
      {:error, exception} ->
        error(exception, s)
    end
  end

  ## data

  defp data(s, status \\ %{parameters: %{}, notifications: []}, buffer) do
    %{sock: sock, timeout: timeout} = s
    case msg_recv(sock, buffer, timeout) do
      {:ok, msg_error(fields: fields), _} ->
        error(Postgrex.Error.exception(postgres: fields), s)
      {:ok, msg, <<>>} ->
        activate(s, handle_msg(status, msg), <<>>)
      {:ok, msg, buffer} ->
        data(s, handle_msg(status, msg), buffer)
      {:error, exception} ->
        error(exception, s)
    end
  end

  ## helpers

  defp encode_params(%{types: types}, %{portal: param_oids}, params) when length(param_oids) == length(params) do
    zipped = Enum.zip(param_oids, params)

    Enum.map(zipped, fn
      {_oid, nil} ->
        {:binary, <<-1::int32>>}
      {oid, param} ->
        {format, encoder} = Types.encoder(oid, types)
        binary = encoder.(param)
        {format, [<<IO.iodata_length(binary)::int32>>, binary]}
    end)
    |> :lists.unzip
  end
  defp encode_params(_, %{portal: param_oids}, _) do
    raise ArgumentError, "parameters must be of length #{length param_oids} for this query"
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

  defp msg_send(msgs, {mod, sock}) when is_list(msgs) do
    binaries = Enum.reduce(msgs, [], &[&2 | encode_msg(&1)])
    mod.send(sock, binaries)
  end

  defp msg_send(msg, {mod, sock}) do
    data = encode_msg(msg)
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
  defp handle_msg(status, msg_ready()) do
    status
  end

  defp ok(_, status, buffer) do
    %{parameters: parameters, notifications: notifications} = status
    {:ok, parameters, Enum.reverse(notifications), buffer}
  end

  defp ok(result, _, status, buffer) do
    %{parameters: parameters, notifications: notifications} = status
    {:ok, result, parameters, Enum.reverse(notifications), buffer}
  end

  defp error(reason, _) do
    {:error, reason}
  end

  defp error(reason, s, status, buffer) do
    case msg_send(msg_sync(), s) do
      :ok ->
        ok(reason, s, status, buffer)
      {:error, exception} ->
        error(exception, s)
    end
  end

  defp sync_error(kind, reason, stack, s, status, buffer) do
    case msg_send(msg_sync(), s) do
      :ok ->
        ok({kind, reason, stack}, s, status, buffer)
      {:error, exception} ->
        error(exception, s)
    end
  end

  defp recv_buffer(%{sock: {:gen_tcp, sock}} = s) do
    receive do
      {:tcp, ^sock, buffer} ->
        {:ok, buffer}
      {:tcp_closed, ^sock} ->
        error(Postgrex.Error.exception(tag: :tcp, action: "async, recv", reason: :closed), s)
      {:tcp_error, ^sock, reason} ->
        error(Postgrex.Error.exception(tag: :tcp, action: "async, recv", reason: reason), s)
    after
      0 ->
        {:ok, <<>>}
    end
  end
  defp recv_buffer(%{sock: {:ssl, sock}} = s) do
    receive do
      {:ssl, ^sock, buffer} ->
        {:ok, buffer}
      {:ssl_closed, ^sock} ->
        error(Postgrex.Error.exception(tag: :ssl, action: "async recv", reason: :closed), s)
      {:ssl_error, ^sock, reason} ->
        error(Postgrex.Error.exception(tag: :ssl, action: "async recv", reason: reason), s)
    after
      0 ->
        {:ok, <<>>}
    end
  end

  defp activate(%{sock: sock}, status, buffer) do
    activate(sock, buffer)
    %{parameters: parameters, notifications: notifications} = status
    {:ok, parameters, Enum.reverse(notifications)}
  end

  ## Fake [active: once] if buffer not empty and delay error/closed to next call
  defp activate({:gen_tcp, sock}, <<>>) do
    case :inet.setopts(sock, [active: :once]) do
      :ok ->
        :ok
      {:error, :closed} ->
        _ = send(self(), {:tcp_closed, sock})
        :ok
      {:error, reason} ->
        _ = send(self(), {:tcp_error, sock, reason})
        :ok
    end
  end
  defp activate({:gen_tcp, sock}, buffer) do
    _ = send(self(), {:tcp, sock, buffer})
    :ok
  end
  defp activate({:ssl, sock}, <<>>) do
    case :ssl.setopts(sock, [active: :once]) do
      :ok ->
        :ok
      {:error, :closed} ->
        _ = send(self(), {:ssl_closed, sock})
        :ok
      {:error, reason} ->
        _ = send(self(), {:ssl_error, sock, reason})
        :ok
    end
  end
  defp activate({:ssl, sock}, buffer) do
    _ = send(self(), {:ssl, sock, buffer})
    :ok
  end
end
