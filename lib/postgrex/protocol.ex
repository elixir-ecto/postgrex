defmodule Postgrex.Protocol do
  @moduledoc false

  alias Postgrex.Connection
  alias Postgrex.Types
  import Postgrex.Messages
  import Postgrex.Utils
  import Postgrex.BinaryUtils
  require Logger

  @timeout 5000
  @default_extensions [{Postgrex.Extensions.Binary, nil}, {Postgrex.Extensions.Text, nil}]
  @sock_opts [active: false, mode: :binary]

  def init(opts) do
    host       = Keyword.fetch!(opts, :hostname) |> to_char_list
    port       = opts[:port] || 5432
    timeout    = opts[:timeout] || @timeout
    sock_opts  = [{:packet, :raw}, :binary] ++ (opts[:socket_options] || [])
    custom     = opts[:extensions] || []
    extensions = custom ++ @default_extensions
    ssl?       = opts[:ssl] || false

    types_key = {host, port, Keyword.fetch!(opts, :database), custom}
    s = %{sock: nil, tail: "", state: :init, parameters: %{}, backend_key: nil,
          rows: [], statement: nil, portal: nil, types: nil,
          queue: :queue.new, timeout: timeout, extensions: extensions,
          listeners: HashDict.new, listener_channels: HashDict.new,
          types_key: types_key}
    case connect(host, port, sock_opts, s) do
      {:ok, s} when ssl? -> ssl(s, opts)
      {:ok, s}           -> startup(s, opts)
      {:stop, _} = stop  -> stop
    end
  end

  def send_query(statement, s) do
    msgs = [
      msg_parse(name: "", query: statement, type_oids: []),
      msg_describe(type: :statement, name: ""),
      msg_flush() ]

    case send_to_result(msgs, s) do
      {:ok, s} ->
        {:ok, %{s | statement: nil, state: :parsing}}
      err ->
        err
    end
  end

  # possible states: init, parsing, describing, binding, executing, ready

  ### parsing state ###

  def message(:parsing, msg_parse_complete(), s) do
    {:ok, %{s | state: :describing}}
  end

  ### describing state ###

  def message(:describing, msg_no_data(), s) do
    send_params(s, [])
  end

  def message(:describing, msg_parameter_desc(type_oids: oids), s) do
    {:ok, %{s | portal: oids}}
  end

  def message(:describing, msg_row_desc(fields: fields), s) do
    {col_oids, col_names} = columns(fields)
    try do
      result_formats = result_formats(col_oids, s.types)
      stat = %{columns: col_names, column_oids: col_oids}
      send_params(%{s | statement: stat}, result_formats)
    catch
      kind, reason ->
        reply({:error, kind, reason, System.stacktrace}, s)
        send_to_result([msg_sync], s)
    end
  end

  ### binding state ###

  def message(:binding, msg_bind_complete(), s) do
    {:ok, %{s | state: :executing}}
  end

  ### executing state ###

  def message(:executing, msg_data_row(values: values), s) do
    {:ok, %{s | rows: [values|s.rows]}}
  end

  def message(:executing, msg_command_complete(tag: tag), s) do
    {command, nrows} = decode_tag(tag)

    reply =
      if is_nil(s.statement) do
        %Postgrex.Result{command: command, num_rows: nrows || 0}
      else
        %{statement: %{column_oids: col_oids, columns: cols},
          types: types, rows: rows} = s

        # Fix for PostgreSQL 8.4 (doesn't include number of selected rows in tag)
        if is_nil(nrows) and command == :select do
          nrows = length(rows)
        end

        try do
          decode_rows(rows, col_oids, types)
        catch
          kind, reason ->
            {:error, kind, reason, System.stacktrace}
        else
          decoded ->
            %Postgrex.Result{command: command, num_rows: nrows || 0,
                             rows: decoded, columns: cols}
        end
      end

    reply(reply, s)
    {:ok, %{s | rows: [], statement: nil, portal: nil}}
  end

  def message(:executing, msg_empty_query(), s) do
    reply(%Postgrex.Result{}, s)
    {:ok, s}
  end

  ### asynchronous messages ###

  def message(_, msg_ready(), s) do
    queue = :queue.drop(s.queue)
    Connection.next(%{s | queue: queue, state: :ready})
  end

  def message(_, msg_parameter(name: name, value: value), s) do
    params = Map.put(s.parameters, name, value)
    {:ok, %{s | parameters: params}}
  end

  def message(state, msg_error(fields: fields), s) do
    error = Postgrex.Error.exception(postgres: fields)
    unless reply(error, s) do
      Logger.warn(fn ->
        ["Unhandled Postgres error: ", Postgrex.Error.message(error)]
      end)
    end
    if state in [:parsing, :describing] do
      # Issue a Sync to get back into valid state when error happened before bind/execute/sync.
      send_to_result([msg_sync], s)
    else
      {:ok, s}
    end
  end

  def message(_, msg_notice(), s) do
    # TODO: subscribers
    {:ok, s}
  end

  def message(_, msg_notify(channel: channel, payload: payload), s) do
    refs = HashDict.get(s.listener_channels, channel) || []
    Enum.each(refs, fn ref ->
      {_channel, pid} = s.listeners[ref]
      send(pid, {:notification, self(), ref, channel, payload})
    end)
    {:ok, s}
  end

  ## connect

  defp connect(host, port, sock_opts, %{timeout: timeout} = s) do
    case :gen_tcp.connect(host, port, sock_opts ++ @sock_opts, timeout) do
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
        error(%Postgrex.Error{message: "tcp connect: #{reason}"}, s)
    end
  end

  ## ssl

  defp ssl(%{sock: sock} = s, opts) do
    case msg_send(msg_ssl_request(), sock) do
      :ok              -> ssl_recv(s, opts)
      {:error, reason} -> error(%Postgrex.Error{message: "tcp send: #{reason}"}, s)
    end
  end

  defp ssl_recv(%{sock: {:gen_tcp, sock}, timeout: timeout} = s, opts) do
    case :gen_tcp.recv(sock, 1, timeout) do
      {:ok, <<?S>>} ->
        ssl_connect(s, opts)
      {:ok, <<?N>>} ->
        error(%Postgrex.Error{message: "ssl not available"}, s)
      {:error, reason} ->
        error(%Postgrex.Error{message: "tcp recv: #{reason}"}, s)
    end
  end

  defp ssl_connect(%{sock: {:gen_tcp, sock}, timeout: timeout} = s, opts) do
    case :ssl.connect(sock, opts[:ssl_opts] || [], timeout) do
      {:ok, ssl_sock} ->
        startup(%{s | sock: {:ssl, ssl_sock}}, opts)
      {:error, reason} ->
        error(%Postgrex.Error{message: "ssl negotiation failed: #{reason}"}, s)
    end
  end

  ## startup

  defp startup(%{sock: sock} = s, opts) do
    params = opts[:parameters] || []
    user = Keyword.fetch!(opts, :username)
    database = Keyword.fetch!(opts, :database)
    msg = msg_startup(params: [user: user, database: database] ++ params)
    case msg_send(msg, sock) do
      :ok ->
        auth_recv(s, opts, <<>>)
      {:error, reason} ->
        error(%Postgrex.Error{message: "tcp send: #{reason}"}, s)
    end
  end

  ## auth

  defp auth_recv(%{sock: sock, timeout: timeout} = s, opts, buffer) do
    case msg_recv(sock, buffer, timeout) do
      {:ok, msg_auth(type: :ok), buffer} ->
        init_recv(s, buffer)
      {:ok, msg_auth(type: :cleartext), buffer} ->
        auth_cleartext(s, opts, buffer)
      {:ok, msg_auth(type: :md5, data: salt), buffer} ->
        auth_md5(s, opts, salt, buffer)
      {:ok, msg_error(fields: fields), _} ->
        error(Postgrex.Error.exception(postgres: fields), s)
      {:error, reason} ->
        error(%Postgrex.Error{message: "tcp recv: #{reason}"}, s)
    end
  end

  defp auth_cleartext(s, opts, buffer) do
    pass = Keyword.fetch!(opts, :password)
    auth_send(s, msg_password(pass: pass), opts, buffer)
  end

  defp auth_md5(s, opts, salt, buffer) do
    user = Keyword.fetch!(opts, :username)
    pass = Keyword.fetch!(opts, :password)

    digest = :crypto.hash(:md5, [pass, user])
    |> Base.encode16(case: :lower)
    digest = :crypto.hash(:md5, [digest, salt])
    |> Base.encode16(case: :lower)
    auth_send(s, msg_password(pass: ["md5", digest]), opts, buffer)
  end

  defp auth_send(%{sock: sock} = s, msg, opts, buffer) do
    case msg_send(msg, sock) do
      :ok ->
        auth_recv(s, opts, buffer)
      {:error, reason} ->
        error(%Postgrex.Error{message: "tcp send: #{reason}"}, s)
    end
  end

  ## init

  defp init_recv(%{sock: sock, timeout: timeout} = s, buffer) do
    case msg_recv(sock, buffer, timeout) do
      {:ok, msg_backend_key(pid: pid, key: key), buffer} ->
        init_recv(%{s | backend_key: {pid, key}}, buffer)
      {:ok, msg_ready(), buffer} ->
        bootstrap(s, buffer)
      {:ok, msg_error(fields: fields), _} ->
        error(Postgrex.Error.exception(postgres: fields), s)
      {:ok, msg, buffer} ->
        {:ok, s} = message(:init, msg, s)
        init_recv(s, buffer)
      {:error, reason} ->
        error(%Postgrex.Error{message: "tcp recv: #{reason}"}, s)
    end
  end

  ## bootstrap

  defp bootstrap(%{types_key: types_key} = s, buffer) do
    case Postgrex.TypeServer.fetch(types_key) do
      {:ok, table} ->
        bootstrap_ready(%{s | types: table}, buffer)
      {:lock, ref, table} ->
        bootstrap_send(%{s | types: table}, ref, buffer)
    end
  end

  defp bootstrap_ready(%{sock: {mod, sock}} = s, buffer) do
    tag = if mod == :gen_tcp, do: :tcp, else: :ssl
    send(self(), {tag, sock, buffer})
    {:ok, %{s | state: :ready}}
  end

  defp bootstrap_send(s, ref, buffer) do
    %{parameters: parameters, extensions: extensions, sock: sock} = s
    extension_keys = Enum.map(extensions, &elem(&1, 0))
    extension_opts = Types.prepare_extensions(extensions, parameters)
    matchers = Types.extension_matchers(extension_keys, extension_opts)
    version = parameters["server_version"] |> Postgrex.Utils.parse_version
    query = Types.bootstrap_query(matchers, version)
    msg = msg_query(query: query)
    case msg_send(msg, sock) do
      :ok ->
        bootstrap_recv(s, ref, {extension_keys, extension_opts}, buffer)
      {:error, reason} ->
        error(%Postgrex.Error{message: "tcp send: #{reason}"}, s)
    end
  end

  defp bootstrap_recv(s, ref, extension_info, buffer) do
    %{sock: sock, timeout: timeout} = s
    case msg_recv(sock, buffer, timeout) do
      {:ok, msg_row_desc(), buffer} ->
        bootstrap_recv(s, ref, extension_info, [], buffer)
      {:ok, msg_error(fields: fields), _} ->
        error(Postgrex.Error.exception(postgres: fields), s)
      {:ok, msg, buffer} ->
        {:ok, s} = message(:init, msg, s)
        bootstrap_recv(s, ref, extension_info, buffer)
      {:error, reason} ->
        error(%Postgrex.Error{message: "tcp recv: #{reason}"}, s)
    end
  end

  defp bootstrap_recv(s, ref, extension_info, rows, buffer) do
    %{sock: sock, timeout: timeout} = s
    case msg_recv(sock, buffer, timeout) do
      {:ok, msg_data_row(values: values), buffer} ->
        bootstrap_recv(s, ref, extension_info, [values | rows], buffer)
      {:ok, msg_command_complete(), buffer} ->
        bootstrap_types(s, ref, extension_info, rows, buffer)
      {:ok, msg_error(fields: fields), _} ->
        error(Postgrex.Error.exception(postgres: fields), s)
      {:ok, msg, buffer} ->
        {:ok, s} = message(:init, msg, s)
        bootstrap_recv(s, ref, extension_info, rows, buffer)
      {:error, reason} ->
        error(%Postgrex.Error{message: "tcp recv: #{reason}"}, s)
    end
  end

  defp bootstrap_types(s, ref, {extension_keys, extension_opts}, rows, buffer) do
    %{types: table} = s
    types = Types.build_types(rows)
    Types.associate_extensions_with_types(table, extension_keys, extension_opts, types)
    Postgrex.TypeServer.unlock(ref)
    bootstrap_await(s, buffer)
  end

  defp bootstrap_await(%{sock: sock, timeout: timeout} = s, buffer) do
    case msg_recv(sock, buffer, timeout) do
      {:ok, msg_ready(), buffer} ->
        bootstrap_ready(s, buffer)
      {:ok, msg_error(fields: fields), _} ->
        error(Postgrex.Error.exception(postgres: fields), s)
      {:ok, msg, buffer} ->
        {:ok, s} = message(:init, msg, s)
        bootstrap_await(s, buffer)
      {:error, reason}  ->
        error(%Postgrex.Error{message: "tcp recv: #{reason}"}, s)
    end
  end

  ### helpers ###

  defp decode_rows(rows, col_oids, types) do
    col_oids = List.to_tuple(col_oids)

    Enum.reduce(rows, [], fn values, acc ->
      {_, row} =
        Enum.reduce(values, {0, []}, fn
          nil, {count, list} ->
            {count + 1, [nil|list]}
          bin, {count, list} ->
            oid = elem(col_oids, count)
            decoded = Postgrex.Types.decode(oid, bin, types)
            {count + 1, [decoded|list]}
        end)
      [Enum.reverse(row)|acc]
    end)
  end

  defp send_params(s, rfs) do
    {msgs, s} =
      try do
        encode_params(s)
      catch
        kind, reason ->
          reply({:error, kind, reason, System.stacktrace}, s)
          {[msg_sync], %{s | portal: nil}}
      else
        {pfs, params} ->
          msgs = [
            msg_bind(name_port: "", name_stat: "", param_formats: pfs, params: params, result_formats: rfs),
            msg_execute(name_port: "", max_rows: 0),
            msg_sync() ]
          {msgs, %{s | state: :binding}}
      end

    case send_to_result(msgs, s) do
      {:ok, s} ->
        {:ok, s}
      err ->
        err
    end
  end

  defp encode_params(%{queue: queue, portal: param_oids, types: types}) do
    %{command: {:query, _statement, params}} = :queue.get(queue)
    zipped = Enum.zip(param_oids, params)

    Enum.map(zipped, fn
      {_oid, nil} ->
        {:binary, <<-1::int32>>}
      {oid, param} ->
        format = Types.format(oid, types)
        binary = Types.encode(oid, param, types)
        {format, [<<IO.iodata_length(binary)::int32>>, binary]}
    end)
    |> :lists.unzip
  end

  defp columns(fields) do
    Enum.map(fields, fn row_field(type_oid: oid, name: name) ->
      {oid, name}
    end) |> :lists.unzip
  end

  defp result_formats(columns, types) do
    Enum.map(columns, &Types.format(&1, types))
  end

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

  defp msg_recv(sock, buffer, timeout) do
    case msg_decode(buffer) do
      {:ok, _, _} = ok -> ok
      {:more, more}    -> msg_recv(sock, buffer, more, timeout)
    end
  end

  defp msg_recv({mod, sock} = sock_info, buffer, more, timeout) do
    case mod.recv(sock, more, timeout) do
      {:ok, data}         -> msg_recv(sock_info, buffer <> data, timeout)
      {:error, _} = error -> error
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
    mod.send(sock, data)
  end

  defp send_to_result(msg, s) do
    case msg_send(msg, s) do
      :ok ->
        {:ok, s}
      {:error, reason} ->
        {:error, %Postgrex.Error{message: "tcp send: #{reason}"} , s}
    end
  end
end
