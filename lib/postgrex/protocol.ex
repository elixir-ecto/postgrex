defmodule Postgrex.Protocol do
  @moduledoc false

  alias Postgrex.Types
  alias Postgrex.TypeServer
  alias Postgrex.Query
  alias Postgrex.Cursor
  alias Postgrex.Stream
  alias Postgrex.Copy
  import Postgrex.Messages
  import Postgrex.BinaryUtils
  require Logger
  use DBConnection

  @timeout 15_000
  @sock_opts [packet: :raw, mode: :binary, active: false]
  @max_packet 64 * 1024 * 1024 # max raw receive length
  @nonposix_errors [:closed, :timeout]
  @max_rows 500

  defstruct [sock: nil, connection_id: nil, connection_key: nil, peer: nil,
             types: nil, null: nil, timeout: nil, parameters: %{}, queries: nil,
             postgres: :idle, transactions: :naive, buffer: nil]

  @type state :: %__MODULE__{sock: {module, any},
                             connection_id: nil | pos_integer,
                             connection_key: nil | pos_integer,
                             peer: nil | {:inet.ip_address, :inet.port_number},
                             types: nil | module,
                             null: atom,
                             timeout: timeout,
                             parameters: %{binary => binary} | reference,
                             queries: nil | :ets.tid,
                             postgres: DBConnection.status |
                               {DBConnection.status, reference},
                             transactions: :strict | :naive,
                             buffer: nil | binary | :active_once}
  @type notify :: ((binary, binary) -> any)

  @spec connect(Keyword.t) ::
    {:ok, state} |
    {:error, Postgrex.Error.t | %DBConnection.ConnectionError{}}
  def connect(opts) do
    port = opts[:port] || 5432

    {host, port} =
      case Keyword.fetch(opts, :socket) do
        {:ok, socket} ->
          {{:local, "#{socket}/.s.PGSQL.#{port}"}, 0}

        :error ->
          case Keyword.fetch(opts, :hostname) do
            {:ok, hostname} ->
              {to_charlist(hostname), port}

            :error ->
              raise ArgumentError, "expected :hostname or :socket to be given"
          end
      end

    timeout    = opts[:timeout] || @timeout
    sock_opts  = [send_timeout: timeout] ++ (opts[:socket_options] || [])
    ssl?       = opts[:ssl] || false
    types_mod  = Keyword.fetch!(opts, :types)

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
                    transactions: transactions}

    types_key = if types_mod, do: {host, port, Keyword.fetch!(opts, :database)}
    status = %{opts: opts, types_mod: types_mod, types_key: types_key,
               types_lock: nil, prepare: prepare, ssl: ssl?}
    connect_timeout = Keyword.get(opts, :connect_timeout, timeout)

    case connect(host, port, sock_opts ++ @sock_opts, connect_timeout, s) do
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
    cancel_request(s)
    :ok
  end

  @spec ping(state) ::
    {:ok, state} |
    {:disconnect, Postgrex.Error.t | %DBConnection.ConnectionError{}, state}
  def ping(%{postgres: :transaction, transactions: :strict} = s) do
    sync_error(s, :transaction)
  end
  def ping(%{buffer: buffer} = s) do
    status = %{notify: notify([]), mode: :transaction}
    s = %{s | buffer: nil}
    case msg_send(s, msg_sync(), buffer) do
      :ok when buffer == :active_once ->
        ping_recv(s, status, :active_once, buffer)
      :ok when is_binary(buffer) ->
        ping_recv(s, status, nil, buffer)
      {:disconnect, _, _} = dis ->
        dis
    end
  end

  @spec checkout(state) ::
    {:ok, state} |
    {:disconnect, Postgrex.Error.t | %DBConnection.ConnectionError{}, state}
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
    {:ok, state} |
    {:disconnect, Postgrex.Error.t | %DBConnection.ConnectionError{}, state}
  def checkin(%{postgres: :transaction, transactions: :strict} = s) do
    sync_error(s, :transaction)
  end
  def checkin(%{buffer: buffer} = s) when is_binary(buffer) do
    activate(s, buffer)
  end

  @spec handle_prepare(Postgrex.Query.t | Postgrex.Stream.t, Keyword.t, state) ::
    {:ok, Postgrex.Query.t, state} |
    {:error, %ArgumentError{} | Postgrex.Error.t, state} |
    {:error, %DBConnection.TransactionError{}, state} |
    {:disconnect, %RuntimeError{}, state} |
    {:disconnect, %DBConnection.ConnectionError{}, state}
  def handle_prepare(%Query{} = query, _, %{postgres: {_, _}} = s) do
    lock_error(s, :prepare, query)
  end
  def handle_prepare(%Query{types: nil, name: ""} = query, opts, s) do
    function = Keyword.get(opts, :function)
    status = %{notify: notify(opts), mode: mode(opts), function: function}
    case function do
      :prepare ->
        parse_describe_close(s, status, query)
      _ ->
        parse_describe_flush(s, status, query)
    end
  end
  def handle_prepare(%Query{types: nil} = query, opts, %{queries: nil} = s) do
    # always use unnamed if no cache
    handle_prepare(%Query{query | name: ""}, opts, s)
  end
  def handle_prepare(%Query{types: nil} = query, opts,  s) do
    function = Keyword.get(opts, :function)
    status = %{notify: notify(opts), mode: mode(opts), function: function}
    case function do
      :prepare ->
        close_parse_describe(s, status, query)
      _ ->
        close_parse_describe_flush(s, status, query)
    end
  end
  def handle_prepare(%Query{types: types} = query, _, %{types: types} = s) do
    query_error(s, "query #{inspect query} has already been prepared")
  end
  def handle_prepare(%Query{} = query, _, s) do
    query_error(s, "query #{inspect query} has invalid types for the connection")
  end
  def handle_prepare(%Stream{query: query} = stream, opts, s) do
    case handle_prepare(query, opts, s) do
      {:ok, %Query{} = query, s} ->
        {:ok, %Stream{stream | query: query}, s}
      {error, _, _} = other when error in [:error, :disconnect] ->
        other
    end
  end

  @spec handle_execute(Postgrex.Parameters.t, nil, Keyword.t, state) ::
    {:ok, %{binary => binary}, state} |
    {:error, Postgrex.Error.t, state}
  def handle_execute(%Postgrex.Parameters{}, nil, _, s) do
    %{parameters: parameters} = s
    case Postgrex.Parameters.fetch(parameters) do
      {:ok, parameters} ->
        {:ok, parameters, s}
      :error ->
        {:error, %Postgrex.Error{message: "parameters not available"}, s}
    end
  end

  @spec handle_execute(Postgrex.Query.t, list, Keyword.t, state) ::
    {:ok, Postgrex.Result.t, state} |
    {:error, %ArgumentError{} | Postgrex.Error.t, state} |
    {:error, %DBConnection.TransactionError{}, state} |
    {:disconnect, %RuntimeError{}, state} |
    {:disconnect, %DBConnection.ConnectionError{}, state}
  def handle_execute(%Query{ref: ref, name: name} = query, params, opts,
      %{postgres: {_, ref}} = s) do
    # ref in lock so query is prepared
    status = %{notify: notify(opts), mode: mode(opts)}
    case name do
      "" ->
        bind_execute_close(s, status, query, params)
      _ ->
        bind_execute(s, status, query, params)
    end
  end
  def handle_execute(%Query{} = query, _, _, %{postgres: {_, _ref}} = s) do
    lock_error(s, :execute, query)
  end
  def handle_execute(%Query{types: nil} = query, _, _, s) do
    query_error(s, "query #{inspect query} has not been prepared")
  end
  def handle_execute(%Query{types: types} = query, params, opts,
      %{types: types} = s) do
    if query_member?(s, query) do
      status = %{notify: notify(opts), mode: mode(opts)}
      rebind_execute(s, status, query, params)
    else
      handle_prepare_execute(query, params, opts, s)
    end
  end
  def handle_execute(%Query{} = query, _, _, s) do
    query_error(s, "query #{inspect query} has invalid types for the connection")
  end

  @spec handle_execute(Postgrex.Stream.t, list, Keyword.t, state) ::
    {:ok, Copy.t, state} |
    {:error, %ArgumentError{} | Postgrex.Error.t, state} |
    {:disconnect, %RuntimeError{}, state} |
    {:disconnect, %DBConnection.ConnectionError{}, state}
  def handle_execute(%Stream{query: query}, params, opts, s) do
    %{connection_id: connection_id} = s
    copy = %Copy{portal: make_portal(), ref: make_ref(), query: query,
                 connection_id: connection_id}
    handle_bind(query, params, copy, opts, s)
  end

  @spec handle_execute(Postgrex.Copy.t, {:copy_data, iodata} | :copy_done,
                       Keyword.t, state) ::
    {:ok, Postgrex.Result.t, state} |
    {:error, %ArgumentError{} | Postgrex.Error.t, state} |
    {:disconnect, %RuntimeError{}, state} |
    {:disconnect, %DBConnection.ConnectionError{}, state}
  def handle_execute(%Copy{ref: ref} = copy, {:copy_data, iodata}, opts, s) do
    case s do
      %{postgres: {_, ^ref}} ->
        copy_in_data(s, iodata)
      %{postgres: {_, _}} ->
        lock_error(s, :execute, copy)
      _ ->
        status = %{notify: notify(opts), mode: mode(opts)}
        copy_in_data(s, status, copy, iodata)
    end
  end
  def handle_execute(%Copy{ref: ref} = copy, :copy_done, opts, s) do
    case s do
      %{postgres: {_, ^ref}} ->
        status = %{notify: notify(opts), mode: mode(opts)}
        copy_in_done(s, status, copy)
      %{postgres: {_, _}} ->
        lock_error(s, :execute, copy)
      _ ->
        status = %{notify: notify(opts), mode: mode(opts)}
        close(s, status, copy)
    end
  end

  @spec handle_close(Postgrex.Query.t | Postgrex.Stream.t, Keyword.t, state) ::
    {:ok, Postgrex.Result.t, state} |
    {:error, %ArgumentError{} | Postgrex.Error.t, state} |
    {:disconnect, %RuntimeError{}, state} |
    {:disconnect, %DBConnection.ConnectionError{}, state}
  def handle_close(%Query{ref: ref} = query, opts, %{postgres: {_, ref}} = s) do
    status = %{notify: notify(opts), mode: mode(opts)}
    flushed_close(s, status, query)
  end
  def handle_close(%Query{} = query, _, %{postgres: {_, _}} = s) do
    lock_error(s, :close, query)
  end
  def handle_close(%Query{} = query, opts, s) do
    status = %{notify: notify(opts), mode: mode(opts)}
    close(s, status, query)
  end
  def handle_close(%Stream{query: query}, opts, s) do
    handle_close(query, opts, s)
  end

  @spec handle_declare(Postgrex.Query.t, list, Keyword.t, state) ::
    {:ok, Postgrex.Cursor.t, state} |
    {:error, %ArgumentError{} | Postgrex.Error.t, state} |
    {:disconnect, %RuntimeError{}, state} |
    {:disconnect, %DBConnection.ConnectionError{}, state}
  def handle_declare(query, params, opts, s) do
    %{connection_id: connection_id} = s
    cursor = %Cursor{portal: make_portal(), ref: make_ref(),
                     connection_id: connection_id, mode: mode(opts)}
    handle_bind(query, params, cursor, opts, s)
  end

  @spec handle_fetch(Postgrex.Query.t, Postgrex.Cursor.t, Keyword.t, state) ::
    {:cont | :halt, Postgrex.Result.t, state} |
    {:error, Postgrex.Error.t, state} |
    {:disconnect, %RuntimeError{}, state} |
    {:disconnect, %DBConnection.ConnectionError{}, state}
  def handle_fetch(query, cursor, opts, %{postgres: {_, ref}} = s) do
    case cursor do
      %Cursor{ref: ^ref, mode: mode} ->
        status = %{notify: notify(opts), mode: mode}
        max_rows = Keyword.get(opts, :max_rows, @max_rows)
        fetch_copy_out(s, status, query, max_rows)
    _ ->
        lock_error(s, "fetch", cursor)
    end
  end
  def handle_fetch(query, cursor, opts, s) do
    status = %{notify: notify(opts), mode: mode(opts)}
    max_rows = Keyword.get(opts, :max_rows, @max_rows)
    execute(s, status, query, cursor, max_rows)
  end

  @spec handle_deallocate(Postgrex.Query.t, Postgrex.Cursor.t, Keyword.t, state) ::
    {:ok, Postgrex.Result.t, state} |
    {:error, Postgrex.Error.t, state} |
    {:disconnect, %RuntimeError{}, state} |
    {:disconnect, %DBConnection.ConnectionError{}, state}
  def handle_deallocate(query, %Cursor{ref: ref}, opts,
      %{postgres: {_, ref}} = s) do
    status = %{notify: notify(opts), mode: mode(opts)}
    copy_out_done(s, status, query)
  end
  def handle_deallocate(_, %Cursor{} = cursor, _, %{postgres: {_, _}} = s) do
    lock_error(s, :deallocate, cursor)
  end
  def handle_deallocate(_, %Cursor{} = cursor, opts, s) do
    status = %{notify: notify(opts), mode: :transaction}
    close(s, status, cursor)
  end

  @spec handle_begin(Keyword.t, state) ::
    {:ok, Postgrex.Result.t, state} |
    {DBConnection.status, state} |
    {:disconnect, %RuntimeError{}, state} |
    {:disconnect, %DBConnection.ConnectionError{} | Postgex.Error.t, state}
  def handle_begin(_, %{postgres: {_, _}} = s) do
    lock_error(s, :begin)
  end
  def handle_begin(opts, %{postgres: postgres} = s) do
    case Keyword.get(opts, :mode, :transaction) do
      :transaction when postgres == :idle ->
        statement = "BEGIN"
        handle_transaction(statement, opts, s)
      :savepoint when postgres == :transaction  ->
        statement = "SAVEPOINT postgrex_savepoint"
        handle_transaction(statement, opts, s)
      mode when mode in [:transaction, :savepoint] ->
        {postgres, s}
    end
  end

  @spec handle_commit(Keyword.t, state) ::
    {:ok, Postgrex.Result.t, state} |
    {DBConnection.status, state} |
    {:disconnect, %RuntimeError{}, state} |
    {:disconnect, %DBConnection.ConnectionError{} | Postgex.Error.t, state}
  def handle_commit(_, %{postgres: {_, _}} = s) do
    lock_error(s, :commit)
  end
  def handle_commit(opts, %{postgres: postgres} = s) do
    case Keyword.get(opts, :mode, :transaction) do
      :transaction when postgres == :transaction ->
        statement = "COMMIT"
        handle_transaction(statement, opts, s)
      :savepoint when postgres == :transaction ->
        statement = "RELEASE SAVEPOINT postgrex_savepoint"
        handle_transaction(statement, opts, s)
      mode when mode in [:transaction, :savepoint] ->
        {postgres, s}
    end
  end

  @spec handle_rollback(Keyword.t, state) ::
    {:ok, Postgrex.Result.t, state} |
    {DBConnection.status, state} |
    {:disconnect, %RuntimeError{}, state} |
    {:disconnect, %DBConnection.ConnectionError{} | Postgex.Error.t, state}
  def handle_rollback(_, %{postgres: {_, _}} = s) do
    lock_error(s, :rollback)
  end
  def handle_rollback(opts, %{postgres: postgres} = s) do
    case Keyword.get(opts, :mode, :transaction) do
      :transaction when postgres in [:transaction, :error] ->
        statement = "ROLLBACK"
        handle_transaction(statement, opts, s)
      :savepoint when postgres in [:transaction, :error] ->
        rollback_release =
          "ROLLBACK TO SAVEPOINT postgrex_savepoint;RELEASE SAVEPOINT postgrex_savepoint"
        handle_transaction(rollback_release, opts, s)
      mode when mode in [:transaction, :savepoint] ->
        {postgres, s}
    end
  end

  @spec handle_status(Keyword.t, state) :: {DBConnection.status, state}
  def handle_status(_, %{postgres: {postgres, _}} = s),
    do: {postgres, s}
  def handle_status(_, %{postgres: postgres} = s),
    do: {postgres, s}

  @spec handle_listener(String.t, Keyword.t, state) ::
    {:ok, Postgrex.Result.t, state} |
    {:error, Postgrex.Error.t, state} |
    {:disconnect, %DBConnection.ConnectionError{}, state}
  def handle_listener(statement, opts, s) do
    %{buffer: buffer, timeout: timeout, sock: sock} = s
    status = %{notify: notify(opts), mode: :transaction}
    timer = start_listener_timer(timeout, sock)
    result = listener(%{s | buffer: nil}, status, statement, buffer)
    cancel_listener_timer(timer)
    result
  end

  @spec handle_info(any, Keyword.t, state) ::
    {:ok, state} |
    {:error, Postgrex.Error.t, state} |
    {:disconnect, %DBConnection.ConnectionError{}, state}
  def handle_info(msg, opts \\ [], s)

  def handle_info({:tcp, sock, data}, opts, %{sock: {:gen_tcp, sock}} = s) do
    handle_data(s, opts, data)
  end
  def handle_info({:tcp_closed, sock}, _, %{sock: {:gen_tcp, sock}} = s) do
    disconnect(s, :tcp, "async recv", :closed)
  end
  def handle_info({:tcp_error, sock, reason}, _, %{sock: {:gen_tcp, sock}} = s) do
    disconnect(s, :tcp, "async recv", reason)
  end
  def handle_info({:ssl, sock, data}, opts, %{sock: {:ssl, sock}} = s) do
    handle_data(s, opts, data)
  end
  def handle_info({:ssl_closed, sock}, _, %{sock: {:ssl, sock}} = s) do
    disconnect(s, :ssl, "async recv", :closed)
  end
  def handle_info({:ssl_error, sock, reason}, _, %{sock: {:ssl, sock}} = s) do
    disconnect(s, :ssl, "async recv", reason)
  end
  def handle_info(msg, _, s) do
    Logger.info(fn() -> [inspect(__MODULE__), ?\s, inspect(self()),
      " received unexpected message: " | inspect(msg)]
    end)
    {:ok, s}
  end

  ## connect

  defp connect(host, port, sock_opts, timeout, s) do
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
        case host do
          {:local, socket_addr} ->
            {:error, conn_error(:tcp, "connect (#{socket_addr})", reason)}
          host ->
            {:error, conn_error(:tcp, "connect (#{host}:#{port})", reason)}
        end

    end
  end

  ## handshake

  defp handshake(%{sock: {:gen_tcp, sock}, timeout: timeout} = s, status) do
    {:ok, peer} = :inet.peername(sock)
    %{opts: opts} = status
    handshake_timeout = Keyword.get(opts, :handshake_timeout, timeout)
    timer = start_handshake_timer(handshake_timeout, sock)
    case do_handshake(%{s | peer: peer}, status) do
      {:ok, %{parameters: parameters} = s} ->
        cancel_handshake_timer(timer)
        ref = Postgrex.Parameters.insert(parameters)
        {:ok, %{s | parameters: ref}}
      {:disconnect, err, s} ->
        cancel_handshake_timer(timer)
        disconnect(err, s)
        {:error, err}
    end
  end

  defp start_handshake_timer(:infinity, _), do: :infinity
  defp start_handshake_timer(timeout, sock) do
    {:ok, tref} = :timer.apply_after(timeout, __MODULE__, :handshake_shutdown,
                                     [timeout, self(), sock])
    {:timer, tref}
  end

  @doc false
  def handshake_shutdown(timeout, pid, sock) do
    if Process.alive?(pid) do
      Logger.error(fn() ->
        [inspect(__MODULE__), " (", inspect(pid),
          ") timed out because it was handshaking for longer than ",
          to_string(timeout) | "ms"]
      end)
      :gen_tcp.shutdown(sock, :read_write)
    end
  end

  def cancel_handshake_timer(:infinity), do: :ok
  def cancel_handshake_timer({:timer, tref}) do
    {:ok, _} = :timer.cancel(tref)
    :ok
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
        disconnect(s, :tcp, "recv", reason)
    end
  end

  defp ssl_connect(%{sock: {:gen_tcp, sock}, timeout: timeout} = s, status) do
    case :ssl.connect(sock, status.opts[:ssl_opts] || [], timeout) do
      {:ok, ssl_sock} ->
        startup(%{s | sock: {:ssl, ssl_sock}}, status)
      {:error, reason} ->
        disconnect(s, :ssl, "connect", reason)
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
      {:ok, msg_backend_key(pid: pid, key: key), buffer} ->
        init_recv(%{s | connection_id: pid, connection_key: key}, status, buffer)
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
  defp bootstrap(s, status, buffer) do
    %{types_mod: types_mod, types_key: types_key} = status
    server = Postgrex.TypeManager.get(types_mod, types_key)
    case TypeServer.fetch(server) do
      {:lock, ref, types} ->
        status = %{status | types_lock: {server, ref}}
        bootstrap_send(%{s | types: types}, status, types, buffer)
      {:go, types} ->
        bootstrap_done(%{s | types: types}, status, buffer)
      :noproc ->
        bootstrap(s, status, buffer)
      :error ->
        {:disconnect, type_fetch_error(), %{s | buffer: buffer}}
    end
  end

  defp bootstrap_send(s, status, types, buffer) do
    %{parameters: parameters} = s
    bootstrap_send(s, status, types, parameters, buffer, &bootstrap_sync_recv/3)
  end

  defp bootstrap_send(s, status, types, parameters, buffer, next) do
    version = parameters["server_version"] |> Postgrex.Utils.parse_version
    statement = Types.bootstrap_query(version, types)
    msg = msg_query(statement: statement)
    case msg_send(s, msg, buffer) do
      :ok ->
        bootstrap_recv(s, status, [], buffer, next)
      {:disconnect, err, s} ->
        bootstrap_fail(s, err, status)
    end
  end

  defp bootstrap_recv(s, status, type_infos, buffer, next) do
    case msg_recv(s, :infinity, buffer) do
      {:ok, msg_row_desc(), buffer} ->
        bootstrap_recv(s, status, type_infos, buffer, next)
      {:ok, msg_data_row(values: values), buffer} ->
        type_infos = [Types.build_type_info(values) | type_infos]
        bootstrap_recv(s, status, type_infos, buffer, next)
      {:ok, msg_command_complete(), buffer} ->
        bootstrap_types(s, status, Enum.reverse(type_infos), buffer, next)
      {:ok, msg_error(fields: fields), buffer} ->
        err = Postgrex.Error.exception(postgres: fields)
        bootstrap_fail(s, err, status, buffer)
      {:ok, msg, buffer} ->
        s = handle_msg(s, status, msg)
        bootstrap_recv(s, status, type_infos, buffer, next)
      {:disconnect, err, s} ->
        bootstrap_fail(s, err, status)
    end
  end

  defp bootstrap_types(s, status, type_infos, buffer, next) do
    query_delete(s, %Query{name: "", statement: ""})
    %{types_lock: {server, ref}} = status
    TypeServer.update(server, ref, type_infos)
    next.(s, status, buffer)
  end

  defp bootstrap_sync_recv(s, status, buffer) do
    case msg_recv(s, :infinity, buffer) do
      {:ok, msg_ready(status: :idle), buffer} ->
        bootstrap_done(s, status, buffer)
      {:ok, msg_ready(status: postgres), buffer} ->
        sync_error(s, postgres, buffer)
      {:ok, msg, buffer} ->
        bootstrap_sync_recv(handle_msg(s, status, msg), status, buffer)
      {:disconnect, _, _} = dis ->
        dis
    end
  end

  defp bootstrap_done(s, %{prepare: :unnamed}, buffer),
    do: activate(s, buffer)
  defp bootstrap_done(s, %{prepare: :named}, buffer),
    do: activate(%{s | queries: queries_new()}, buffer)

  defp bootstrap_fail(s, err, %{types_lock: {server, ref}}) do
    TypeServer.fail(server, ref)
    {:disconnect, err, s}
  end

  defp bootstrap_fail(s, err, status, buffer) do
    bootstrap_fail(%{s | buffer: buffer}, err, status)
  end

  defp type_fetch_error() do
    msg = "awaited on another connection that failed to bootstrap types"
    RuntimeError.exception(message: msg)
  end

  ## listener

  defp listener(s, status, statement, buffer) do
    msgs = [msg_parse(name: "", statement: statement, type_oids: []),
            msg_bind(name_port: "", name_stat: "", param_formats: [], params: [], result_formats: []),
            msg_execute(name_port: "", max_rows: 0),
            msg_close(type: :statement, name: ""),
            msg_sync()]

    s = %{s | buffer: nil}
    with :ok <- msg_send(s, msgs, buffer),
         {:ok, s, buffer} <- recv_parse(s, status, buffer),
         {:ok, s, buffer} <- recv_bind(s, status, buffer),
         {:ok, result, s, buffer} <- recv_listener(s, status, buffer),
         {:ok, s, buffer} <- recv_close(s, status, buffer),
         {:ok, s} <- recv_ready(s, status, buffer) do
      {:ok, result, s}
    else
      {:error, %Postgrex.Error{} = err, s, buffer} ->
        error_ready(s, status, err, buffer)
      {:disconnect, _err, _s} = disconnect ->
        disconnect
    end
  end

  defp recv_listener(s, status, buffer) do
    case msg_recv(s, :infinity, buffer) do
      {:ok, msg_command_complete(tag: tag), buffer} ->
        {:ok, done(s, [tag]), s, buffer}
      {:ok, msg_error(fields: fields), buffer} ->
        {:error, Postgrex.Error.exception(postgres: fields), s, buffer}
      {:ok, msg, buffer} ->
        recv_listener(handle_msg(s, status, msg), status, buffer)
      {:disconnect, _, _} = dis ->
        dis
    end
  end

  defp start_listener_timer(:infinity, _), do: :infinity
  defp start_listener_timer(timeout, {mod, sock}) do
    {:ok, tref} = :timer.apply_after(timeout, mod, :close, [sock])
    {:timer, tref}
  end

  def cancel_listener_timer(:infinity), do: :ok
  def cancel_listener_timer({:timer, tref}) do
    {:ok, _} = :timer.cancel(tref)
    :ok
  end

  ## prepare

  defp parse_describe(s, %{mode: :transaction} = status, query) do
    msgs = parse_describe_msgs(query) ++ [msg_sync()]

    %{buffer: buffer} = s
    with :ok <- msg_send(%{s | buffer: nil}, msgs, buffer),
         {:ok, query, s, buffer} <-
           recv_parse_describe(s, status, query, buffer),
         {:ok, s} <- recv_ready(s, status, buffer) do
      {:ok, query, s}
    else
      {:reload, oid, s, buffer} ->
        reload_ready(s, status, query, oid, buffer)
      {:disconnect, err, s} ->
        {:disconnect, err, s}
      {:error, %Postgrex.Error{} = err, s, buffer} ->
        error_ready(s, status, err, buffer)
    end
  end
  defp parse_describe(%{postgres: :transaction} = s,
       %{mode: :savepoint} = status, query) do
    msgs = [msg_query(statement: "SAVEPOINT postgrex_query")] ++
            parse_describe_msgs(query) ++
           [msg_query(statement: "RELEASE SAVEPOINT postgrex_query")]

    %{buffer: buffer} = s
    with :ok <- msg_send(%{s | buffer: nil}, msgs, buffer),
         {:ok, _, %{buffer: buffer} = s} <- recv_transaction(s, status, buffer),
         {:ok, query, s, buffer}
          <- recv_parse_describe(s, status, query, buffer),
         {:ok, _, s} <- recv_transaction(s, status, buffer) do
      {:ok, query, s}
    else
      {:reload, oid, s, buffer} ->
        reload_transaction(s, status, query, oid, buffer)
      {:disconnect, err, s} ->
        {:disconnect, err, s}
      {:error, %Postgrex.Error{} = err, s, buffer} ->
        rollback_flushed(s, status, err, buffer)
    end
  end
  defp parse_describe(%{postgres: postgres} = s, %{mode: :savepoint}, _)
       when postgres in [:idle, :error] do
    transaction_error(s, postgres)
  end

  defp parse_describe_close(s, %{mode: :transaction} = status, query) do
    %Query{name: name} = query
    msgs = parse_describe_msgs(query) ++
           [msg_close(type: :statement, name: name),
            msg_sync()]

    %{buffer: buffer} = s
    with :ok <- msg_send(%{s | buffer: nil}, msgs, buffer),
         {:ok, query, s, buffer} <-
           recv_parse_describe(s, status, query, buffer),
         {:ok, s, buffer} <- recv_close(s, status, buffer),
         query_delete(s, query),
         {:ok, s} <- recv_ready(s, status, buffer) do
      {:ok, query, s}
    else
      {:reload, oid, s, buffer} ->
        reload_closed(s, status, query, oid, buffer)
      {:disconnect, err, s} ->
        {:disconnect, err, s}
      {:error, %Postgrex.Error{} = err, s, buffer} ->
        error_ready(s, status, err, buffer)
    end
  end
  defp parse_describe_close(s, %{mode: :savepoint} = status, query) do
    # only used for unnamed queries and the savepoint release will close the
    # query
    parse_describe(s, status, query)
  end

  defp parse_describe_flush(s, %{mode: :transaction} = status, query) do
    %{buffer: buffer} = s
    msgs = parse_describe_msgs(query) ++ [msg_flush()]

    with :ok <- msg_send(%{s | buffer: nil}, msgs, buffer),
         {:ok, %Query{ref: ref} = query, %{postgres: postgres} = s, buffer} <-
           recv_parse_describe(s, status, query, buffer) do
      # lock state with unique query reference as not synced
      {:ok, query, %{s | postgres: {postgres, ref}, buffer: buffer}}
    else
      {:error, err, s, buffer} ->
        error_flushed(s, status, err, buffer)
      {:reload, oid, s, buffer} ->
        reload_flushed(s, status, query, oid, buffer)
      {:disconnect, _err, _s} = disconnect ->
        disconnect
    end
  end
  defp parse_describe_flush(%{postgres: :transaction, buffer: buffer} = s,
       %{mode: :savepoint} = status, query) do
    msgs = [msg_query(statement: "SAVEPOINT postgrex_query")] ++
           parse_describe_msgs(query) ++ [msg_flush()]

    with :ok <- msg_send(%{s | buffer: nil}, msgs, buffer),
         {:ok, _, %{buffer: buffer} = s} <- recv_transaction(s, status, buffer),
         {:ok, %Query{ref: ref} = query, %{postgres: postgres} = s, buffer} <-
           recv_parse_describe(s, status, query, buffer) do
      # lock state with unique query reference as not synced
      {:ok, query, %{s | postgres: {postgres, ref}, buffer: buffer}}
    else
      {:error, err, s, buffer} ->
        rollback_flushed(s, status, err, buffer)
      {:reload, oid, s, buffer} ->
        reload_flushed(s, status, query, oid, buffer)
      {:disconnect, _err, _s} = disconnect ->
        disconnect
    end
  end
  defp parse_describe_flush(%{postgres: postgres} = s, %{mode: :savepoint}, _)
       when postgres in [:idle, :error] do
    transaction_error(s, postgres)
  end

  defp close_parse_describe(s, %{mode: :transaction} = status, query) do
    %Query{name: name} = query

    msgs = [msg_close(type: :statement, name: name)] ++
           parse_describe_msgs(query) ++ [msg_sync()]

    %{buffer: buffer} = s
    with :ok <- msg_send(%{s | buffer: nil}, msgs, buffer),
         {:ok, s, buffer} <- recv_close(s, status, buffer),
         query_delete(s, query),
         {:ok, query, s, buffer} <-
           recv_parse_describe(s, status, query, buffer),
         {:ok, s} <- recv_ready(s, status, buffer) do
      {:ok, query, s}
    else
      {:reload, oid, s, buffer} ->
        reload_ready(s, status, query, oid, buffer)
      {:disconnect, err, s} ->
        {:disconnect, err, s}
      {:error, %Postgrex.Error{} = err, s, buffer} ->
        error_ready(s, status, err, buffer)
    end
  end
  defp close_parse_describe(%{postgres: :transaction} = s,
       %{mode: :savepoint} = status, query) do
    %Query{name: name} = query
    msgs = [msg_query(statement: "SAVEPOINT postgrex_query"),
            msg_close(type: :statement, name: name)] ++
            parse_describe_msgs(query) ++
           [msg_query(statement: "RELEASE SAVEPOINT postgrex_query")]

    %{buffer: buffer} = s
    with :ok <- msg_send(%{s | buffer: nil}, msgs, buffer),
         {:ok, _, %{buffer: buffer} = s} <- recv_transaction(s, status, buffer),
         {:ok, s, buffer} <- recv_close(s, status, buffer),
         query_delete(s, query),
         {:ok, query, s, buffer}
          <- recv_parse_describe(s, status, query, buffer),
         {:ok, _, s} <- recv_transaction(s, status, buffer) do
      {:ok, query, s}
    else
      {:reload, oid, s, buffer} ->
        reload_transaction(s, status, query, oid, buffer)
      {:disconnect, err, s} ->
        {:disconnect, err, s}
      {:error, %Postgrex.Error{} = err, s, buffer} ->
        rollback_flushed(s, status, err, buffer)
    end
  end
  defp close_parse_describe(%{postgres: postgres} = s,
       %{mode: :savepoint}, _) when postgres in [:idle, :error] do
    transaction_error(s, postgres)
  end

  defp close_parse_describe_flush(s, %{mode: :transaction} = status, query) do
    %Query{name: name} = query
    msgs = [msg_close(type: :statement, name: name)] ++
           parse_describe_msgs(query) ++
           [msg_flush()]

    %{buffer: buffer} = s
    with :ok <- msg_send(%{s | buffer: nil}, msgs, buffer),
         {:ok, s, buffer} <- recv_close(s, status, buffer),
         query_delete(s, query),
         {:ok, %Query{ref: ref} = query, %{postgres: postgres} = s, buffer} <-
           recv_parse_describe(s, status, query, buffer) do
      # lock state with unique query reference as not synced
      {:ok, query, %{s | postgres: {postgres, ref}, buffer: buffer}}
    else
      {:error, err, s, buffer} ->
        error_flushed(s, status, err, buffer)
      {:reload, oid, s, buffer} ->
        reload_flushed(s, status, query, oid, buffer)
      {:disconnect, _err, _s} = disconnect ->
        disconnect
    end
  end
  defp close_parse_describe_flush(%{postgres: :transaction, buffer: buffer} = s,
       %{mode: :savepoint} = status, query) do
    %Query{name: name} = query
    msgs = [msg_query(statement: "SAVEPOINT postgrex_query"),
            msg_close(type: :statement, name: name)] ++
           parse_describe_msgs(query) ++
           [msg_flush()]

    with :ok <- msg_send(%{s | buffer: nil}, msgs, buffer),
         {:ok, _, %{buffer: buffer} = s} <- recv_transaction(s, status, buffer),
         {:ok, s, buffer} <- recv_close(s, status, buffer),
         {:ok, %Query{ref: ref} = query, %{postgres: postgres} = s, buffer} <-
           recv_parse_describe(s, status, query, buffer) do
      # lock state with unique query reference as not synced
      {:ok, query, %{s | postgres: {postgres, ref}, buffer: buffer}}
    else
      {:error, err, s, buffer} ->
        rollback_flushed(s, status, err, buffer)
      {:reload, oid, s, buffer} ->
        reload_flushed(s, status, query, oid, buffer)
      {:disconnect, _err, _s} = disconnect ->
        disconnect
    end
  end
  defp close_parse_describe_flush(%{postgres: postgres} = s,
       %{mode: :savepoint}, _) when postgres in [:idle, :error] do
    transaction_error(s, postgres)
  end

  defp parse_describe_msgs(query) do
    %Query{name: name, statement: statement, param_oids: param_oids} = query
    type_oids = param_oids || []
    [msg_parse(name: name, statement: statement, type_oids: type_oids),
     msg_describe(type: :statement, name: name)]
  end

  defp recv_parse_describe(s, status, %Query{ref: nil} = query, buffer) do
    with {:ok, s, buffer} <- recv_parse(s, status, buffer),
         {:ok, param_oids, result_oids, columns, s, buffer} <-
           recv_describe(s, status, buffer) do
      describe(s, query, param_oids, result_oids, columns, buffer)
    end
  end
  defp recv_parse_describe(s, status, query, buffer) do
    %Query{param_oids: param_oids, result_oids: result_oids,
           columns: columns} = query
    with {:ok, s, buffer} <- recv_parse(s, status, buffer),
         {:ok, ^param_oids, ^result_oids, ^columns, s, buffer}
          <- recv_describe(s, status, param_oids, buffer) do
      query_put(s, query)
      {:ok, query, s, buffer}
    else
      {:ok, ^param_oids, new_result_oids, new_columns, s, buffer} ->
        redescribe(s, query, new_result_oids, new_columns, buffer)
      {:error, %Postgrex.Error{}, _, _} = error ->
        error
      {:disconnect, _, _} = disconnect ->
        disconnect
    end
  end

  defp recv_parse(s, status, buffer) do
    case msg_recv(s, :infinity, buffer) do
      {:ok, msg_parse_complete(), buffer} ->
        {:ok, s, buffer}
      {:ok, msg_error(fields: fields), buffer} ->
        {:error, Postgrex.Error.exception(postgres: fields), s, buffer}
      {:ok, msg, buffer} ->
        recv_parse(handle_msg(s, status, msg), status, buffer)
      {:disconnect, _, _} = dis ->
        dis
    end
  end

  defp recv_describe(s, status, param_oids \\ [], buffer) do
    case msg_recv(s, :infinity, buffer) do
      {:ok, msg_no_data(), buffer} ->
        {:ok, param_oids, nil, nil, s, buffer}
      {:ok, msg_parameter_desc(type_oids: param_oids), buffer} ->
        recv_describe(s, status, param_oids, buffer)
      {:ok, msg_row_desc(fields: fields), buffer} ->
        {result_oids, columns} = columns(fields)
        {:ok, param_oids, result_oids, columns, s, buffer}
      {:ok, msg_too_many_parameters(len: len, max_len: max), buffer} ->
        msg = "postgresql protocol can not handle #{len} parameters, " <>
          "the maximum is #{max}"
        err = RuntimeError.exception(message: msg)
        {:disconnect, err, %{s | buffer: buffer}}
      {:ok, msg_error(fields: fields), buffer} ->
        {:error, Postgrex.Error.exception(postgres: fields), s, buffer}
      {:ok, msg, buffer} ->
        recv_describe(handle_msg(s, status, msg), status, param_oids, buffer)
      {:disconnect, _, _, _} = dis ->
        dis
    end
  end

  defp describe(s, query, param_oids, result_oids, columns, buffer) do
    with {:ok, query} <- describe_params(s, query, param_oids),
         {:ok, query} <- describe_result(s, query, result_oids, columns) do
      query_put(s, query)
      {:ok, query, s, buffer}
    else
      {:reload, oid} ->
        {:reload, oid, s, buffer}
      {:error, err} ->
        {:disconnect, err, %{s | buffer: buffer}}
    end
  end

  defp redescribe(s, query, result_oids, columns, buffer) do
    with {:ok, query} <- describe_result(s, query, result_oids, columns) do
      query_put(s, query)
      {:ok, query, s, buffer}
    else
      {:reload, oid} ->
        {:reload, oid, s, buffer}
      {:error, err} ->
        {:disconnect, err, %{s | buffer: buffer}}
    end
  end

  defp describe_params(%{types: types}, query, param_oids) do
    with {:ok, param_info} <- fetch_type_info(param_oids, types),
         {param_formats, param_types} = Enum.unzip(param_info) do
      query = %Query{query | param_oids: param_oids,
        param_formats: param_formats, param_types: param_types}
      {:ok, query}
    end
  end

  defp describe_result(%{types: types}, query, nil, nil) do
      query = %Query{query | ref: make_ref(), types: types, columns: nil,
        result_oids: nil, result_formats: [], result_types: nil}
      {:ok, query}
  end
  defp describe_result(%{types: types}, query, result_oids, columns) do
    with {:ok, result_info} <- fetch_type_info(result_oids, types),
         {result_formats, result_types} = Enum.unzip(result_info) do
      query = %Query{query | ref: make_ref(), types: types, columns: columns,
        result_oids: result_oids, result_formats: result_formats,
        result_types: result_types}
      {:ok, query}
    end
  end

  defp error_flushed(s, %{mode: :transaction} = status, err, buffer) do
    with :ok <- msg_send(s, [msg_sync()], buffer) do
      error_ready(s, status, err, buffer)
    else
      {:disconnect, _err, _s} = disconnect ->
        disconnect
    end
  end

  defp rollback_flushed(s, %{mode: :savepoint} = status, err, buffer) do
    rollback_release =
      "ROLLBACK TO SAVEPOINT postgrex_query;RELEASE SAVEPOINT postgrex_query"
    msgs = [msg_sync(),
            msg_query(statement: rollback_release)]

    with :ok <- msg_send(s, msgs, buffer),
         {:error, err, %{buffer: buffer} = s} <-
           error_ready(s, status, err, buffer),
         {:ok, _, s} <- recv_transaction(s, status, buffer) do
      {:error, err, s}
    else
      {:disconnect, _err, _s} = disconnect ->
        disconnect
    end
  end

  defp reload_transaction(s, status, query, oid, buffer) do
    %Query{name: name} = query
    msgs = [msg_close(type: :statement, name: name),
            msg_sync()]
    with {:ok, _, %{buffer: buffer} = s} <- recv_transaction(s, status, buffer),
         :ok <- msg_send(s, msgs, buffer) do
      reload_closed(s, status, query, oid, buffer)
    else
      {:disconnect, _err, _s} = disconnect ->
        disconnect
    end
  end

  defp reload_flushed(s, %{mode: :transaction} = status, query, oid, buffer) do
    %Query{name: name} = query
    msgs = [msg_close(type: :statement, name: name),
            msg_sync()]
    with :ok <- msg_send(s, msgs, buffer) do
      reload_closed(s, status, query, oid, buffer)
    else
      {:disconnect, _err, _s} = disconnect ->
        disconnect
    end
  end
  defp reload_flushed(s, %{mode: :savepoint} = status, query, oid, buffer) do
    %Query{name: name} = query
    rollback_release =
      "ROLLBACK TO SAVEPOINT postgrex_query;RELEASE SAVEPOINT postgrex_query"
    msgs = [msg_close(type: :statement, name: name),
            msg_query(statement: rollback_release)]

    with :ok <- msg_send(s, msgs,  buffer),
         {:ok, s, buffer} <- recv_close(s, status, buffer),
         {:ok, _, %{buffer: buffer} = s}
          <- recv_transaction(s, status, buffer) do
      reload_spawn(%{s | buffer: nil}, status, query, oid, buffer)
    else
      {:disconnect, _err, _s} = disconnect ->
        disconnect
    end
  end

  defp reload_ready(s, status, query, oid, buffer) do
    %Query{name: name} = query
    msgs = [msg_close(type: :statement, name: name),
            msg_sync()]
    with {:ok, %{buffer: buffer} = s} <- recv_ready(s, status, buffer),
         :ok <- msg_send(s, msgs, buffer) do
      reload_closed(s, status, query, oid, buffer)
    else
      {:disconnect, _err, _s} = disconnect ->
        disconnect
    end
  end

  defp reload_closed(s, status, query, oid, buffer) do
    with {:ok, s, buffer} <- recv_close(s, status, buffer),
         {:ok, %{buffer: buffer} = s} <- recv_ready(s, status, buffer) do
      reload_spawn(%{s | buffer: nil}, status, query, oid, buffer)
    else
      {:disconnect, _err, _s} = disconnect ->
        disconnect
    end
  end

  defp fetch_type_info(oids, types, infos \\ [])
  defp fetch_type_info([], _, infos) do
    {:ok, Enum.reverse(infos)}
  end
  defp fetch_type_info([oid | oids], types, infos) do
    case Postgrex.Types.fetch(oid, types) do
      {:ok, info} ->
        fetch_type_info(oids, types, [info | infos])
      {:error, %Postgrex.TypeInfo{} = info, mod} ->
        msg = Postgrex.Utils.type_msg(info, mod)
        {:error, RuntimeError.exception(message: msg)}
      {:error, nil, _} ->
        {:reload, oid}
    end
  end

  defp reload_spawn(s, status, query, oid, buffer) do
    Logger.warn(fn() ->
      [inspect(query) | " uses unknown oid `#{oid}` causing bootstrap"]
    end)
    ref = make_ref()
    {_, mon} = spawn_monitor(fn() -> reload_lock(s, status, ref, buffer) end)
    receive do
      {:DOWN, ^mon, _, _, {^ref, s, buffer}} ->
        reload_fetch(s, status, query, oid, buffer)
      {:DOWN, ^mon, _, _, _} ->
        {:disconnect, type_fetch_error(), %{s | buffer: buffer}}
    end
  end

  defp reload_lock(%{types: types} = s, status, exit_ref, buffer) do
    with {:ok, server} <- Postgrex.Types.owner(types),
         {:lock, lock_ref, ^types} <- TypeServer.fetch(server),
         status = Map.put(status, :types_lock, {server, lock_ref}),
         {:ok, s} <- reload_send(s, status, types, buffer) do
      %{buffer: buffer} = s
      exit({exit_ref, %{s | buffer: nil}, buffer})
    else
      {:go, ^types} ->
        exit({exit_ref, s, buffer})
      :noproc ->
        exit(:normal)
      :error ->
        exit(:normal)
      {error, err, _} when error in [:error, :disconnect] ->
        raise err
    end
  end

  defp reload_send(s, status, types, buffer) do
    %{parameters: parameters} = s
    case Postgrex.Parameters.fetch(parameters) do
      {:ok, parameters} ->
        status = %{status | mode: :transaction}
        bootstrap_send(s, status, types, parameters, buffer, &sync_recv/3)
      :error ->
        s = %{s | buffer: buffer}
        {:error, %Postgrex.Error{message: "parameters not available"}, s}
    end
  end

  defp reload_fetch(%{types: types} = s, status, query, oid, buffer) do
    case Postgrex.Types.fetch(oid, types) do
      {:ok, _} ->
        reload_prepare(%{s | buffer: buffer}, status, query)
      {:error, %Postgrex.TypeInfo{} = info, mod} ->
        msg = Postgrex.Utils.type_msg(info, mod)
        reload_error(s, msg, buffer)
      {:error, nil, _} ->
        msg = "oid `#{oid}` lacks type information after bootstrap"
        reload_error(s, msg, buffer)
    end
  end

  defp reload_prepare(s, %{function: function} = status, query) do
    %Query{name: name} = query
    case function do
      :prepare when name == "" ->
        # unnamed queries closed on prepare when not re-using
        parse_describe_close(s, status, query)
      :prepare ->
        # named queries closed when oid not found
        parse_describe(s, status, query)
      _ ->
        # flush awaiting execute or declare
        parse_describe_flush(s, status, query)
    end
  end

  defp reload_error(s, msg, buffer) do
    {:disconnect, RuntimeError.exception(message: msg), %{s | buffer: buffer}}
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
    {:disconnect, RuntimeError.exception(msg), s}
  end

  defp transaction_error(s, status) do
    {:error, DBConnection.TransactionError.exception(status), s}
  end

  defp handle_prepare_execute(%Query{name: "", ref: ref} = query, params, opts,
       s) do
    status = %{notify: notify(opts), mode: mode(opts),
      function: :prepare_execute}
    with {:ok, %Query{ref: new_ref} = new_query, s} when new_ref != ref
          <- parse_describe_flush(s, status, query),
         {:ok, result, s} <- bind_execute_close(s, status, new_query, params) do
      {:ok, new_query, result, s}
    else
      {:ok, %Query{ref: ^ref} = query, s} ->
        bind_execute_close(s, status, query, params)
      {error, _, _} = other when error in [:error, :disconnect] ->
        other
    end
  end
  defp handle_prepare_execute(%Query{ref: ref} = query, params, opts, s) do
    status = %{notify: notify(opts), mode: mode(opts),
      function: :prepare_execute}
    with {:ok, %Query{ref: new_ref} = new_query, s} when new_ref != ref
          <- close_parse_describe_flush(s, status, query),
         {:ok, result, s} <- bind_execute(s, status, new_query, params) do
      {:ok, new_query, result, s}
    else
      {:ok, %Query{ref: ^ref} = query, s} ->
        bind_execute(s, status, query, params)
      {error, _, _} = other when error in [:error, :disconnect] ->
        other
    end
  end

  defp bind_execute_close(s, %{mode: :transaction} = status, query, params) do
    %Query{param_formats: pfs, result_formats: rfs, name: name} = query
    msgs = [msg_bind(name_port: "", name_stat: name, param_formats: pfs, params: params, result_formats: rfs),
            msg_execute(name_port: "", max_rows: 0),
            msg_close(type: :statement, name: name),
            msg_sync()]

    %{buffer: buffer} = s
    with :ok <- msg_send(%{s | buffer: nil}, msgs,  buffer),
         {:ok, s, buffer} <- recv_bind(s, status, buffer),
         {:ok, result, s, buffer} <- recv_execute(s, status, query, buffer),
         {:ok, s, buffer} <- recv_close(s, status, buffer),
         {:ok, s} <- recv_ready(s, status, buffer) do
      {:ok, result, s}
    else
      {:error, %Postgrex.Error{} = err, s, buffer} ->
        error_ready(s, status, err, buffer)
      {:disconnect, _err, _s} = disconnect ->
        disconnect
    end
  end
  defp bind_execute_close(s, %{mode: :savepoint} = status, query, params) do
    # only used for un-named and query will always get closed by release
    bind_execute(s, status, query, params)
  end

  defp bind_execute(s, %{mode: :transaction} = status, query, params) do
    %Query{param_formats: pfs, result_formats: rfs, name: name} = query
    msgs = [msg_bind(name_port: "", name_stat: name, param_formats: pfs, params: params, result_formats: rfs),
            msg_execute(name_port: "", max_rows: 0),
            msg_sync()]

    %{buffer: buffer} = s
    with :ok <- msg_send(%{s | buffer: nil}, msgs,  buffer),
         {:ok, s, buffer} <- recv_bind(s, status, buffer),
         {:ok, result, s, buffer} <- recv_execute(s, status, query, buffer),
         {:ok, s} <- recv_ready(s, status, buffer) do
      {:ok, result, s}
    else
      {:error, %Postgrex.Error{} = err, s, buffer} ->
        error_ready(s, status, err, buffer)
      {:disconnect, _err, _s} = disconnect ->
        disconnect
    end
  end
  defp bind_execute(s, %{mode: :savepoint} = status, query, params) do
    %Query{param_formats: pfs, result_formats: rfs, name: name} = query
    msgs = [msg_bind(name_port: "", name_stat: name, param_formats: pfs, params: params, result_formats: rfs),
            msg_execute(name_port: "", max_rows: 0),
            msg_query(statement: "RELEASE SAVEPOINT postgrex_query")]

    %{buffer: buffer} = s
    with :ok <- msg_send(%{s | buffer: nil}, msgs,  buffer),
         {:ok, s, buffer} <- recv_bind(s, status, buffer),
         {:ok, result, s, buffer} <- recv_execute(s, status, query, buffer),
         {:ok, _, s} <- recv_transaction(s, status, buffer) do
      {:ok, result, s}
    else
      {:error, %Postgrex.Error{} = err, s, buffer} ->
        rollback_flushed(s, status, err, buffer)
      {:disconnect, _err, _s} = disconnect ->
        disconnect
    end
  end

  defp rebind_execute(s, %{mode: :transaction} = status, query, params) do
    # using a cached query is same as using it for the first time when don't
    # need to setup savepoints
    bind_execute(s, status, query, params)
  end
  defp rebind_execute(%{postgres: :transaction} = s,
       %{mode: :savepoint} = status, query, params) do
    # using a named cache query so savepoint/simple query does not unprepare
    %Query{param_formats: pfs, result_formats: rfs, name: name} = query
    msgs = [msg_query(statement: "SAVEPOINT postgrex_query"),
            msg_bind(name_port: "", name_stat: name, param_formats: pfs, params: params, result_formats: rfs),
            msg_execute(name_port: "", max_rows: 0),
            msg_query(statement: "RELEASE SAVEPOINT postgrex_query")]

    %{buffer: buffer} = s
    with :ok <- msg_send(%{s | buffer: nil}, msgs,  buffer),
         {:ok, _, %{buffer: buffer} = s} <- recv_transaction(s, status, buffer),
         {:ok, s, buffer} <- recv_bind(%{s | buffer: nil}, status, buffer),
         {:ok, result, s, buffer} <- recv_execute(s, status, query, buffer),
         {:ok, _, s} <- recv_transaction(s, status, buffer) do
      {:ok, result, s}
    else
      {:error, %Postgrex.Error{} = err, s, buffer} ->
        rollback_flushed(s, status, err, buffer)
      {:disconnect, _err, _s} = disconnect ->
        disconnect
    end
  end
  defp rebind_execute(%{postgres: postgres} = s,
       %{mode: :savepoint}, _, _) when postgres in [:idle, :error] do
    transaction_error(s, postgres)
  end

  defp recv_bind(s, status, buffer) do
    case msg_recv(s, :infinity, buffer) do
      {:ok, msg_bind_complete(), buffer} ->
        {:ok, s, buffer}
      {:ok, msg_error(fields: fields), buffer} ->
        {:error, Postgrex.Error.exception(postgres: fields), s, buffer}
      {:ok, msg, buffer} ->
        recv_bind(handle_msg(s, status, msg), status, buffer)
      {:disconnect, _, _} = dis ->
        dis
    end
  end

  defp recv_execute(s, status, query, rows \\ [], buffer) do
    %Query{result_types: types} = query
    case rows_recv(s, types, rows, buffer) do
      {:ok, msg_command_complete(tag: tag), rows, buffer} ->
        {:ok, done(s, query, rows, tag), s, buffer}
      {:ok, msg_error(fields: fields), _, buffer} ->
        {:error, Postgrex.Error.exception(postgres: fields), s, buffer}
      {:ok, msg_empty_query(), [], buffer} ->
        {:ok, done(s, query, nil, nil), s, buffer}
      {:ok, msg_copy_in_response(), [], buffer} ->
        copy_in_disconnect(s, query, buffer)
      {:ok, msg_copy_out_response(), [], buffer} ->
        recv_copy_out(s, status, query, buffer)
      {:ok, msg_copy_both_response(), [], buffer} ->
        copy_both_disconnect(s, query, buffer)
      {:ok, msg, rows, buffer} ->
        recv_execute(handle_msg(s, status, msg), status, query, rows, buffer)
      {:disconnect, _, _} = dis ->
        dis
    end
  end

  defp copy_in_disconnect(s, query, buffer) do
    msg = "query #{inspect query} is trying to copy in but no copy data to send"
    {:disconnect, RuntimeError.exception(msg), %{s | buffer: buffer}}
  end

  defp copy_both_disconnect(s, query, buffer) do
    msg = "query #{inspect query} is trying to copy both ways but it is not supported"
    {:disconnect, RuntimeError.exception(msg), %{s | buffer: buffer}}
  end

  defp recv_copy_out(s, status, query, acc \\ [], buffer) do
     case msg_recv(s, :infinity, buffer) do
      {:ok, msg_copy_data(data: data), buffer} ->
        recv_copy_out(s, status, query, [data | acc], buffer)
      {:ok, msg_copy_done(), buffer} ->
        recv_copy_out(s, status, query, acc, buffer)
      {:ok, msg_command_complete(tag: tag), buffer} ->
        {:ok, done(s, query, acc, tag), s, buffer}
      {:ok, msg_error(fields: fields), buffer} ->
        {:error, Postgrex.Error.exception(postgres: fields), s, buffer}
      {:ok, msg, buffer} ->
        s
        |> handle_msg(status, msg)
        |> recv_copy_out(status, query, acc, buffer)
      {:disconnect, _, _} = dis ->
        dis
    end
  end

  defp make_portal() do
    System.unique_integer([:positive])
    |> Integer.to_string(36)
  end

  defp handle_bind(%Query{ref: ref} = query, params, res, opts,
       %{postgres: {_, ref}} = s) do
    status = %{notify: notify(opts), mode: mode(opts)}
    bind(s, status, query, params, res)
  end
  defp handle_bind(query, _, _, _, %{postgres: {_, _}} = s) do
    lock_error(s, :bind, query)
  end
  defp handle_bind(%Query{types: nil} = query, _, _, _, s) do
    query_error(s, "query #{inspect query} has not been prepared")
  end
  defp handle_bind(%Query{types: types} = query, params, res, opts,
       %{types: types} = s) do
    if query_member?(s, query) do
      status = %{notify: notify(opts), mode: mode(opts)}
      rebind(s, status, query, params, res)
    else
      handle_prepare_bind(query, params, res, opts, s)
    end
  end
  defp handle_bind(%Query{} = query, _, _, _, s) do
    query_error(s, "query #{inspect query} has invalid types for the connection")
  end

  defp handle_prepare_bind(%Query{name: ""} = query, params, res, opts, s) do
    status = %{notify: notify(opts), mode: mode(opts), function: :prepare_bind}
    case parse_describe_flush(s, status, query) do
      {:ok, query, s} ->
        bind(s, status, query, params, res)
      {error, _, _} = other when error in [:error, :disconnect] ->
        other
    end
  end
  defp handle_prepare_bind(query, params, res, opts, s) do
    status = %{notify: notify(opts), mode: mode(opts), function: :prepare_bind}
    case close_parse_describe_flush(s, status, query) do
      {:ok, query, s} ->
        bind(s, status, query, params, res)
      {error, _, _} = other when error in [:error, :disconnect] ->
        other
    end
  end

  defp bind(s, %{mode: :transaction} = status, query, params, cursor) do
    %Query{param_formats: pfs, result_formats: rfs, name: name} = query
    %{portal: portal} = cursor
    msgs = [msg_bind(name_port: portal, name_stat: name, param_formats: pfs, params: params, result_formats: rfs),
            msg_sync()]

    %{buffer: buffer} = s
    with :ok <- msg_send(%{s | buffer: nil}, msgs,  buffer),
         {:ok, s, buffer} <- recv_bind(s, status, buffer),
         {:ok, s} <- recv_ready(s, status, buffer) do
      {:ok, cursor, s}
    else
      {:error, %Postgrex.Error{} = err, s, buffer} ->
        error_ready(s, status, err, buffer)
      {:disconnect, _err, _s} = disconnect ->
        disconnect
    end
  end
  defp bind(s, %{mode: :savepoint} = status, query, params, cursor) do
    %Query{param_formats: pfs, result_formats: rfs, name: name} = query
    %{portal: portal} = cursor
    msgs = [msg_bind(name_port: portal, name_stat: name, param_formats: pfs, params: params, result_formats: rfs),
            msg_query(statement: "RELEASE SAVEPOINT postgrex_query")]

    %{buffer: buffer} = s
    with :ok <- msg_send(%{s | buffer: nil}, msgs,  buffer),
         {:ok, s, buffer} <- recv_bind(s, status, buffer),
         {:ok, _, s} <- recv_transaction(s, status, buffer) do
      {:ok, cursor, s}
    else
      {:error, %Postgrex.Error{} = err, s, buffer} ->
        rollback_flushed(s, status, err, buffer)
      {:disconnect, _err, _s} = disconnect ->
        disconnect
    end
  end

  defp rebind(s, %{mode: :transaction} = status, query, params, cursor) do
    # using a cached query is same as using it for the first time when don't
    # need to setup savepoints
    bind(s, status, query, params, cursor)
  end
  defp rebind(%{postgres: :transaction} = s, %{mode: :savepoint} = status,
       query, params, cursor) do
    %Query{param_formats: pfs, result_formats: rfs, name: name} = query
    %{portal: portal} = cursor
    msgs = [msg_query(statement: "SAVEPOINT postgrex_query"),
            msg_bind(name_port: portal, name_stat: name, param_formats: pfs, params: params, result_formats: rfs),
            msg_query(statement: "RELEASE SAVEPOINT postgrex_query")]

    %{buffer: buffer} = s
    with :ok <- msg_send(%{s | buffer: nil}, msgs,  buffer),
         {:ok, _, %{buffer: buffer} = s} <- recv_transaction(s, status, buffer),
         {:ok, s, buffer} <- recv_bind(s, status, buffer),
         {:ok, _, s} <- recv_transaction(s, status, buffer) do
      {:ok, cursor, s}
    else
      {:error, %Postgrex.Error{} = err, s, buffer} ->
        rollback_flushed(s, status, err, buffer)
      {:disconnect, _err, _s} = disconnect ->
        disconnect
    end
  end
  defp rebind(%{postgres: postgres} = s,
       %{mode: :savepoint}, _, _, _) when postgres in [:idle, :error] do
    transaction_error(s, postgres)
  end

  defp execute(s, %{mode: :transaction} = status, query, cursor, max_rows) do
    %Cursor{portal: portal} = cursor
    msgs = [msg_execute(name_port: portal, max_rows: max_rows),
            msg_sync()]

    %{buffer: buffer} = s
    with :ok <- msg_send(%{s | buffer: nil}, msgs,  buffer),
         {ok, result, s, buffer} when ok in [:cont, :halt] <-
           recv_execute(s, status, query, cursor, max_rows, [], buffer),
         {:ok, s} <- recv_ready(s, status, buffer) do
      {ok, result, s}
    else
      {:copy_out, result, s} ->
        {:cont, result, s}
      {:error, %Postgrex.Error{} = err, s, buffer} ->
        error_ready(s, status, err, buffer)
      {:disconnect, _err, _s} = disconnect ->
        disconnect
    end
  end
  defp execute(%{postgres: :transaction} = s, %{mode: :savepoint} = status,
       query, cursor, max_rows) do
    %Cursor{portal: portal} = cursor
    msgs = [msg_query(statement: "SAVEPOINT postgrex_query"),
            msg_execute(name_port: portal, max_rows: max_rows),
            msg_query(statement: "RELEASE SAVEPOINT postgrex_query")]

    %{buffer: buffer} = s
    with :ok <- msg_send(%{s | buffer: nil}, msgs,  buffer),
         {:ok, _, %{buffer: buffer} = s} <- recv_transaction(s, status, buffer),
         {ok, result, s, buffer} when ok in [:cont, :halt] <-
           recv_execute(s, status, query, cursor, max_rows, [], buffer),
         {:ok, _, s} <- recv_transaction(s, status, buffer) do
      {ok, result, s}
    else
      {:copy_out, result, s} ->
        {:cont, result, s}
      {:error, %Postgrex.Error{} = err, s, buffer} ->
        rollback_flushed(s, status, err, buffer)
      {:disconnect, _err, _s} = disconnect ->
        disconnect
    end
  end

  defp recv_execute(s, status, query, cursor, max_rows, rows, buffer) do
    %Query{result_types: types} = query
    case rows_recv(s, types, rows, buffer) do
      {:ok, msg_command_complete(tag: tag), rows, buffer} ->
        {:halt, halt(s, query, rows, tag), s, buffer}
      {:ok, msg_portal_suspend(), rows, buffer} ->
        {:cont, done(s, query, rows, :stream, max_rows), s, buffer}
      {:ok, msg_error(fields: fields), _, buffer} ->
        {:error, Postgrex.Error.exception(postgres: fields), s, buffer}
      {:ok, msg_empty_query(), [], buffer} ->
        {:halt, done(s, query, nil, nil), s, buffer}
      {:ok, msg_copy_in_response(), [], buffer} ->
        copy_in_disconnect(s, query, buffer)
      {:ok, msg_copy_out_response(), [],  buffer} ->
        %{postgres: postgres} = s
        %Cursor{ref: ref} = cursor
        s = %{s | postgres: {postgres, ref}}
        recv_copy_out(s, status, query, max_rows, [], buffer)
      {:ok, msg_copy_both_response(), [], buffer} ->
        copy_both_disconnect(s, query, buffer)
      {:ok, msg, rows, buffer} ->
        s = handle_msg(s, status, msg)
        recv_execute(s, status, query, cursor, max_rows, rows, buffer)
      {:disconnect, _, _} = dis ->
        dis
    end
  end

  defp fetch_copy_out(%{buffer: buffer} = s, %{mode: :transaction} = status,
       query, max_rows) do
    s = %{s | buffer: nil}
    with {:halt, result, s, buffer} <-
           recv_copy_out(s, status, query, max_rows, [], buffer),
         {:ok, s} <- recv_ready(s, status, buffer) do
      {:halt, result, s}
    else
      {:copy_out, result, s} ->
        {:cont, result, s}
      {:error, err, s, buffer} ->
        error_ready(s, status, err, buffer)
      {:disconnect, _err, _s} = disconnect ->
        disconnect
    end
  end
  defp fetch_copy_out(%{buffer: buffer} = s, %{mode: :savepoint} = status,
       query, max_rows) do
    s = %{s | buffer: nil}
    with {:halt, result, s, buffer} <-
           recv_copy_out(s, status, query, max_rows, [], buffer),
         {:ok, _, s} <- recv_transaction(s, status, buffer) do
      {:halt, result, s}
    else
      {:copy_out, result, s} ->
        {:cont, result, s}
      {:error, err, s, buffer} ->
        rollback_flushed(s, status, err, buffer)
      {:disconnect, _err, _s} = disconnect ->
        disconnect
    end
  end

  defp recv_copy_out(s, status, query, max_rows, [], buffer) do
    max_rows = if max_rows == 0, do: :infinity, else: max_rows
    recv_copy_out(s, status, query, max_rows, [], 0, buffer)
  end

  defp recv_copy_out(s, _, query, max_rows, acc, max_rows, buffer) do
    s = %{s | buffer: buffer}
    {:copy_out, done(s, query, acc, :copy_stream, max_rows), s}
  end
  defp recv_copy_out(s, status, query, max_rows, acc, nrows, buffer) do
     case msg_recv(s, :infinity, buffer) do
      {:ok, msg_copy_data(data: data), buffer} ->
        recv_copy_out(s, status, query, max_rows, [data | acc], nrows+1, buffer)
      {:ok, msg_copy_done(), buffer} ->
        recv_copy_out(s, status, query, max_rows, acc, nrows, buffer)
      {:ok, msg_command_complete(tag: tag), buffer} ->
        {:halt, halt(s, query, acc, tag), s, buffer}
      {:ok, msg_error(fields: fields), buffer} ->
        {:error, Postgrex.Error.exception(postgres: fields), s, buffer}
      {:ok, msg, buffer} ->
        s
        |> handle_msg(status, msg)
        |> recv_copy_out(status, query, max_rows, acc, nrows, buffer)
      {:disconnect, _, _} = dis ->
        dis
    end
  end

  defp copy_in_data(s, %{mode: :transaction}, copy, data) do
    %Copy{portal: portal, ref: ref} = copy
    msgs = [msg_execute(name_port: portal, max_rows: 0),
            data]
    %{postgres: postgres, buffer: buffer} = s
    case msg_send(s, msgs, buffer) do
      :ok ->
        {:ok, copied(s), %{s | postgres: {postgres, ref}}}
      {:disconnect, _err, _s} = disconnect ->
        disconnect
    end
  end
  defp copy_in_data(s, %{mode: :savepoint} = status, copy, data) do
    %Copy{portal: portal, ref: ref} = copy
    msgs = [msg_query(statement: "SAVEPOINT postgrex_query"),
            msg_execute(name_port: portal, max_rows: 0),
            data]
    %{buffer: buffer} = s
    with :ok <- msg_send(%{s | buffer: nil}, msgs,  buffer),
         {:ok, _, %{postgres: postgres} = s} <-
           recv_transaction(s, status, buffer) do
      {:ok, copied(s), %{s | postgres: {postgres, ref}}}
    else
      {:disconnect, _err, _s} = disconnect ->
        disconnect
    end
  end

  defp copy_in_data(%{sock: {mod, sock}} = s, data) do
    case mod.send(sock, data) do
      :ok ->
        {:ok, copied(s), s}
      {:error, reason} ->
        disconnect(s, tag(mod), "send", reason)
    end
  end

  defp copied(%{connection_id: connection_id}) do
    %Postgrex.Result{command: :copy_stream, num_rows: :copy_stream, rows: nil,
      columns: nil, connection_id: connection_id}
  end

  defp copy_in_done(s, %{mode: :transaction} = status, %Copy{query: query}) do
    msgs = [msg_copy_done(),
            msg_sync()]

   %{buffer: buffer} = s
    with :ok <- msg_send(%{s | buffer: nil}, msgs,  buffer),
         {:ok, result, s, buffer} <- recv_copy_in(s, status, query, buffer),
         {:ok, s} <- recv_ready(s, status, buffer) do
      {:ok, result, s}
    else
      {:error, %Postgrex.Error{} = err, s, buffer} ->
        error_ready(s, status, err, buffer)
      {:disconnect, _err, _s} = disconnect ->
        disconnect
    end
  end
  defp copy_in_done(s, %{mode: :savepoint} = status, %Copy{query: query}) do
    msgs = [msg_copy_done(),
            msg_query(statement: "RELEASE SAVEPOINT postgrex_query")]

   %{buffer: buffer} = s
    with :ok <- msg_send(%{s | buffer: nil}, msgs,  buffer),
         {:ok, result, s, buffer} <- recv_copy_in(s, status, query, buffer),
         {:ok, _, s} <- recv_transaction(s, status, buffer) do
      {:ok, result, s}
    else
      {:error, %Postgrex.Error{} = err, s, buffer} ->
        rollback_flushed(s, status, err, buffer)
      {:disconnect, _err, _s} = disconnect ->
        disconnect
    end
  end

  defp recv_copy_in(s, status, query, buffer) do
    %Query{result_types: types} = query
    case rows_recv(s, types, [], buffer) do
      {:ok, msg_copy_in_response(), [], buffer} ->
        recv_copy_in_done(s, status, query, buffer)
      {:ok, msg_command_complete(tag: tag), rows, buffer} ->
        {:ok, done(s, query, rows, tag), s, buffer}
      {:ok, msg_empty_query(), [], buffer} ->
        {:ok, done(s, query, nil, nil), s, buffer}
      {:ok, msg_error(fields: fields), _, buffer} ->
        {:error, Postgrex.Error.exception(postgres: fields), s, buffer}
      {:ok, msg_copy_out_response(), [], buffer} ->
        recv_copy_out(s, status, query, buffer)
      {:ok, msg_copy_both_response(), [], buffer} ->
        copy_both_disconnect(s, query, buffer)
      {:ok, msg, [], buffer} ->
        s
        |> handle_msg(status, msg)
        |> recv_copy_in(status, query, buffer)
      {:ok, msg, [_|_] = rows, buffer} ->
        s
        |> handle_msg(status, msg)
        |> recv_execute(status, query, rows, buffer)
      {:disconnect, _, _} = dis ->
        dis
    end
  end

  defp recv_copy_in_done(s, status, query, buffer) do
    case msg_recv(s, :infinity, buffer) do
      {:ok, msg_command_complete(tag: tag), buffer} ->
        {:ok, done(s, query, nil, tag), s, buffer}
      {:ok, msg_error(fields: fields), buffer} ->
        {:error, Postgrex.Error.exception(postgres: fields), s, buffer}
      {:ok, msg, buffer} ->
        s
        |> handle_msg(status, msg)
        |> recv_copy_in_done(status, query, buffer)
      {:disconnect, _, _} = dis ->
        dis
    end
  end

  ## close

  defp copy_out_done(s, %{mode: :transaction} = status, query) do
    %{buffer: buffer} = s
    s = %{s | buffer: nil}
    with {:halt, result, s, buffer} <-
           recv_copy_out(s, status, query, :infinity, [], buffer),
         {:ok, s} <- recv_ready(s, status, buffer) do
      {:ok, result, s}
    else
      {:error, %Postgrex.Error{} = err, s, buffer} ->
        error_ready(s, status, err, buffer)
      {:disconnect, _err, _s} = disconnect ->
        disconnect
    end
  end
  defp copy_out_done(s, %{mode: :savepoint} = status, query) do
    %{buffer: buffer} = s
    s = %{s | buffer: nil}
    with {:halt, result, s, buffer} <-
           recv_copy_out(s, status, query, :infinity, [], buffer),
         {:ok, _, s} <- recv_transaction(s, status, buffer) do
      {:ok, result, s}
    else
      {:error, %Postgrex.Error{} = err, s, buffer} ->
        rollback_flushed(s, status, err, buffer)
      {:disconnect, _err, _s} = disconnect ->
        disconnect
    end
  end

  defp flushed_close(s, %{mode: :transaction} = status, query) do
    # closing query without transaction if not flushed is the same as if doing
    # with preparing immediately before.
    close(s, status, query)
  end
  defp flushed_close(s, %{mode: :savepoint} = status, query) do
    %Query{name: name} = query
    rollback_release =
          "ROLLBACK TO SAVEPOINT postgrex_savepoint;RELEASE SAVEPOINT postgrex_savepoint"
    msgs = [msg_close(type: :statement, name: name),
            msg_query(statement: rollback_release)]

    %{buffer: buffer} = s
    with :ok <- msg_send(%{s | buffer: nil}, msgs,  buffer),
         {:ok, s, buffer} <- recv_close(s, status, buffer),
         query_delete(s, query),
         {:ok, _, s} <- recv_transaction(s, status, buffer) do
      %{connection_id: connection_id} = s
      {:ok, %Postgrex.Result{command: :close, connection_id: connection_id}, s}
    else
      {:disconnect, _err, _s} = disconnect ->
        disconnect
    end
  end

  defp close(s, status, %Query{name: name} = query) do
    msgs = [msg_close(type: :statement, name: name),
            msg_sync()]

    %{buffer: buffer} = s
    with :ok <- msg_send(%{s | buffer: nil}, msgs,  buffer),
         {:ok, s, buffer} <- recv_close(s, status, buffer),
         query_delete(s, query),
         {:ok, s} <- recv_ready(s, status, buffer) do
      %{connection_id: connection_id} = s
      {:ok, %Postgrex.Result{command: :close, connection_id: connection_id}, s}
    else
      {:disconnect, _err, _s} = disconnect ->
        disconnect
    end
  end
  defp close(s, status, %{portal: portal}) do
    msgs = [msg_close(type: :portal, name: portal),
            msg_sync()]

    %{buffer: buffer} = s
    with :ok <- msg_send(%{s | buffer: nil}, msgs,  buffer),
         {:ok, s, buffer} <- recv_close(s, status, buffer),
         {:ok, s} <- recv_ready(s, status, buffer) do
      %{connection_id: connection_id} = s
      {:ok, %Postgrex.Result{command: :close, connection_id: connection_id}, s}
    else
      {:disconnect, _err, _s} = disconnect ->
        disconnect
    end
  end

  ## ping

  defp ping_recv(s, status, old_buffer, buffer) do
    %{timeout: timeout, postgres: postgres, transactions: transactions} = s
    case msg_recv(s, timeout, buffer) do
      {:ok, msg_ready(status: :idle), buffer}
      when postgres == :transaction and transactions == :strict ->
        sync_error(s, :idle, buffer)
      {:ok, msg_ready(status: :transaction), buffer}
      when postgres == :idle and transactions == :strict ->
        sync_error(s, :transaction, buffer)
      {:ok, msg_ready(status: :error), buffer}
      when postgres == :idle and transactions == :strict ->
        sync_error(s, :error, buffer)
      {:ok, msg_ready(status: postgres), buffer}
          when old_buffer == :active_once ->
        activate(%{s | postgres: postgres}, buffer)
      {:ok, msg_ready(status: postgres), buffer} when is_nil(old_buffer) ->
        {:ok, %{s | postgres: postgres, buffer: buffer}}
      {:ok, msg_error(fields: fields), buffer} ->
        disconnect(s, Postgrex.Error.exception(postgres: fields), buffer)
      {:ok, msg, buffer} ->
        ping_recv(handle_msg(s, status, msg), status, old_buffer, buffer)
      {:disconnect, _, _} = dis ->
        dis
    end
  end

  ## transaction

  defp handle_transaction(statement, opts, %{buffer: buffer} = s) do
    status = %{notify: notify(opts)}
    msgs = [msg_query(statement: statement)]
    with :ok <- msg_send(%{s | buffer: nil}, msgs, buffer) do
      recv_transaction(s, status, buffer)
    else
      {:disconnect, err, s} ->
        {:disconnect, err, s}
      {:error, %Postgrex.Error{} = err, s, buffer} ->
        error_ready(s, status, err, buffer)
    end
  end

  defp recv_transaction(s, status, tags \\ [], buffer) do
    case msg_recv(s, :infinity, buffer) do
      {:ok, msg_command_complete(tag: tag), buffer} ->
        recv_transaction(s, status, [tag | tags], buffer)
      {:ok, msg_error(fields: fields), buffer} ->
        err = Postgrex.Error.exception(postgres: fields)
        {:disconnect, err, %{s | buffer: buffer}}
      {:ok, msg_ready(status: postgres), buffer} ->
        s = %{s | postgres: postgres, buffer: buffer}
        {:ok, done(s, Enum.reverse(tags)), s}
      {:ok, msg, buffer} ->
        recv_transaction(handle_msg(s, status, msg), status, tags, buffer)
      {:disconnect, _, _} = dis ->
        dis
    end
  end

  defp recv_close(s, status, buffer) do
    case msg_recv(s, :infinity, buffer) do
      {:ok, msg_close_complete(), buffer} ->
        {:ok, s, buffer}
      {:ok, msg_error(fields: fields), buffer} ->
        err = Postgrex.Error.exception(postgres: fields)
        {:disconnect, err, %{s | buffer: buffer}}
      {:ok, msg, buffer} ->
        recv_close(handle_msg(s, status, msg), status, buffer)
      {:disconnect, _, _} = dis ->
        dis
    end
  end

  defp recv_ready(%{transactions: :naive} = s, status, buffer) do
    case msg_recv(s, :infinity, buffer) do
      {:ok, msg_ready(status: postgres), buffer} ->
        {:ok, %{s | postgres: postgres, buffer: buffer}}
      {:ok, msg, buffer} ->
        recv_ready(handle_msg(s, status, msg), status, buffer)
      {:disconnect, _, _} = dis ->
        dis
    end
  end
  defp recv_ready(%{transactions: :strict, postgres: {postgres, _}} = s, status,
       buffer) do
    recv_strict_ready(s, status, postgres, buffer)
  end
  defp recv_ready(%{transactions: :strict, postgres: postgres} = s, status,
       buffer) do
    recv_strict_ready(s, status, postgres, buffer)
  end

  defp recv_strict_ready(s, status, expected, buffer) do
    case msg_recv(s, :infinity, buffer) do
      {:ok, msg_ready(status: ^expected), buffer} ->
        {:ok, %{s | postgres: expected, buffer: buffer}}
      {:ok, msg_ready(status: :error), buffer} when expected == :transaction ->
        {:ok, %{s | postgres: :error, buffer: buffer}}
      {:ok, msg_ready(status: unexpected), buffer} ->
        sync_error(s, unexpected, buffer)
      {:ok, msg, buffer} ->
        recv_strict_ready(handle_msg(s, status, msg), status, expected, buffer)
      {:disconnect, _, _} = dis ->
        dis
    end
  end

  defp error_ready(s, status, %Postgrex.Error{} = err, buffer) do
    case recv_ready(s, status, buffer) do
      {:ok, s} ->
        %{connection_id: connection_id} = s
        {:error, %Postgrex.Error{err | connection_id: connection_id}, s}
      {:disconnect, _, _} = disconnect ->
        disconnect
    end
  end
  defp error_ready(s, status, err, buffer) do
    case recv_ready(s, status, buffer) do
      {:ok, s} ->
        {:error, err, s}
      {:disconnect, _, _} = disconnect ->
        disconnect
    end
  end

  defp done(%{connection_id: connection_id}, tags) do
    {command, nil} = decode_tags(tags)

    %Postgrex.Result{command: command, num_rows: nil, rows: nil,
      columns: nil, connection_id: connection_id}
  end

  defp done(s, %Query{} = query, rows, tag) do
    {command, nrows} =
      if tag, do: decode_tag(tag), else: {nil, nil}
    done(s, query, rows, command, nrows)
  end

  defp done(s, query, rows, command, nrows) do
    %{connection_id: connection_id} = s
    %Query{columns: cols} = query
    # Fix for PostgreSQL 8.4 (doesn't include number of selected rows in tag)
    nrows =
      if is_nil(nrows) and command == :select, do: length(rows), else: nrows

    rows =
      if is_nil(cols) and rows == [] and command != :copy, do: nil, else: rows

    %Postgrex.Result{command: command, num_rows: nrows || 0, rows: rows,
      columns: cols, connection_id: connection_id}
  end

  defp halt(s, query, rows, tag) do
    case done(s, query, rows, tag) do
      %Postgrex.Result{rows: rows} = result when is_list(rows) ->
        # shows rows for all streamed results but we only want for last chunk.
        %Postgrex.Result{result | num_rows: length(rows)}
      result ->
        result
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
    fields
    |> Enum.map(fn row_field(type_oid: oid, name: name) -> {oid, name} end)
    |> :lists.unzip
  end

  defp tag(:gen_tcp), do: :tcp
  defp tag(:ssl), do: :ssl

  defp decode_tags([tag]),
    do: decode_tag(tag)
  defp decode_tags(tags),
    do: Enum.map_reduce(tags, nil, &decode_tags/2)

  defp decode_tags(tag, acc) do
    case decode_tag(tag) do
      {command, nil} ->
        {command, acc}
      {command, nrows} ->
        {command, nrows + (acc || 0)}
    end
  end

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
    do: decode_tag(t, <<acc::binary, h + 32>>)
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

  defp rows_recv(%{types: types} = s, result_types, rows, buffer) do
    case Types.decode_rows(buffer, result_types, rows, types) do
      {:ok, rows, buffer} ->
        rows_msg(s, rows, buffer)
      {:more, buffer, rows, more} ->
        rows_recv(s, result_types, rows, buffer, more)
    end
  end

  defp rows_recv(%{sock: {mod, sock}} = s, result_types, rows, buffer, more) do
    case mod.recv(sock, 0, :infinity) do
      {:ok, data} when byte_size(data) < more ->
        rows_recv(s, result_types, rows, [buffer | data], more - byte_size(data))
      {:ok, data} when is_binary(buffer) ->
        rows_recv(s, result_types, rows, buffer <> data)
      {:ok, data} when is_list(buffer) ->
        rows_recv(s, result_types, rows, IO.iodata_to_binary([buffer | data]))
      {:error, reason} ->
        disconnect(s, tag(mod), "recv", reason, IO.iodata_to_binary(buffer))
    end
  end

  defp rows_msg(s, rows, buffer) do
    case msg_recv(s, :infinity, buffer) do
      {:ok, msg, buffer} ->
        {:ok, msg, rows, buffer}
      {:disconnect, _, _} = dis ->
        dis
    end
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

  defp disconnect(s, tag, action, reason, buffer) do
    disconnect(%{s | buffer: buffer}, tag, action, reason)
  end

  defp disconnect(s, tag, action, reason) do
    {:disconnect, conn_error(tag, action, reason), s}
  end

  defp conn_error(mod, action, reason) when reason in @nonposix_errors do
    conn_error("#{mod} #{action}: #{reason}")
  end

  defp conn_error(:tcp, action, reason) do
    formatted_reason = :inet.format_error(reason)
    conn_error("tcp #{action}: #{formatted_reason} - #{inspect(reason)}")
  end

  defp conn_error(:ssl, action, reason) do
    formatted_reason = :ssl.format_error(reason)
    conn_error("ssl #{action}: #{formatted_reason} - #{inspect(reason)}")
  end

  defp conn_error(message) do
    DBConnection.ConnectionError.exception(message)
  end

  defp disconnect(%{connection_id: connection_id} = s, %Postgrex.Error{} = err, buffer) do
    {:disconnect, %{err | connection_id: connection_id}, %{s | buffer: buffer}}
  end

  defp sync_recv(s, status, buffer) do
    %{postgres: postgres, transactions: transactions} = s
    case msg_recv(s, :infinity, buffer) do
      {:ok, msg_ready(status: :idle), buffer}
      when postgres == :transaction and transactions == :strict ->
        sync_error(s, :idle, buffer)
      {:ok, msg_ready(status: :transaction), buffer}
      when postgres == :idle and transactions == :strict ->
        sync_error(s, :transaction, buffer)
      {:ok, msg_ready(status: :error), buffer}
      when postgres == :idle and transactions == :strict ->
        sync_error(s, :error, buffer)
      {:ok, msg_ready(status: postgres), buffer} ->
        {:ok, %{s | postgres: postgres, buffer: buffer}}
      {:ok, msg_error(fields: fields), buffer} ->
        disconnect(s, Postgrex.Error.exception(postgres: fields), buffer)
      {:ok, msg, buffer} ->
        sync_recv(handle_msg(s, status, msg), status, buffer)
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

  defp cancel_request(%{connection_key: nil}), do: :ok
  defp cancel_request(s) do
    case do_cancel_request(s) do
      :ok ->
        :ok
      {:error, action, reason} ->
        err = conn_error(:tcp, action, reason)
        Logger.error fn() ->
          ["#{inspect __MODULE__} #{inspect self()} could not cancel backend: " |
            Exception.message(err)]
        end
    end
  end

  defp do_cancel_request(%{peer: {ip, port}, timeout: timeout} = s) do
    case :gen_tcp.connect(ip, port, [mode: :binary, active: false], timeout) do
      {:ok, sock}      -> cancel_send_recv(s, sock)
      {:error, reason} -> {:error, :connect, reason}
    end
  end

  defp cancel_send_recv(%{connection_id: pid, connection_key: key} = s, sock) do
    msg = msg_cancel_request(pid: pid, key: key)
    case :gen_tcp.send(sock, encode_msg(msg)) do
      :ok              -> cancel_recv(s, sock)
      {:error, reason} -> {:error, :send, reason}
    end
  end

  defp cancel_recv(%{timeout: timeout}, sock) do
    # ignore result as socket will close, else can do nothing
    _ = :gen_tcp.recv(sock, 0, timeout)
    :gen_tcp.close(sock)
  end

  defp sock_close(%{sock: {mod, sock}}), do: mod.close(sock)

  defp delete_parameters(%{parameters: ref}) when is_reference(ref) do
    Postgrex.Parameters.delete(ref)
  end
  defp delete_parameters(_), do: :ok

  defp queries_new(), do: :ets.new(__MODULE__, [:set, :public])

  defp queries_delete(%{queries: nil}), do: true
  defp queries_delete(%{queries: queries}), do: :ets.delete(queries)

  defp query_put(%{queries: nil}, _), do: :ok
  defp query_put(_, %Query{ref: nil}), do: :ok
  defp query_put(_, %Query{name: ""}), do: :ok
  defp query_put(%{queries: queries}, %Query{name: name, ref: ref}) do
    try do
      :ets.insert(queries, {name, ref})
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

  defp query_delete(%{queries: nil}, _), do: :ok
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

  defp query_member?(%{queries: nil}, _),
    do: false
  defp query_member?(%{queries: queries}, %Query{name: name, ref: ref}) do
    try do
      :ets.lookup_element(queries, name, 2)
    rescue
      ArgumentError ->
        false
    else
      ^ref ->
        true
      _ ->
        false
    end
  end
end
