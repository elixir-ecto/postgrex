defmodule Postgrex.ReplicationReconnectTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog
  alias Postgrex.ReplicationConnection, as: Connection

  @moduletag :logical_replication
  @moduletag min_pg_version: "10.0"
  @role "postgrex_scram_pw"

  defmodule Receiver do
    use Postgrex.ReplicationConnection

    def start_link(options) do
      {owner, options} = Keyword.pop!(options, :owner)
      {disconnect_options, options} = Keyword.pop(options, :disconnect_options, [])
      state = %{owner: owner, disconnect_options: disconnect_options, disconnects: 0}
      Connection.start_link(__MODULE__, state, options)
    end

    @impl true
    def init(state), do: {:ok, state}

    @impl true
    def handle_connect(state) do
      send(state.owner, {:connected, self()})
      {:noreply, state}
    end

    @impl true
    def handle_disconnect({from, state}) do
      Connection.reply(from, :disconnected)
      handle_disconnect(state)
    end

    def handle_disconnect(state) do
      state = %{state | disconnects: state.disconnects + 1}
      send(state.owner, {:disconnected, self(), state.disconnects})

      case state.disconnect_options do
        [] ->
          {:noreply, state}

        [:unchanged | rest] ->
          {:noreply, %{state | disconnect_options: rest}}

        [options | rest] ->
          {:noreply, options, %{state | disconnect_options: rest}}
      end
    end

    @impl true
    def handle_call({:query, sql}, from, state) do
      {:query, sql, {from, state}}
    end

    @impl true
    def handle_result(results, {from, state}) do
      Connection.reply(from, results)
      {:noreply, state}
    end
  end

  setup do
    admin = start_supervised!({Postgrex, database: "postgrex_test"})

    %Postgrex.Result{rows: [[replication]]} =
      query!(admin, "SELECT rolreplication FROM pg_roles WHERE rolname = $1", [@role])

    # Like the login tests, these tests rely on the server's password authentication.
    # They run serially and restore the role's original password and replication privilege.
    on_exit(fn ->
      {:ok, cleanup} = Postgrex.start_link(database: "postgrex_test")

      try do
        query!(cleanup, "SET password_encryption = 'scram-sha-256'")
        privilege = if replication, do: "REPLICATION", else: "NOREPLICATION"
        query!(cleanup, "ALTER ROLE #{@role} WITH #{privilege} PASSWORD '#{@role}'")
      after
        GenServer.stop(cleanup)
      end
    end)

    query!(admin, "SET password_encryption = 'scram-sha-256'")
    query!(admin, "ALTER ROLE #{@role} WITH REPLICATION")
    %{admin: admin}
  end

  @tag :capture_log
  test "merges refreshed credentials and retains them for later reconnects", context do
    password = replacement_password()
    receiver = receiver(disconnect_options: [[password: password]])
    assert_receive {:connected, ^receiver}
    original_backend = backend(receiver)
    rotate_password(context.admin, password)

    disconnect(receiver)
    assert_receive {:disconnected, ^receiver, 1}
    assert_receive {:connected, ^receiver}
    next_backend = backend(receiver)
    assert next_backend != original_backend

    disconnect(receiver)
    assert_receive {:disconnected, ^receiver, 2}
    assert_receive {:connected, ^receiver}
    assert backend(receiver) != next_backend
    refute_received {:disconnected, ^receiver, _}
  end

  test "refreshes credentials after consecutive failed reconnect attempts", context do
    password = replacement_password()

    receiver = receiver(disconnect_options: [:unchanged, :unchanged, [password: password]])

    assert_receive {:connected, ^receiver}
    original_backend = backend(receiver)
    rotate_password(context.admin, password)

    log =
      capture_log(fn ->
        disconnect(receiver)
        assert_receive {:disconnected, ^receiver, 1}
        assert_receive {:disconnected, ^receiver, 2}
        assert_receive {:disconnected, ^receiver, 3}
        assert_receive {:connected, ^receiver}
        assert backend(receiver) != original_backend
        refute_received {:disconnected, ^receiver, _}
      end)

    assert log =~ "28P01 (invalid_password)"
  end

  test "refreshes credentials after a failed initial connection attempt", context do
    password = replacement_password()
    rotate_password(context.admin, password)

    log =
      capture_log(fn ->
        receiver = receiver(disconnect_options: [[password: password]])
        assert_receive {:disconnected, ^receiver, 1}
        assert_receive {:connected, ^receiver}
        assert is_integer(backend(receiver))
        refute_received {:disconnected, ^receiver, _}
      end)

    assert log =~ "28P01 (invalid_password)"
  end

  test "refreshes credentials after a failed asynchronous initial connection attempt", context do
    password = replacement_password()
    rotate_password(context.admin, password)

    log =
      capture_log(fn ->
        receiver = receiver(sync_connect: false, disconnect_options: [[password: password]])
        assert_receive {:disconnected, ^receiver, 1}
        assert_receive {:connected, ^receiver}
        assert is_integer(backend(receiver))
        refute_received {:disconnected, ^receiver, _}
      end)

    assert log =~ "28P01 (invalid_password)"
  end

  @tag :capture_log
  test "restores replication mode when replacing connection parameters" do
    receiver =
      receiver(
        parameters: [application_name: "before_reconnect"],
        disconnect_options: [[parameters: [application_name: "after_reconnect"]]]
      )

    assert_receive {:connected, ^receiver}

    assert [%Postgrex.Result{rows: [["before_reconnect"]]}] =
             Connection.call(receiver, {:query, "SHOW application_name"})

    disconnect(receiver)
    assert_receive {:disconnected, ^receiver, 1}
    assert_receive {:connected, ^receiver}

    assert [%Postgrex.Result{rows: [["after_reconnect"]]}] =
             Connection.call(receiver, {:query, "SHOW application_name"})

    assert [%Postgrex.Result{}] = Connection.call(receiver, {:query, "IDENTIFY_SYSTEM"})
    refute_received {:disconnected, ^receiver, _}
  end

  @tag :capture_log
  test "ignores returned reconnect management options", context do
    password = replacement_password()

    receiver =
      receiver(
        disconnect_options: [
          [auto_reconnect: false, reconnect_backoff: :invalid, sync_connect: :invalid],
          [password: password]
        ]
      )

    assert_receive {:connected, ^receiver}
    rotate_password(context.admin, password)

    disconnect(receiver)
    assert_receive {:disconnected, ^receiver, 1}
    assert_receive {:disconnected, ^receiver, 2}
    assert_receive {:connected, ^receiver}

    disconnect(receiver)
    assert_receive {:disconnected, ^receiver, 3}
    assert_receive {:connected, ^receiver}
    assert is_integer(backend(receiver))

    refute stored_option?(receiver, :auto_reconnect)
    refute stored_option?(receiver, :reconnect_backoff)
    assert stored_option(receiver, :sync_connect) == true

    refute_received {:disconnected, ^receiver, _}
  end

  @tag :capture_log
  test "normalizes string ports and replaces endpoints with a hostname", context do
    %Postgrex.Result{rows: [[port]]} = query!(context.admin, "SHOW port")

    receiver =
      receiver(
        endpoints: [{"localhost", String.to_integer(port)}],
        disconnect_options: [[hostname: "localhost", port: port]]
      )

    assert_receive {:connected, ^receiver}
    disconnect(receiver)
    assert_receive {:disconnected, ^receiver, 1}
    assert_receive {:connected, ^receiver}
    assert is_integer(backend(receiver))

    assert stored_option(receiver, :hostname) == "localhost"
    assert stored_option(receiver, :port) == String.to_integer(port)
    refute stored_option?(receiver, :endpoints)
    refute stored_option?(receiver, :socket_dir)
    refute stored_option?(receiver, :socket)
    refute_received {:disconnected, ^receiver, _}
  end

  @tag :capture_log
  test "replaces the endpoint group when only a port is returned", context do
    %Postgrex.Result{rows: [[port]]} = query!(context.admin, "SHOW port")

    receiver =
      receiver(
        endpoints: [{"localhost", String.to_integer(port)}],
        disconnect_options: [[port: port]]
      )

    assert_receive {:connected, ^receiver}
    disconnect(receiver)
    assert_receive {:disconnected, ^receiver, 1}
    assert_receive {:connected, ^receiver}
    assert is_integer(backend(receiver))

    assert stored_option(receiver, :port) == String.to_integer(port)
    refute stored_option?(receiver, :endpoints)
    refute_received {:disconnected, ^receiver, _}
  end

  @tag :capture_log
  @tag :unix
  test "replaces each endpoint selector as a group", context do
    %Postgrex.Result{rows: [[port]]} = query!(context.admin, "SHOW port")
    socket_dir = System.get_env("PG_SOCKET_DIR") || "/tmp"
    socket = Path.join(socket_dir, ".s.PGSQL.#{port}")
    endpoint_keys = [:hostname, :endpoints, :socket_dir, :socket]

    endpoints = [
      [endpoints: [{"localhost", String.to_integer(port)}]],
      [socket_dir: socket_dir, port: port],
      [hostname: "localhost", port: port],
      [socket: socket]
    ]

    transitions = Enum.zip(endpoints, tl(endpoints) ++ [hd(endpoints)])

    # Use the same login as the admin connection, including on peer-authenticated sockets.
    credentials =
      [database: "postgrex_test"]
      |> Postgrex.Utils.default_opts()
      |> Keyword.take([:username, :password])

    for {initial, refreshed} <- transitions do
      options =
        initial
        |> Keyword.merge(credentials)
        |> Keyword.put(:disconnect_options, [refreshed])

      receiver = receiver(options)
      assert_receive {:connected, ^receiver}
      disconnect(receiver)
      assert_receive {:disconnected, ^receiver, 1}
      assert_receive {:connected, ^receiver}
      assert is_integer(backend(receiver))

      for key <- endpoint_keys do
        assert stored_option(receiver, key) == Keyword.get(refreshed, key)
      end

      refute_received {:disconnected, ^receiver, _}
      stop_supervised!(Receiver)
    end
  end

  @tag :capture_log
  test "uses default ports when replacing the endpoint selector", context do
    %Postgrex.Result{rows: [[port]]} = query!(context.admin, "SHOW port")
    endpoint_keys = [:hostname, :endpoints, :socket_dir, :socket]

    endpoint_options =
      []
      |> Postgrex.Utils.default_opts()
      |> Keyword.take(endpoint_keys)

    receiver =
      receiver(
        endpoints: [{"localhost", String.to_integer(port)}],
        port: 1,
        disconnect_options: [endpoint_options]
      )

    assert_receive {:connected, ^receiver}
    disconnect(receiver)
    assert_receive {:disconnected, ^receiver, 1}
    assert_receive {:connected, ^receiver}
    assert is_integer(backend(receiver))

    refute stored_option(receiver, :port) == 1
    refute stored_option?(receiver, :endpoints)
    refute_received {:disconnected, ^receiver, _}
  end

  @tag :capture_log
  test "removes nil options and reapplies environment defaults" do
    previous_password = System.get_env("PGPASSWORD")

    on_exit(fn ->
      if previous_password do
        System.put_env("PGPASSWORD", previous_password)
      else
        System.delete_env("PGPASSWORD")
      end
    end)

    System.put_env("PGPASSWORD", @role)

    receiver =
      receiver(disconnect_options: [[password: nil, parameters: nil, socket_options: nil]])

    assert_receive {:connected, ^receiver}
    disconnect(receiver)
    assert_receive {:disconnected, ^receiver, 1}
    assert_receive {:connected, ^receiver}
    assert is_integer(backend(receiver))

    assert stored_option?(receiver, :password)
    assert stored_option(receiver, :parameters) == [replication: "database"]
    refute stored_option?(receiver, :socket_options)
    refute_received {:disconnected, ^receiver, _}
  end

  @tag :capture_log
  test "rejects disconnect options that are not a keyword list" do
    receiver = receiver(disconnect_options: [[:invalid]])
    assert_receive {:connected, ^receiver}
    monitor = Process.monitor(receiver)

    disconnect(receiver)
    assert_receive {:disconnected, ^receiver, 1}

    assert_receive {:DOWN, ^monitor, :process, ^receiver, {%ArgumentError{message: message}, _}}

    expected = "expected handle_disconnect/1 to return connection options as a keyword list"
    assert message == expected
    refute_received {:connected, ^receiver}
  end

  @tag :capture_log
  test "stops after a live disconnection when auto_reconnect is false" do
    # Merged options would fail normalization, so the stop reason proves they are ignored.
    receiver = receiver(auto_reconnect: false, disconnect_options: [[port: "not-a-port"]])
    assert_receive {:connected, ^receiver}
    monitor = Process.monitor(receiver)

    disconnect(receiver)
    assert_receive {:disconnected, ^receiver, 1}

    assert_receive {:DOWN, ^monitor, :process, ^receiver, %DBConnection.ConnectionError{}}

    refute_received {:disconnected, ^receiver, _}
    refute_received {:connected, ^receiver}
  end

  @tag :capture_log
  test "stops after failed sync and async attempts without auto_reconnect" do
    Process.flag(:trap_exit, true)

    for sync_connect <- [true, false] do
      result =
        Receiver.start_link(
          owner: self(),
          database: "postgrex_test",
          username: @role,
          password: replacement_password(),
          sync_connect: sync_connect,
          auto_reconnect: false,
          disconnect_options: [[password: @role]]
        )

      if sync_connect do
        assert {:error, %Postgrex.Error{postgres: %{code: :invalid_password}}} = result
      else
        assert {:ok, receiver} = result

        assert_receive {:EXIT, ^receiver, %Postgrex.Error{postgres: %{code: :invalid_password}}}
      end

      refute_received {:disconnected, _, _}
      refute_received {:connected, _}
    end
  end

  defp receiver(options) do
    start_supervised!(
      {Receiver,
       Keyword.merge(
         [
           owner: self(),
           database: "postgrex_test",
           username: @role,
           password: @role,
           auto_reconnect: true,
           reconnect_backoff: 10
         ],
         options
       )},
      restart: :temporary
    )
  end

  defp replacement_password, do: "replication-credential-#{System.unique_integer([:positive])}"

  defp backend(receiver) do
    assert [%Postgrex.Result{rows: [[pid]]}] =
             Connection.call(receiver, {:query, "SELECT pg_backend_pid()"})

    String.to_integer(pid)
  end

  defp rotate_password(admin, password) do
    query!(admin, "ALTER ROLE #{@role} PASSWORD '#{password}'")
  end

  defp disconnect(receiver) do
    {_, state} = :sys.get_state(receiver)
    {:gen_tcp, socket} = state.protocol.sock
    :ok = :gen_tcp.shutdown(socket, :read_write)
    assert :disconnected = Connection.call(receiver, {:query, "SELECT 1"})
  end

  defp stored_options(receiver) do
    {:dictionary, dictionary} = Process.info(receiver, :dictionary)
    Keyword.get(dictionary, Connection, [])
  end

  defp stored_option(receiver, key), do: Keyword.get(stored_options(receiver), key)

  defp stored_option?(receiver, key), do: Keyword.has_key?(stored_options(receiver), key)

  defp query!(connection, sql, params \\ []) do
    case Postgrex.query(connection, sql, params) do
      {:ok, result} -> result
      {:error, _} -> flunk("database setup failed")
    end
  end
end
