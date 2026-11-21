defmodule Postgrex.ReplicationConfigurationTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog
  alias Postgrex.ReplicationConnection, as: Connection

  @moduletag :logical_replication
  @moduletag min_pg_version: "10.0"
  @credential_key {__MODULE__, :credential}

  defmodule Receiver do
    use Postgrex.ReplicationConnection

    def start_link(options) do
      {owner, options} = Keyword.pop!(options, :owner)
      Connection.start_link(__MODULE__, owner, options)
    end

    @impl true
    def init(owner), do: {:ok, owner}

    @impl true
    def handle_connect(owner) do
      send(owner, {:connected, self()})
      {:noreply, owner}
    end

    @impl true
    def handle_call(:backend, from, owner) do
      {:query, "SELECT pg_backend_pid()", {from, owner}}
    end

    @impl true
    def handle_result([%Postgrex.Result{rows: [[pid]]}], {from, owner}) do
      Connection.reply(from, String.to_integer(pid))
      {:noreply, owner}
    end

    @impl true
    def handle_info(:crash, _owner), do: raise("replication crash probe")
    def handle_info({:disconnect, _}, owner), do: {:disconnect, :reconnect_probe}
  end

  setup do
    {:ok, admin} = Postgrex.start_link(database: "postgrex_test")
    role = "postgrex_configure_#{System.unique_integer([:positive])}"
    password = "replication-credential-#{System.unique_integer([:positive])}"
    query!(admin, "CREATE ROLE #{role} WITH LOGIN REPLICATION PASSWORD '#{password}'")
    :persistent_term.put(@credential_key, {role, password})

    on_exit(fn ->
      :persistent_term.erase(@credential_key)
      {:ok, cleanup} = Postgrex.start_link(database: "postgrex_test")

      try do
        query!(cleanup, "DROP ROLE #{role}")
      after
        GenServer.stop(cleanup)
      end
    end)

    %{admin: admin, role: role, password: password}
  end

  def configure(options, owner) do
    assert options[:password] == nil
    send(owner, {:configured, self()})
    {role, password} = :persistent_term.get(@credential_key)
    Keyword.merge(options, database: "postgrex_test", username: role, password: password)
  end

  test "named configuration runs before every connection attempt", context do
    receiver = receiver(configure: {__MODULE__, :configure, [self()]})
    assert_receive {:configured, ^receiver}
    assert_receive {:connected, ^receiver}
    assert is_integer(Connection.call(receiver, :backend))

    replacement = context.password <> "-rotated"
    query!(context.admin, "ALTER ROLE #{context.role} PASSWORD '#{replacement}'")
    :persistent_term.put(@credential_key, {context.role, replacement})

    capture_log(fn ->
      send(receiver, {:disconnect, :reconnect_probe})
      assert_receive {:configured, ^receiver}
      assert_receive {:connected, ^receiver}
      assert is_integer(Connection.call(receiver, :backend))
    end)
  end

  test "anonymous configuration receives the unresolved options" do
    owner = self()

    receiver =
      receiver(
        configure: fn options ->
          assert options[:database] == "unconfigured"
          configure(options, owner)
        end
      )

    assert_receive {:configured, ^receiver}
    assert_receive {:connected, ^receiver}
    assert is_integer(Connection.call(receiver, :backend))
  end

  test "configured credentials stay out of status and crash reports", context do
    receiver = receiver(configure: {__MODULE__, :configure, [self()]}, restart: :temporary)
    assert_receive {:connected, ^receiver}

    status_exposed? = exposed?(:sys.get_status(receiver), context.password)

    monitor = Process.monitor(receiver)

    report =
      capture_log(fn ->
        send(receiver, :crash)
        assert_receive {:DOWN, ^monitor, :process, ^receiver, _}
        Logger.flush()
      end)

    directory =
      Path.join(System.tmp_dir!(), "postgrex-report-#{System.unique_integer([:positive])}")

    File.mkdir!(directory)
    File.chmod!(directory, 0o700)
    path = Path.join(directory, "crash.log")

    try do
      File.write!(path, report)
      credential_exposed? = path |> File.read!() |> String.contains?(context.password)
      crash_reported? = String.contains?(report, inspect(receiver))
      refute credential_exposed?
      refute status_exposed?
      assert crash_reported?
    after
      File.rm_rf!(directory)
    end
  end

  defp receiver(options) do
    {restart, options} = Keyword.pop(options, :restart, :permanent)

    start_supervised!(
      {Receiver,
       Keyword.merge(
         [owner: self(), database: "unconfigured", password: nil, auto_reconnect: true],
         options
       )},
      restart: restart
    )
  end

  defp exposed?(term, credential),
    do: :binary.match(:erlang.term_to_binary(term), credential) != :nomatch

  defp query!(connection, sql, params \\ []) do
    case Postgrex.query(connection, sql, params) do
      {:ok, result} -> result
      {:error, _} -> flunk("database setup failed")
    end
  end
end
