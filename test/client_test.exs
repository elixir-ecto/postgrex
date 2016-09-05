defmodule ClientTest do
  use ExUnit.Case
  import Postgrex.TestHelper

  setup do
    opts = [ database: "postgrex_test", backoff_type: :stop ]
    {:ok, pid} = Postgrex.start_link(opts)
    {:ok, [pid: pid, options: opts]}
  end

  test "active client timeout", context do
    conn = context[:pid]

    %Postgrex.Result{connection_id: connection_id} = Postgrex.query!(conn, "SELECT 42", [])

    Process.flag(:trap_exit, true)
    capture_log fn ->
      assert [[_]] = query("SELECT pg_stat_get_activity($1)", [connection_id])
      assert_raise DBConnection.ConnectionError, "tcp recv: closed",
        fn() -> query("SELECT pg_sleep(10)", [], [timeout: 50]) end

      assert_receive {:EXIT, ^conn, {:shutdown, %DBConnection.ConnectionError{}}}
    end

    :timer.sleep(500)
    {:ok, pid} = Postgrex.start_link(context[:options])
    assert %Postgrex.Result{rows: []} =
      Postgrex.query!(pid, "SELECT pg_stat_get_activity($1)", [connection_id])
  end

  test "active client cancel", context do
    conn = context[:pid]
    :sys.suspend(conn)

    assert {:timeout, _} = catch_exit(query("SELECT 42", [], [pool_timeout: 0]))

    Process.flag(:trap_exit, true)
    :sys.resume(conn)

    assert [[42]] = query("SELECT 42", [])
  end

  test "active client DOWN", context do
    self_pid = self()
    conn = context[:pid]

    pid = spawn fn ->
      send self_pid, query("SELECT pg_sleep(0.2)", [])
    end

    :timer.sleep(100)
    Process.flag(:trap_exit, true)
    capture_log fn ->
      Process.exit(pid, :shutdown)
      assert_receive {:EXIT, ^conn, {:shutdown, %DBConnection.ConnectionError{}}}
    end
  end

  test "queued client cancel", context do
    self_pid = self()
    Enum.each(1..10, fn _ ->
      spawn_link fn ->
        send self_pid, query("SELECT pg_sleep(0.1)", [])
      end
    end)

    :sys.suspend(context[:pid])

    assert {:timeout, _} = catch_exit(query("SELECT 42", [], [pool_timeout: 0]))

    :sys.resume(context[:pid])

    assert [[42]] = query("SELECT 42", [])

     Enum.each(1..10, fn _ ->
      assert_received [[:void]]
    end)
  end

  test "queued client DOWN", context do
    self_pid = self()
    Enum.each(1..10, fn _ ->
      spawn_link fn ->
        send self_pid, query("SELECT pg_sleep(0.1)", [])
      end
    end)

    :sys.suspend(context[:pid])

    pid = spawn fn ->
      send self_pid, query("SELECT 42", [])
    end

    :sys.resume(context[:pid])

    Process.exit(pid, :shutdown)

    assert [[42]] = query("SELECT 42", [])

    Enum.each(1..10, fn _ ->
      assert_received [[:void]]
    end)
  end
end
