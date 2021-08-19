defmodule ClientTest do
  use ExUnit.Case
  import Postgrex.TestHelper
  import ExUnit.CaptureLog

  setup do
    opts = [database: "postgrex_test", backoff_type: :stop, max_restarts: 0]
    {:ok, pid} = Postgrex.start_link(opts)
    {:ok, [pid: pid, options: opts]}
  end

  test "active client timeout", context do
    conn = context[:pid]
    %Postgrex.Result{connection_id: connection_id} = Postgrex.query!(conn, "SELECT 42", [])

    Process.flag(:trap_exit, true)

    assert capture_log(fn ->
             assert [[_]] = query("SELECT pg_stat_get_activity($1)", [connection_id])

             case query("SELECT pg_sleep(10)", [], timeout: 50) do
               %Postgrex.Error{postgres: %{message: "canceling statement due to user request"}} ->
                 :ok

               %DBConnection.ConnectionError{message: "tcp recv: closed" <> _} ->
                 :ok

               other ->
                 flunk("unexpected result: #{inspect(other)}")
             end

             assert_receive {:EXIT, ^conn, :killed}
           end) =~ "disconnected: ** (DBConnection.ConnectionError)"

    :timer.sleep(500)
    {:ok, pid} = Postgrex.start_link(context[:options])

    assert %Postgrex.Result{rows: []} =
             Postgrex.query!(pid, "SELECT pg_stat_get_activity($1)", [connection_id])
  end

  test "active client DOWN", context do
    self_pid = self()
    conn = context[:pid]

    pid =
      spawn(fn ->
        send(self_pid, query("SELECT pg_sleep(0.2)", []))
      end)

    :timer.sleep(100)
    Process.flag(:trap_exit, true)

    capture_log(fn ->
      Process.exit(pid, :shutdown)
      assert_receive {:EXIT, ^conn, :killed}
    end)
  end
end
