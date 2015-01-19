defmodule TimeoutTest do
  use ExUnit.Case, async: true
  alias Postgrex.Connection, as: P

  test "does not crash on client timeout" do
    opts = [database: "postgrex_test"]
    {:ok, pid} = P.start_link(opts)

    assert {:ok, _} = P.query(pid, "SELECT 123", [], timeout: 100)
    assert {:timeout, _} = catch_exit P.query(pid, "SELECT pg_sleep(1)", [], timeout: 0)

    ref = Process.monitor(pid)
    refute_receive {:DOWN, ^ref, :process, ^pid, _reason}, 100
  end
end
