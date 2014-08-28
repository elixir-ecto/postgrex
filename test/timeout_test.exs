defmodule TimeoutTest do
  use ExUnit.Case, async: true
  alias Postgrex.Connection, as: P

  test "timeout" do
    opts = [ database: "postgrex_test" ]
    {:ok, pid} = P.start_link(opts)

    assert {:ok, _} = P.query(pid, "SELECT pg_sleep(0.1)", [], timeout: 200)
    assert {:timeout, _} = catch_exit P.query(pid, "SELECT pg_sleep(0.1)", [], timeout: 0)
    :timer.sleep(100)
    assert {:noproc, _} = catch_exit P.query(pid, "SELECT pg_sleep(0.1)", [], timeout: 200)
  end
end
