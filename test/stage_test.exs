defmodule StageTest do
  use ExUnit.Case, async: true
  import Postgrex.TestHelper
  alias Postgrex.Stage

  setup context do
    options = [database: "postgrex_test", backoff_type: :stop,
               prepare: context[:prepare] || :named]
    {:ok, pid} = Postgrex.start_link(options)
    {:ok, [pid: pid, options: options]}
  end

  test "COPY FROM STDIN", context do
    query = prepare("", "COPY uniques FROM STDIN")
    query("BEGIN", [])
    {:ok, stage} = Stage.copy(context.pid, query, [], [mode: :savepoint])
    mon = Process.monitor(stage)

    ["2\n", "3\n4\n"]
    |> Flow.from_enumerable()
    |> Flow.into_stages([stage], [max_demand: 1])

    assert_receive {:DOWN, ^mon, _, _, :normal}

    assert query("SELECT * FROM uniques", []) == [[2], [3], [4]]
    query("ROLLBACK", [])
  end
end
