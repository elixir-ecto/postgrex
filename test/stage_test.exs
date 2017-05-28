defmodule StageTest do
  use ExUnit.Case, async: true
  import Postgrex.TestHelper
  alias Postgrex.CopyConsumer
  alias Postgrex.Producer

  setup context do
    options = [database: "postgrex_test", backoff_type: :stop,
               prepare: context[:prepare] || :named]
    {:ok, pid} = Postgrex.start_link(options)
    {:ok, [pid: pid, options: options]}
  end

  test "consume COPY FROM STDIN", context do
    query = prepare("", "COPY uniques FROM STDIN")
    query("BEGIN", [])
    {:ok, stage} = CopyConsumer.start_link(context.pid, query, [], [mode: :savepoint])
    mon = Process.monitor(stage)

    ["2\n", "3\n4\n"]
    |> Flow.from_enumerable()
    |> Flow.into_stages([stage], [max_demand: 1])

    assert_receive {:DOWN, ^mon, _, _, :normal}

    assert query("SELECT * FROM uniques", []) == [[2], [3], [4]]
    query("ROLLBACK", [])
  end

  test "prepare and consume COPY FROM STDIN", context do
    query = "COPY uniques FROM STDIN"
    query("BEGIN", [])
    {:ok, stage} = CopyConsumer.start_link(context.pid, query, [], [mode: :savepoint])
    mon = Process.monitor(stage)

    ["2\n", "3\n4\n"]
    |> Flow.from_enumerable()
    |> Flow.into_stages([stage], [max_demand: 1])

    assert_receive {:DOWN, ^mon, _, _, :normal}

    assert query("SELECT * FROM uniques", []) == [[2], [3], [4]]
    query("ROLLBACK", [])
  end

  test "produce COPY TO STDOUT", context do
    query = "COPY (VALUES (1, 2), (3, 4)) TO STDOUT"
    parent = self()
    opts = [stream_mapper: fn(_, %{rows: rows}) -> send(parent, rows) end]
    {:ok, stage} = Producer.start_link(context.pid, query, [], opts)
    mon = Process.monitor(stage)

    assert [{stage, [cancel: :transient, max_demand: 1]}] |> GenStage.stream() |> Enum.to_list() == ["1\t2\n", "3\t4\n"]

    assert_receive ["1\t2\n"]
    assert_receive ["3\t4\n"]
    assert_receive []

    assert_receive {:DOWN, ^mon, _, _, :normal}
  end

  test "produce query in chunks", context do
    query = prepare("", "SELECT * FROM generate_series(1, 3)")
    parent = self()
    opts = [stream_mapper: fn(_, %{rows: rows}) -> send(parent, rows) end]
    {:ok, stage} = Producer.start_link(context.pid, query, [], opts)
    mon = Process.monitor(stage)

    assert [{stage, [cancel: :transient, max_demand: 2]}] |> GenStage.stream() |> Enum.to_list() == [[1], [2], [3]]

    assert_receive [[1], [2]]
    assert_receive [[3]]

    assert_receive {:DOWN, ^mon, _, _, :normal}
  end

  test "prepare and produce query in chunks", context do
    query = "SELECT * FROM generate_series(1, 3)"
    {:ok, stage} = Producer.start_link(context.pid, query, [])
    mon = Process.monitor(stage)

    assert [{stage, [cancel: :transient, max_demand: 2]}] |> GenStage.stream() |> Enum.to_list() == [[1], [2], [3]]

    assert_receive {:DOWN, ^mon, _, _, :normal}
  end
end
