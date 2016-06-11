defmodule StreamTest do
  use ExUnit.Case, async: true
  import Postgrex.TestHelper
  alias Postgrex.Result

  setup context do
    options = [database: "postgrex_test", backoff_type: :stop,
             prepare: context[:prepare] || :named]
    {:ok, pid} = Postgrex.start_link(options)
    {:ok, [pid: pid, options: options]}
  end

  test "MAY take part of stream", context do
    query = prepare("", "SELECT * FROM generate_series(1, 3)")
    transaction(fn(conn) ->
      assert [[[1]]] = Postgrex.stream(conn, query, [], max_rows: 1)
        |> Stream.map(fn(%Result{rows: rows}) -> rows end)
        |> Enum.take(1)
    end)
  end

  test "streams query in chunks", context do
    query = prepare("", "SELECT * FROM generate_series(1, 3)")
    transaction(fn(conn) ->
      assert [[[1], [2]], [[3]]] = Postgrex.stream(conn, query, [], max_rows: 2)
        |> Stream.map(fn(%Result{rows: rows}) -> rows end)
        |> Enum.to_list()
    end)
  end

  # this happens when number_of_rows % max_rows == 0
  # last chunk is empty
  #
  test "latest empty chunk is not emitted", context do
    query = prepare("", "SELECT * FROM generate_series(1, 3)")
    transaction(fn(conn) ->
           assert [[[1]], [[2]], [[3]]] = Postgrex.stream(conn, query, [], max_rows: 1)
        |> Stream.map(fn(%Result{rows: rows}) -> rows end)
        |> Enum.to_list()
    end)
  end

  # actual characterization
  # see [50.2.3. Extended Query](http://www.postgresql.org/docs/9.5/static/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY)
  # >
  # > Named portals must be explicitly closed before they can be redefined by another Bind message
  # >
  test "rebind named portal fails", context do
    query = prepare("", "SELECT 42")
    transaction(fn(conn) ->
      stream = Postgrex.stream(conn, query, [])
      stream = %Postgrex.Stream{stream | portal: "E2MANY"}

      _ = for _ <- stream do
        assert_raise Postgrex.Error, ~r"ERROR \(duplicate_cursor\)",
          fn() -> Enum.take(stream, 1) end
      end
    end)
  end

  test "stream closes named portal ", context do
    query = prepare("", "SELECT 42")

    transaction(fn(conn) ->
      stream = Postgrex.stream(conn, query, [])
      stream = %Postgrex.Stream{stream | portal: "CLOSES"}

      assert [%Result{rows: [[42]]}] = stream |> Enum.take(1)
      assert [%Result{rows: [[42]]}] = stream |> Enum.take(1)
    end)
  end

  test "prepare, stream and close", context do
    query = prepare("S42", "SELECT 42")
    transaction(fn(conn) ->
      assert [%Result{rows: [[42]]}] = Postgrex.stream(conn, query, []) |> Enum.take(1)
      assert [%Result{rows: [[42]]}] = Postgrex.stream(conn, query, []) |> Enum.take(1)
      assert Postgrex.close(conn, query) == :ok
    end)
  end

  test "prepare query and stream different query with same name raises", context do
    query = prepare("ENOENT", "SELECT 42")
    :ok = close(query)
    _ = prepare("ENOENT", "SELECT 41")
    transaction(fn(conn) ->
      assert_raise Postgrex.Error, ~r"ERROR \(duplicate_prepared_statement\)",
        fn -> Postgrex.stream(conn, query, []) |> Enum.take(1) end
    end)
  end

  test "prepare, close and stream", context do
    query = prepare("S42", "SELECT 42")
    :ok = close(query)
    transaction(fn(conn) ->
      assert [%Result{rows: [[42]]}] = Postgrex.stream(conn, query, []) |> Enum.take(1)
    end)
  end

  @tag prepare: :unnamed
  test "stream named is unnamed when named not allowed", context do
    assert (%Postgrex.Query{name: ""} = query) = prepare("42", "SELECT 42")
    assert [%Result{rows: [[42]]}] = stream(query, []) |> Enum.take(1)
    assert [%Result{rows: [[42]]}] = stream(query, []) |> Enum.take(1)
    assert :ok = close(query)
    assert [[42]] = query("SELECT 42", [])
  end

  test "stream query prepared query on another connection", context do
    query = prepare("S42", "SELECT 42")

    {:ok, pid2} = Postgrex.start_link(context[:options])
    assert [%Result{rows: [[42]]}] = Postgrex.stream(pid2, query, []) |> Enum.take(1)
    assert {:ok, %Result{rows: [[41]]}} = Postgrex.query(pid2, "SELECT 41", [])
  end

  test "raise when executing prepared query on connection with different types", context do
    query = prepare("S42", "SELECT 42")

    {:ok, pid2} = Postgrex.start_link([decode_binary: :reference] ++ context[:options])

    assert_raise ArgumentError, ~r"invalid types for the connection",
      fn() -> Postgrex.stream(pid2, query, []) |> Enum.take(1) end
  end

  test "connection works after failure in binding state", context do
    query = prepare("", "insert into uniques values (CAST($1::text AS int))")

    assert_raise Postgrex.Error, ~r"ERROR \(invalid_text_representation\)",
      fn -> stream(query, ["EBADF"]) |> Enum.take(1) end

    assert [[42]] = query("SELECT 42", [])
  end

  test "connection works after failure in executing state", context do
    query = prepare("", "insert into uniques values (1), (1)")

    assert_raise Postgrex.Error, ~r"ERROR \(unique_violation\)",
      fn -> stream(query, []) |> Enum.take(1) end

    assert [[42]] = query("SELECT 42", [])
  end

  test "connection reuses prepared query after query", context do
    query = prepare("", "SELECT 41")
    assert [[42]] = query("SELECT 42", [])
    assert [%Result{rows: [[41]]}] = stream(query, []) |> Enum.take(1)
  end

  test "connection forces prepare on stream after prepare of same name", context do
    query41 = prepare("", "SELECT 41")
    query42 = prepare("", "SELECT 42")
    assert [[42]] = execute(query42, [])
    assert [%Result{rows: [[41]]}] = stream(query41, []) |> Enum.take(1)
  end

  test "raise when trying to stream unprepared query", context do
    query = %Postgrex.Query{name: "ENOENT", statement: "SELECT 42"}

    assert_raise ArgumentError, ~r/has not been prepared/,
      fn -> stream(query, []) |> Enum.take(1) end
  end

  test "raise when trying to stream reserved query", context do
    query = prepare("", "BEGIN")

    assert_raise ArgumentError, ~r/uses reserved name/,
      fn -> stream(%{query | name: "POSTGREX COMMIT"}, []) |> Enum.take(1) end
  end

  test "stream struct interpolates to statement", context do
    query = prepare("", "BEGIN")
    assert "#{stream(query, [])}" == "BEGIN"
  end

  test "connection_id", context do
    query = prepare("", "SELECT pg_backend_pid()")
    assert [%Result{connection_id: connection_id, rows: [[backend_pid]]}] =
      stream(query, []) |> Enum.take(1)
    assert is_integer(connection_id)
    assert connection_id == backend_pid

    query = prepare("", "insert into uniques values (1), (1)")

    try do
      stream(query, []) |> Enum.take(1)
    rescue
      err ->
        assert %Postgrex.Error{connection_id: ^connection_id} = err
    end
  end

  defp range(pid, name, x, y) do
    {:ok, q} = Postgrex.prepare(pid, name, "SELECT * FROM generate_series(CAST($1 as int), $2)")
    Postgrex.stream(pid, q, [x, y], max_rows: 1)
    |> Stream.map(fn (res) -> :lists.flatten(res.rows) end)
  end

  # nest two ranges in a transaction, first query has name1, second has name2
  #
  defp range_x_range(pid, name1, name2, x, y) do
    Postgrex.transaction(pid, fn conn ->
      range(conn, name1, x, y)
      |> Stream.map(fn ([x]) -> range(conn, name2, 1, x) |> Enum.flat_map(&(&1)) end)
      |> Enum.to_list
    end)
  end

  test "streams can be nested using named queries", context do
    assert {:ok, [[1], [1, 2]]} = range_x_range(context.pid, "S1", "S2", 1, 2)
  end

  test "streams can be nested using unnamed queries", context do
    assert {:ok, [[1], [1, 2]]} = range_x_range(context.pid, "", "", 1, 2)
  end

  @tag prepare: :unnamed
  test "streams can be nested using named queries when names not allowed", context do
    assert {:ok, [[1], [1, 2]]} = range_x_range(context.pid, "S1", "S2", 1, 2)
  end

  defp range_x_cast(pid, name1, name2) do
    q1 = Postgrex.prepare!(pid, name1, "SELECT * FROM generate_series(1, 2)")
    q2 = Postgrex.prepare!(pid, name2, "SELECT CAST($1 as int)")

    Postgrex.transaction(pid, fn (conn) ->
      Postgrex.stream(conn, q1, [], max_rows: 1)
      |> Stream.map(fn %{rows: [[x]]} -> x end)
      |> Stream.map(fn x -> Postgrex.stream(conn, q2, [x], max_rows: 1) |> Enum.flat_map(fn res -> res.rows end) end)
      |> Enum.to_list
      |> :lists.flatten
    end)
  end

  test "transaction with nested named stream", context do
    assert {:ok, [1, 2]} == range_x_cast(context.pid, "S1", "S2")
  end

  test "transaction with nested unnamed stream", context do
    assert {:ok, [1, 2]} == range_x_cast(context.pid, "", "")
  end

  @tag prepare: :unnamed
  test "transaction with nested named stream when names not allowed", context do
    assert {:ok, [1, 2]} == range_x_cast(context.pid, "S1", "S2")
  end
end
