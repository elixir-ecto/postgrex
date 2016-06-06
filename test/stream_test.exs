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
    assert {:ok, _} = transaction(fn(conn) ->
      assert {:ok, q} = Postgrex.prepare(conn, "", "SELECT * FROM generate_series(1, 3)")

      assert [[[1]]] == Postgrex.stream(conn, q, [], max_rows: 1)
        |> Stream.map(fn (%Result{rows: rows}) -> rows end)
        |> Enum.take(1)
    end)
  end

  test "streams query in chunks", context do
    assert {:ok, _} = transaction(fn(conn) ->
      assert {:ok, q} = Postgrex.prepare(conn, "", "SELECT * FROM generate_series(1, 3)")

      assert [[[1], [2]], [[3]]] == Postgrex.stream(conn, q, [], max_rows: 2)
        |> Stream.map(fn (%{rows: rows}) -> rows end)
        |> Enum.to_list
    end)
  end

  # this happens when number_of_rows % max_rows == 0
  # last chunk is empty
  #
  test "latest empty chunk is not emitted", context do
    assert {:ok, _} = transaction(fn(conn) ->
      assert {:ok, q} = Postgrex.prepare(conn, "", "SELECT * FROM generate_series(1, 3)")

      assert [[[1]], [[2]], [[3]]] == Postgrex.stream(conn, q, [], max_rows: 1)
        |> Stream.map(fn (%{rows: rows}) -> rows end)
        |> Enum.to_list
    end)
  end

  # actual characterization
  # see [50.2.3. Extended Query](http://www.postgresql.org/docs/9.5/static/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY)
  # >
  # > Named portals must be explicitly closed before they can be redefined by another Bind message
  # >
  test "rebind named portal fails", context do
    assert {:ok, _} = transaction(fn(conn) ->
      assert {:ok, q} = Postgrex.prepare(conn, "", "SELECT 42")
      stream = %Postgrex.Stream{portal: "E2MANY", query: q}

      assert {:ok, _} = Postgrex.execute(conn, stream, [])
      assert {:error, %{postgres: %{code: :duplicate_cursor}}} = Postgrex.execute(conn, stream, [])
    end)
  end

  # TODO fragile test
  # the portal name is hidden in stream state, there is no function to act on it (ie `execute(portal, 500)`)
  # code may use a unique name and still not close the portal
  #
  # I added a test seam in stream : when given a portal name, it will use it
  # otherwise, stream generate an _unique_ name
  #
  # another option would be to pass the portal to emitted result AND be able to
  test "stream closes named portal", context do
    assert {:ok, _} = transaction(fn(conn) ->
      assert {:ok, q} = Postgrex.prepare(conn, "", "SELECT 42")
      stream = Postgrex.stream(conn, q, [], portal: "P")

      assert [%{rows: [[42]]}] = stream |> Enum.take(1)
      assert [%{rows: [[42]]}] = stream |> Enum.take(1)
    end)
  end

  test "stream a closed query is ok (parse again)", context do
    assert {:ok, _} = transaction(fn(conn) ->
      assert {:ok, q42} = Postgrex.prepare(conn, "S42", "SELECT 42")
      assert Postgrex.close(conn, q42) == :ok

      assert [%{rows: [[42]]}] = Postgrex.stream(conn, q42, []) |> Enum.take(1)
    end)
  end

  test "stream a reopened statement raises", context do
    assert {:ok, _} = transaction(fn(conn) ->
      assert {:ok, q42} = Postgrex.prepare(conn, "ENOENT", "SELECT 42")
      assert Postgrex.close(conn, q42) == :ok

      assert {:ok, _} = Postgrex.prepare(conn, "ENOENT", "SELECT 41")

      err = assert_raise(Postgrex.Error, fn -> Postgrex.stream(conn, q42, []) |> Enum.take(1) end)
      assert %Postgrex.Error{postgres: %{code: :duplicate_prepared_statement}} = err

    end)
  end

  test "can stream prepared query on another connection", context do
    q = prepare("S42", "SELECT 42")

    {:ok, pid2} = Postgrex.start_link(context[:options])
    assert [%{rows: [[42]]}] = Postgrex.stream(pid2, q, []) |> Enum.take(1)
  end

  test "connection works after failure in binding state", context do
    q = prepare("S42", "insert into uniques values (CAST($1::text AS int))")

    err = assert_raise(Postgrex.Error, fn -> stream(q, ["EBADF"]) |> Enum.take(1) end)
    assert %Postgrex.Error{postgres: %{code: :invalid_text_representation}} = err

    assert [[42]] = query("SELECT 42", [])
  end

  test "prepared query q1 is reusable after execution of q2", context do
    q = prepare("41", "SELECT 41")
    assert [[42]] = query("SELECT 42", [])
    assert [%{rows: [[41]]}] = stream(q, []) |> Enum.take(1)
  end

  test "connection reuses prepared query after failure in executing state", context do
    q1 = prepare("41", "SELECT 41")
    q2 = prepare(  "", "insert into uniques values (1), (1)")

    err = assert_raise(Postgrex.Error, fn -> stream(q2, []) |> Enum.take(1) end)
    assert %Postgrex.Error{postgres: %{code: :unique_violation}} = err

    assert [%{rows: [[41]]}] = stream(q1, []) |> Enum.take(1)
  end

  test "prepares unnamed query again when statement changed", context do
		q1 = prepare("", "SELECT 41")
    q2 = prepare("", "SELECT 42")
    assert [%{rows: [[42]]}] = stream(q2, []) |> Enum.take(1)
    assert [%{rows: [[41]]}] = stream(q1, []) |> Enum.take(1)
  end

  test "connection describes query when already prepared", context do
    prepare("", "SELECT 41")
    q = prepare("", "SELECT 41")
    assert [%{rows: [[41]]}] = stream(q, []) |> Enum.take(1)
  end

  test "raise on unprepared query", context do
    q = %Postgrex.Query{name: "ENOENT", statement: "SELECT 42"}

    assert_raise ArgumentError, ~r/has not been prepared/,
      fn -> stream(q, []) |> Enum.take(1) end
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

  test "streams MAY BE be nested using unnamed queries", context do
    assert {:ok, [[1], [1, 2]]} = range_x_range(context.pid,   "",   "", 1, 2)
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

  test "named queries, transaction with nested stream", context do
    assert {:ok, [1, 2]} == range_x_cast(context.pid , "S1", "S2")
  end

  test "unnamed queries, transaction with nested stream", context do
    assert {:ok, [1, 2]} == range_x_cast(context.pid ,   "",   "")
  end
end
