defmodule AlterTest do
  use ExUnit.Case, async: false
  import Postgrex.TestHelper

  setup context do
    options = [database: "postgrex_test", backoff_type: :stop,
               prepare: context[:prepare] || :named]

    on_exit(fn() ->
      {:ok, pid} = Postgrex.start_link(options)
      Postgrex.query(pid, "ALTER TABLE altering ALTER a type int2", [])
    end)

    {:ok, pid} = Postgrex.start_link(options)
    {:ok, [pid: pid, options: options]}
  end

  test "prepare query, alter and execute returns error", context do
    query = prepare("select", "SELECT a FROM altering")

    assert :ok = query("ALTER TABLE altering ALTER a TYPE int4", [])

    assert %Postgrex.Error{postgres: %{code: :feature_not_supported}} = execute(query, [])

    assert [[42]] = query("SELECT 42", [])
  end

  test "prepare query, close, alter and execute raises (and closes)", context do
    query1 = prepare("select", "SELECT a FROM altering WHERE a=$1")
    query2 = prepare("select", "SELECT a FROM altering")
    close(query1)
    close(query2)

    assert :ok = query("ALTER TABLE altering ALTER a TYPE int4", [])

    assert_raise ArgumentError, ~r"stale type information",
      fn() -> execute(query1, [1]) end
    assert [[42]] = query("SELECT 42", [])

    assert_raise ArgumentError, ~r"stale type information",
      fn() -> execute(query2, []) end
    assert [[42]] = query("SELECT 42", [])
  end

  test "transaction with prepare query, alter and execute errors", context do
    query = prepare("select", "SELECT a FROM altering")
    assert :ok = query("ALTER TABLE altering ALTER a TYPE int4", [])

    transaction(fn(conn) ->
      assert {:error, %Postgrex.Error{postgres: %{code: :feature_not_supported}}} =
        Postgrex.execute(conn, query, [])
    end)
  end

  test "transaction with prepare query, alter, close and execute raises", context do
    query1 = prepare("select", "SELECT a FROM altering WHERE a=$1")
    query2 = prepare("select", "SELECT a FROM altering")
    assert :ok = query("ALTER TABLE altering ALTER a TYPE int4", [])
    assert :ok = close(query1)
    assert :ok = close(query2)

    transaction(fn(conn) ->
      assert_raise ArgumentError, ~r"stale type information",
        fn() -> Postgrex.execute(conn, query1, [1]) end
    end)

    assert [[42]] = query("SELECT 42", [])

    transaction(fn(conn) ->
      assert_raise ArgumentError, ~r"stale type information",
        fn() -> Postgrex.execute(conn, query2, []) end
    end)

    assert [[42]] = query("SELECT 42", [])
  end

  test "transaction with prepare query, alter and savepoint execute errors", context do
    query = prepare("select", "SELECT a FROM altering")
    assert :ok = query("ALTER TABLE altering ALTER a TYPE int4", [])

    transaction(fn(conn) ->
      assert {:error, %Postgrex.Error{postgres: %{code: :feature_not_supported}}} =
        Postgrex.execute(conn, query, [], [mode: :savepoint])

      assert %Postgrex.Result{rows: [[42]]} = Postgrex.query!(conn, "SELECT 42", [])
    end)

    assert [[42]] = query("SELECT 42", [])
  end

  test "transaction with prepare query, close, alter and savepoint execute errors", context do
    query1 = prepare("select", "SELECT a FROM altering WHERE a=$1")
    query2 = prepare("select", "SELECT a FROM altering")
    assert :ok = close(query1)
    assert :ok = close(query2)
    assert :ok = query("ALTER TABLE altering ALTER a TYPE int4", [])

    transaction(fn(conn) ->
      assert_raise ArgumentError, ~r"stale type information",
        fn() -> Postgrex.execute(conn, query1, [1], [mode: :savepoint]) end

      assert_raise ArgumentError, ~r"stale type information",
        fn() -> Postgrex.execute(conn, query2, [], [mode: :savepoint]) end

      assert %Postgrex.Result{rows: [[42]]} = Postgrex.query!(conn, "SELECT 42", [])
    end)

    assert [[42]] = query("SELECT 42", [])
  end

  @tag prepare: :unnamed
  test "prepare query, alter and execute raises with unnamed", context do
    query1 = prepare("select", "SELECT a FROM altering WHERE a=$1")
    query2 = prepare("select", "SELECT a FROM altering")
    assert :ok = query("ALTER TABLE altering ALTER a TYPE int4", [])

    assert_raise ArgumentError, ~r"stale type information",
      fn() -> execute(query1, [1]) end

    assert [[42]] = query("SELECT 42", [])

    assert_raise ArgumentError, ~r"stale type information",
      fn() -> execute(query2, []) end

    assert [[42]] = query("SELECT 42", [])
  end

  @tag prepare: :unnamed
  test "transaction with prepare query, alter and execute raises with unnamed", context do
    query1 = prepare("select", "SELECT a FROM altering WHERE a=$1")
    query2 = prepare("select", "SELECT a FROM altering")
    assert :ok = query("ALTER TABLE altering ALTER a TYPE int4", [])

    transaction(fn(conn) ->
      assert_raise ArgumentError, ~r"stale type information",
        fn() -> Postgrex.execute(conn, query1, [1]) end

      assert %Postgrex.Result{rows: [[42]]} = Postgrex.query!(conn, "SELECT 42", [])

      assert_raise ArgumentError, ~r"stale type information",
        fn() -> Postgrex.execute(conn, query2, []) end

      assert %Postgrex.Result{rows: [[42]]} = Postgrex.query!(conn, "SELECT 42", [])
    end)

    assert [[42]] = query("SELECT 42", [])
  end

  @tag prepare: :unnamed
  test "transaction with prepare query, alter and savepoint execute errors with unnamed", context do
    query1 = prepare("select", "SELECT a FROM altering WHERE a=$1")
    query2 = prepare("select", "SELECT a FROM altering")
    assert :ok = query("ALTER TABLE altering ALTER a TYPE int4", [])

    transaction(fn(conn) ->
      assert_raise ArgumentError, ~r"stale type information",
        fn() -> Postgrex.execute(conn, query1, [1], [mode: :savepoint]) end

      assert %Postgrex.Result{rows: [[42]]} = Postgrex.query!(conn, "SELECT 42", [])

      assert_raise ArgumentError, ~r"stale type information",
        fn() -> Postgrex.execute(conn, query2, [], [mode: :savepoint]) end

      assert %Postgrex.Result{rows: [[42]]} = Postgrex.query!(conn, "SELECT 42", [])
    end)

    assert [[42]] = query("SELECT 42", [])
  end
end
