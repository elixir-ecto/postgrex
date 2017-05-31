defmodule AlterTest do
  use ExUnit.Case, async: false
  import Postgrex.TestHelper

  @moduletag :capture_log

  setup context do
    options = [database: "postgrex_test", backoff_type: :stop,
               prepare: context[:prepare] || :named]

    reset = fn() ->
      {:ok, pid} = Postgrex.start_link(options)
      Postgrex.query!(pid, "ALTER TABLE altering ALTER a type int2", [])
      Postgrex.query!(pid, "DROP TABLE IF EXISTS missing_enum_table", [])
      Postgrex.query!(pid, "DROP TABLE IF EXISTS missing_comp_table", [])
      Postgrex.query!(pid, "DROP TYPE IF EXISTS missing_enum", [])
      Postgrex.query!(pid, "DROP TYPE IF EXISTS missing_comp", [])
      pid
    end

    on_exit(reset)

    pid = reset.()
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

  test "new oid is bootstrapped", context do
    assert :ok = query("CREATE TYPE missing_comp AS (a int, b int)", [])
    assert :ok = query("CREATE TABLE missing_comp_table (a missing_comp)", [])

    assert :ok = query("INSERT INTO missing_comp_table VALUES ($1)", [{1, 2}])
    assert [[{1, 2}]] = query("SELECT a FROM missing_comp_table", [])
  end

  @tag prepare: :unnamed
  test "new oid is bootstrapped with unnamed", context do
    assert :ok = query("CREATE TYPE missing_enum AS ENUM ('missing')", [])
    assert :ok = query("CREATE TABLE missing_enum_table (a missing_enum)", [])
    assert :ok = query("INSERT INTO missing_enum_table VALUES ($1)", ["missing"])
    assert [["missing"]] = query("SELECT a FROM missing_enum_table", [])
  end

  test "new oid is bootstrapped on prepare and prepared executes", context do
    assert :ok = query("CREATE TYPE missing_enum AS ENUM ('missing')", [])
    assert :ok = query("CREATE TABLE missing_enum_table (a missing_enum)", [])
    unnamed = prepare("", "SELECT 42")

    named = prepare("foo", "INSERT INTO missing_enum_table VALUES ($1)")
    assert [[42]] = execute(unnamed, [])
    assert :ok = execute(named, ["missing"])
  end

  test "new oid is bootstrapped on prepare and prepared executes inside transaction", context do
    assert :ok = query("CREATE TYPE missing_enum AS ENUM ('missing')", [])
    assert :ok = query("CREATE TABLE missing_enum_table (a missing_enum)", [])
    unnamed = prepare("", "SELECT 42")

    assert transaction(fn(conn) ->
      named = Postgrex.prepare!(conn, "foo", "INSERT INTO missing_enum_table VALUES ($1)")

      %Postgrex.Result{rows: [[42]]} = Postgrex.execute!(conn, unnamed, [])

      assert %Postgrex.Result{command: :insert} = Postgrex.execute!(conn, named, ["missing"])

      Postgrex.query!(conn, "CREATE TYPE missing_comp AS (a int, b int)", [])
      Postgrex.query!(conn, "CREATE TABLE missing_comp_table (a missing_comp)", [])

      unnamed = Postgrex.prepare!(conn, "", "SELECT 42")
      named2 = Postgrex.prepare!(conn, "bar", "INSERT INTO missing_comp_table VALUES ($1)", [mode: :savepoint])

      assert %Postgrex.Result{rows: [[42]]} = Postgrex.execute!(conn, unnamed, [], [mode: :savepoint])

      assert %Postgrex.Result{command: :insert} = Postgrex.execute!(conn, named2, [{1, 2}])
    end)
  end

  test "new oid is bootstrapped inside transaction", context do
    assert :ok = query("CREATE TYPE missing_enum AS ENUM ('missing')", [])
    assert :ok = query("CREATE TABLE missing_enum_table (a missing_enum)", [])

    assert transaction(fn(conn) ->
      assert {:ok, %Postgrex.Result{num_rows: 1, command: :insert}} =
        Postgrex.query(conn, "INSERT INTO missing_enum_table VALUES ($1)", ["missing"])
      assert {:ok, %Postgrex.Result{rows: [["missing"]]}} =
        Postgrex.query(conn, "SELECT a FROM missing_enum_table", [])

      Postgrex.query!(conn, "CREATE TYPE missing_comp AS (a int, b int)", [])
      Postgrex.query!(conn, "CREATE TABLE missing_comp_table (a missing_comp)", [])

      assert {:ok, %Postgrex.Result{num_rows: 1, command: :insert}} =
        Postgrex.query(conn, "INSERT INTO missing_comp_table VALUES ($1)", [{1, 2}])

      :done
    end) == {:ok, :done}
  end

  @tag prepare: :unnamed
  test "new oid is bootstrapped inside transaction with unnamed", context do
    assert :ok = query("CREATE TYPE missing_enum AS ENUM ('missing')", [])
    assert :ok = query("CREATE TABLE missing_enum_table (a missing_enum)", [])

    assert transaction(fn(conn) ->
      assert {:ok, %Postgrex.Result{num_rows: 1, command: :insert}} =
        Postgrex.query(conn, "INSERT INTO missing_enum_table VALUES ($1)", ["missing"])
      assert {:ok, %Postgrex.Result{rows: [["missing"]]}} =
        Postgrex.query(conn, "SELECT a FROM missing_enum_table", [])

      Postgrex.query!(conn, "CREATE TYPE missing_comp AS (a int, b int)", [])
      Postgrex.query!(conn, "CREATE TABLE missing_comp_table (a missing_comp)", [])

      assert {:ok, %Postgrex.Result{num_rows: 1, command: :insert}} =
        Postgrex.query(conn, "INSERT INTO missing_comp_table VALUES ($1)", [{1, 2}], [mode: :savepoint])

      :done
    end) == {:ok, :done}
  end

  test "new oid is bootstrapped when preparing enumerable stream", context do
    assert :ok = query("CREATE TYPE missing_enum AS ENUM ('missing')", [])
    assert :ok = query("CREATE TABLE missing_enum_table (a missing_enum)", [])

    assert transaction(fn(conn) ->
      stream = Postgrex.stream(conn, "INSERT INTO missing_enum_table VALUES ($1)", ["missing"])

      assert [%Postgrex.Result{num_rows: 1, command: :insert}] = Enum.to_list(stream)

      Postgrex.query!(conn, "CREATE TYPE missing_comp AS (a int, b int)", [])
      Postgrex.query!(conn, "CREATE TABLE missing_comp_table (a missing_comp)", [])

      stream2 = Postgrex.stream(conn, "INSERT INTO missing_comp_table VALUES ($1)", [{1, 2}], [mode: :savepoint])

      assert [%Postgrex.Result{num_rows: 1, command: :insert}] = Enum.to_list(stream2)

      :done
    end) == {:ok, :done}
  end

  test "new oid is bootstrapped when preparing collectable stream", context do
    assert :ok = query("CREATE TYPE missing_enum AS ENUM ('missing')", [])
    assert :ok = query("CREATE TABLE missing_enum_table (a missing_enum)", [])

    assert transaction(fn(conn) ->
      stream = Postgrex.stream(conn, "INSERT INTO missing_enum_table VALUES ($1)", ["missing"])

      assert Enum.into(["foo"], stream) == stream

      Postgrex.query!(conn, "CREATE TYPE missing_comp AS (a int, b int)", [])
      Postgrex.query!(conn, "CREATE TABLE missing_comp_table (a missing_comp)", [])

      stream2 = Postgrex.stream(conn, "INSERT INTO missing_comp_table VALUES ($1)", [{1, 2}], [mode: :savepoint])

      assert Enum.into(["bar"], stream2) == stream2

      :done
    end) == {:ok, :done}
  end
end
