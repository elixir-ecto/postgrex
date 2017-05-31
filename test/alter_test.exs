defmodule AlterTest do
  use ExUnit.Case, async: false
  import Postgrex.TestHelper

  setup context do
    options = [database: "postgrex_test", backoff_type: :stop,
               prepare: context[:prepare] || :named]

    reset = fn() ->
      {:ok, pid} = Postgrex.start_link(options)
      Postgrex.query!(pid, "ALTER TABLE altering ALTER a type int2", [])
      Postgrex.query!(pid, "DROP TABLE IF EXISTS missing_oid", [])
      Postgrex.query!(pid, "DROP TABLE IF EXISTS missing_oid2", [])
      Postgrex.query!(pid, "DROP TYPE IF EXISTS missing_enum", [])
      Postgrex.query!(pid, "DROP TYPE IF EXISTS missing_comp", [])
      Postgrex.query!(pid, "DROP TYPE IF EXISTS missing_range", [])
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
    assert :ok = query("CREATE TYPE missing_enum AS ENUM ('missing')", [])
    assert :ok = query("CREATE TYPE missing_comp AS (a int, b int)", [])
    assert :ok = query("CREATE TABLE missing_oid (a missing_enum, b missing_comp)", [])

    assert :ok = query("INSERT INTO missing_oid VALUES ($1, $2)", ["missing", {1, 2}])
    assert [["missing", {1, 2}]] = query("SELECT a,b FROM missing_oid", [])
  end

  @tag prepare: :unnamed
  test "new oid is bootstrapped with unnamed", context do
    assert :ok = query("CREATE TYPE missing_enum AS ENUM ('missing')", [])
    assert :ok = query("CREATE TYPE missing_comp AS (a int, b int)", [])
    assert :ok = query("CREATE TABLE missing_oid (a missing_enum, b missing_comp)", [])

    assert :ok = query("INSERT INTO missing_oid VALUES ($1, $2)", ["missing", {1, 2}])
    assert [["missing", {1, 2}]] = query("SELECT a,b FROM missing_oid", [])
  end

  test "new oid is bootstrapped on prepare and prepared executes", context do
    assert :ok = query("CREATE TYPE missing_enum AS ENUM ('missing')", [])
    assert :ok = query("CREATE TYPE missing_comp AS (a int, b int)", [])
    assert :ok = query("CREATE TABLE missing_oid (a missing_enum, b missing_comp)", [])
    unnamed = prepare("", "SELECT 42")
    named = prepare("foo", "INSERT INTO missing_oid VALUES ($1, $2)")

    assert [[42]] = execute(unnamed, [])
    assert :ok = execute(named, ["missing", {1, 2}])
  end

  test "new oid is bootstrapped on prepare and prepared executes inside transaction", context do
    assert :ok = query("CREATE TYPE missing_enum AS ENUM ('missing')", [])
    assert :ok = query("CREATE TYPE missing_comp AS (a int, b int)", [])
    assert :ok = query("CREATE TABLE missing_oid (a missing_enum, b missing_comp)", [])
    unnamed = prepare("", "SELECT 42")

    assert transaction(fn(conn) ->
      named = Postgrex.prepare!(conn, "foo", "INSERT INTO missing_oid VALUES ($1, $2)")

      %Postgrex.Result{rows: [[42]]} = Postgrex.execute!(conn, unnamed, [])

      assert %Postgrex.Result{command: :insert} =
        Postgrex.execute!(conn, named, ["missing", {1, 2}])

      Postgrex.query!(conn, "CREATE TYPE missing_range AS RANGE (subtype = int)", [])
      Postgrex.query!(conn, "CREATE TABLE missing_oid2 (a missing_range)", [])
      unnamed = Postgrex.prepare!(conn, "", "SELECT 42")
      named2 = Postgrex.prepare!(conn, "bar", "INSERT INTO missing_oid2 VALUES ($1)", [mode: :savepoint])

      assert %Postgrex.Result{rows: [[42]]} = Postgrex.execute!(conn, unnamed, [], [mode: :savepoint])

      assert %Postgrex.Result{command: :insert} =
        Postgrex.execute!(conn, named2, [%Postgrex.Range{lower: 1, upper: 2}])
    end)
  end

  test "new oid is bootstrapped inside transaction", context do
    assert transaction(fn(conn) ->
      Postgrex.query!(conn, "CREATE TYPE missing_enum AS ENUM ('missing')", [])
      Postgrex.query!(conn, "CREATE TYPE missing_comp AS (a int, b int)", [])
      Postgrex.query!(conn, "CREATE TABLE missing_oid (a missing_enum, b missing_comp)", [])

      assert {:ok, %Postgrex.Result{num_rows: 1, command: :insert}} =
        Postgrex.query(conn, "INSERT INTO missing_oid VALUES ($1, $2)", ["missing", {1, 2}])
      assert {:ok, %Postgrex.Result{rows: [["missing", {1, 2}]]}} =
        Postgrex.query(conn, "SELECT a,b FROM missing_oid", [])

      Postgrex.query!(conn, "CREATE TYPE missing_range AS RANGE (subtype = integer)", [])
      Postgrex.query!(conn, "CREATE TABLE missing_oid2 (a missing_range)", [])

      assert {:ok, %Postgrex.Result{num_rows: 1, command: :insert}} =
        Postgrex.query(conn, "INSERT INTO missing_oid2 VALUES ($1)", [%Postgrex.Range{lower: 1, upper: 2}])

      :done
    end) == {:ok, :done}
  end

  @tag prepare: :unnamed
  test "new oid is bootstrapped inside transaction with unnamed", context do
    assert transaction(fn(conn) ->
      Postgrex.query!(conn, "CREATE TYPE missing_enum AS ENUM ('missing')", [])
      Postgrex.query!(conn, "CREATE TYPE missing_comp AS (a int, b int)", [])
      Postgrex.query!(conn, "CREATE TABLE missing_oid (a missing_enum, b missing_comp)", [])

      assert {:ok, %Postgrex.Result{num_rows: 1, command: :insert}} =
        Postgrex.query(conn, "INSERT INTO missing_oid VALUES ($1, $2)", ["missing", {1, 2}])
      assert {:ok, %Postgrex.Result{rows: [["missing", {1, 2}]]}} =
        Postgrex.query(conn, "SELECT a,b FROM missing_oid", [])

      Postgrex.query!(conn, "CREATE TYPE missing_range AS RANGE (subtype = int)", [])
      Postgrex.query!(conn, "CREATE TABLE missing_oid2 (a missing_range)", [])

      assert {:ok, %Postgrex.Result{num_rows: 1, command: :insert}} =
        Postgrex.query(conn, "INSERT INTO missing_oid2 VALUES ($1)", [%Postgrex.Range{lower: 1, upper: 2}], [mode: :savepoint])

      :done
    end) == {:ok, :done}
  end
end
