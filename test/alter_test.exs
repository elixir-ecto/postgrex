defmodule AlterTest do
  use ExUnit.Case, async: false
  import Postgrex.TestHelper

  @moduletag :capture_log

  setup context do
    options = [
      database: "postgrex_test",
      backoff_type: :stop,
      prepare: context[:prepare] || :named
    ]

    reset = fn ->
      {:ok, pid} = Postgrex.start_link(options)
      Postgrex.query!(pid, "ALTER TABLE altering ALTER a type int2 USING 0", [])
      Postgrex.query!(pid, "DROP TABLE IF EXISTS missing_enum_table", [])
      Postgrex.query!(pid, "DROP TABLE IF EXISTS missing_comp_table", [])
      Postgrex.query!(pid, "DROP TYPE IF EXISTS missing_comp", [])
      Postgrex.query!(pid, "DROP TYPE IF EXISTS missing_enum", [])
      pid
    end

    on_exit(reset)

    pid = reset.()
    {:ok, [pid: pid, options: options]}
  end

  describe "cache statement" do
    test "after alter returns ok", context do
      query = fn -> query("SELECT a FROM altering", [], cache_statement: "select") end
      assert [] = query.()
      assert :ok = query("ALTER TABLE altering ALTER a TYPE int4", [])
      assert [] = query.()
    end

    test "after alter returns ok for bang queries", context do
      pid = context[:pid]

      query = fn ->
        Postgrex.query!(pid, "SELECT a FROM altering", [], cache_statement: "select")
      end

      assert %Postgrex.Result{rows: []} = query.()
      assert :ok = query("ALTER TABLE altering ALTER a TYPE int4", [])
      assert %Postgrex.Result{rows: []} = query.()
    end

    test "after alter returns ok in transaction", context do
      transaction(fn conn ->
        query = fn ->
          Postgrex.query(conn, "SELECT a FROM altering", [], cache_statement: "select")
        end

        assert {:ok, %{rows: []}} = query.()
        assert {:ok, _} = Postgrex.query(conn, "ALTER TABLE altering ALTER a TYPE int4", [])
        assert {:error, %Postgrex.Error{postgres: %{code: :feature_not_supported}}} = query.()
      end)
    end
  end

  test "prepare query, alter result and execute returns error", context do
    query = prepare("select", "SELECT a FROM altering")

    assert :ok = query("ALTER TABLE altering ALTER a TYPE int4", [])

    # types changed after query prepared
    assert %Postgrex.Error{postgres: %{code: :feature_not_supported}} = execute(query, [])

    assert [[42]] = query("SELECT 42", [])
  end

  test "prepare query, close, alter param and execute returns error", context do
    query = prepare("select", "SELECT a FROM altering WHERE a=$1")
    close(query)

    assert :ok = query("ALTER TABLE altering ALTER a TYPE timestamp USING CURRENT_TIMESTAMP", [])

    # can't cast int4 to timestamp
    assert %Postgrex.Error{postgres: %{code: :undefined_function}} = execute(query, [1])

    assert [[42]] = query("SELECT 42", [])
  end

  test "prepare query, close, alter and execute with params that cast", context do
    query1 = prepare("select", "SELECT a FROM altering WHERE a=$1")
    query2 = prepare("select", "SELECT a FROM altering")
    close(query1)
    close(query2)

    assert :ok = query("ALTER TABLE altering ALTER a TYPE int4", [])

    assert execute(query1, [1]) == []

    assert [[42]] = query("SELECT 42", [])

    assert execute(query2, []) == []

    assert [[42]] = query("SELECT 42", [])
  end

  test "prepare query, close, alter and execute with params that cast with error", context do
    query1 = prepare("select", "SELECT a FROM altering WHERE a=$1")
    query2 = prepare("select", "SELECT a FROM altering")
    close(query1)
    close(query2)

    assert :ok = query("ALTER TABLE altering ALTER a TYPE numeric", [])

    assert execute(query1, [1]) == []
    assert [[42]] = query("SELECT 42", [])

    assert execute(query1, [Decimal.new(1)]) == []
    assert [[42]] = query("SELECT 42", [])

    assert execute(query2, []) == []
    assert [[42]] = query("SELECT 42", [])
  end

  test "transaction with prepare query, alter result and execute errors", context do
    query = prepare("select", "SELECT a FROM altering")
    assert :ok = query("ALTER TABLE altering ALTER a TYPE int4", [])

    transaction(fn conn ->
      assert {:error, %Postgrex.Error{postgres: %{code: :feature_not_supported}}} =
               Postgrex.execute(conn, query, [])
    end)
  end

  test "transaction with prepare query, alter param and execute errors", context do
    query = prepare("select", "SELECT a FROM altering WHERE a=$1")
    close(query)

    assert :ok = query("ALTER TABLE altering ALTER a TYPE timestamp USING CURRENT_TIMESTAMP", [])

    transaction(fn conn ->
      assert {:error, %Postgrex.Error{postgres: %{code: :undefined_function}}} =
               Postgrex.execute(conn, query, [1])
    end)
  end

  test "transaction with prepare query, alter, close and execute with param cast succeeds",
       context do
    query1 = prepare("select", "SELECT a FROM altering WHERE a=$1")
    query2 = prepare("select", "SELECT a FROM altering")
    assert :ok = query("ALTER TABLE altering ALTER a TYPE int4", [])
    assert :ok = close(query1)
    assert :ok = close(query2)

    assert transaction(fn conn ->
             %Postgrex.Result{} = Postgrex.execute!(conn, query1, [1])
             :done
           end) == {:ok, :done}

    assert [[42]] = query("SELECT 42", [])

    assert transaction(fn conn ->
             %Postgrex.Result{} = Postgrex.execute!(conn, query2, [])
             :done
           end) == {:ok, :done}

    assert [[42]] = query("SELECT 42", [])
  end

  test "transaction with prepare query, alter result and savepoint execute errors", context do
    query = prepare("select", "SELECT a FROM altering")
    assert :ok = query("ALTER TABLE altering ALTER a TYPE int4", [])

    transaction(fn conn ->
      assert {:error, %Postgrex.Error{postgres: %{code: :feature_not_supported}}} =
               Postgrex.execute(conn, query, [], mode: :savepoint)

      assert %Postgrex.Result{rows: [[42]]} = Postgrex.query!(conn, "SELECT 42", [])
    end)

    assert [[42]] = query("SELECT 42", [])
  end

  test "transaction with prepare query, alter param and savepoint execute errors", context do
    query = prepare("select", "SELECT a FROM altering WHERE a=$1")
    close(query)

    assert :ok = query("ALTER TABLE altering ALTER a TYPE timestamp USING CURRENT_TIMESTAMP", [])

    transaction(fn conn ->
      assert {:error, %Postgrex.Error{postgres: %{code: :undefined_function}}} =
               Postgrex.execute(conn, query, [1], mode: :savepoint)

      assert %Postgrex.Result{rows: [[42]]} = Postgrex.query!(conn, "SELECT 42", [])
    end)
  end

  test "transaction with prepare query, close, alter and savepoint execute with param cast succeeds",
       context do
    query1 = prepare("select", "SELECT a FROM altering WHERE a=$1")
    query2 = prepare("select", "SELECT a FROM altering")
    assert :ok = close(query1)
    assert :ok = close(query2)
    assert :ok = query("ALTER TABLE altering ALTER a TYPE int4", [])

    assert transaction(fn conn ->
             %Postgrex.Result{} = Postgrex.execute!(conn, query1, [1], mode: :savepoint)
             :done
           end) == {:ok, :done}

    assert [[42]] = query("SELECT 42", [])

    assert transaction(fn conn ->
             %Postgrex.Result{} = Postgrex.execute!(conn, query2, [], mode: :savepoint)
             :done
           end) == {:ok, :done}

    assert [[42]] = query("SELECT 42", [])
  end

  @tag prepare: :unnamed
  test "prepare unnamed query, alter and execute with param cast succeeds", context do
    query1 = prepare("select", "SELECT a FROM altering WHERE a=$1")
    query2 = prepare("select", "SELECT a FROM altering")
    assert :ok = query("ALTER TABLE altering ALTER a TYPE int4", [])

    assert execute(query1, [1]) == []

    assert [[42]] = query("SELECT 42", [])

    assert execute(query2, []) == []
  end

  @tag prepare: :unnamed
  test "transaction with prepare unnamed query, alter and savepoint execute with param cast succeeds",
       context do
    query1 = prepare("select", "SELECT a FROM altering WHERE a=$1")
    query2 = prepare("select", "SELECT a FROM altering")
    assert :ok = query("ALTER TABLE altering ALTER a TYPE int4", [])

    assert transaction(fn conn ->
             %Postgrex.Result{} = Postgrex.execute!(conn, query1, [1], mode: :savepoint)
             :done
           end) == {:ok, :done}

    assert [[42]] = query("SELECT 42", [])

    assert transaction(fn conn ->
             %Postgrex.Result{} = Postgrex.execute!(conn, query2, [], mode: :savepoint)
             :done
           end) == {:ok, :done}

    assert [[42]] = query("SELECT 42", [])
  end

  test "transaction with prepare query, alter result and stream errors", context do
    query = prepare("select", "SELECT a FROM altering")
    assert :ok = query("ALTER TABLE altering ALTER a TYPE int4", [])

    transaction(fn conn ->
      assert_raise Postgrex.Error, ~r"\(feature_not_supported\)", fn ->
        conn |> Postgrex.stream(query, []) |> Enum.to_list()
      end
    end)
  end

  test "transaction with prepare query, alter param and stream errors", context do
    query = prepare("select", "SELECT a FROM altering WHERE a=$1")
    close(query)

    assert :ok = query("ALTER TABLE altering ALTER a TYPE timestamp USING CURRENT_TIMESTAMP", [])

    transaction(fn conn ->
      assert_raise Postgrex.Error, ~r"\(undefined_function\)", fn ->
        conn |> Postgrex.stream(query, [1]) |> Enum.to_list()
      end
    end)
  end

  test "transaction with prepare query, alter, close and stream with param cast succeeds",
       context do
    query1 = prepare("select", "SELECT a FROM altering WHERE a=$1")
    query2 = prepare("select", "SELECT a FROM altering")
    assert :ok = query("ALTER TABLE altering ALTER a TYPE int4", [])
    assert :ok = close(query1)
    assert :ok = close(query2)

    assert transaction(fn conn ->
             stream = Postgrex.stream(conn, query1, [1])
             assert [%Postgrex.Result{}] = Enum.to_list(stream)
             :done
           end) == {:ok, :done}

    assert [[42]] = query("SELECT 42", [])

    assert transaction(fn conn ->
             stream = Postgrex.stream(conn, query2, [])
             assert [%Postgrex.Result{}] = Enum.to_list(stream)
             :done
           end) == {:ok, :done}

    assert [[42]] = query("SELECT 42", [])
  end

  test "transaction with prepare query, alter result and savepoint stream errors", context do
    query = prepare("select", "SELECT a FROM altering")
    assert :ok = query("ALTER TABLE altering ALTER a TYPE int4", [])

    transaction(fn conn ->
      assert_raise Postgrex.Error, ~r"\(feature_not_supported\)", fn ->
        conn |> Postgrex.stream(query, [], mode: :savepoint) |> Enum.to_list()
      end

      assert %Postgrex.Result{rows: [[42]]} = Postgrex.query!(conn, "SELECT 42", [])
    end)

    assert [[42]] = query("SELECT 42", [])
  end

  test "transaction with prepare query, alter param and savepoint stream errors", context do
    query = prepare("select", "SELECT a FROM altering WHERE a=$1")
    close(query)

    assert :ok = query("ALTER TABLE altering ALTER a TYPE timestamp USING CURRENT_TIMESTAMP", [])

    transaction(fn conn ->
      assert_raise Postgrex.Error, ~r"\(undefined_function\)", fn ->
        conn |> Postgrex.stream(query, [1], mode: :savepoint) |> Enum.to_list()
      end

      assert %Postgrex.Result{rows: [[42]]} = Postgrex.query!(conn, "SELECT 42", [])
    end)
  end

  test "transaction with prepare query, close, alter and savepoint stream with param cast succeeds",
       context do
    query1 = prepare("select", "SELECT a FROM altering WHERE a=$1")
    query2 = prepare("select", "SELECT a FROM altering")
    assert :ok = close(query1)
    assert :ok = close(query2)
    assert :ok = query("ALTER TABLE altering ALTER a TYPE int4", [])

    assert transaction(fn conn ->
             stream = Postgrex.stream(conn, query1, [1], mode: :savepoint)
             assert [%Postgrex.Result{}] = Enum.to_list(stream)
             :done
           end) == {:ok, :done}

    assert [[42]] = query("SELECT 42", [])

    assert transaction(fn conn ->
             stream = Postgrex.stream(conn, query2, [], mode: :savepoint)
             assert [%Postgrex.Result{}] = Enum.to_list(stream)
             :done
           end) == {:ok, :done}

    assert [[42]] = query("SELECT 42", [])
  end

  test "new oid is bootstrapped", context do
    assert :ok = query("CREATE TYPE missing_comp AS (a int, b int)", [])
    assert :ok = query("CREATE TABLE missing_comp_table (a missing_comp)", [])

    assert :ok = query("INSERT INTO missing_comp_table VALUES ($1)", [{1, 2}])
    assert [[{1, 2}]] = query("SELECT a FROM missing_comp_table", [])
  end

  test "new oids in param and result are bootstrapped", context do
    assert :ok = query("CREATE TYPE missing_enum AS ENUM ('missing')", [])
    assert :ok = query("CREATE TYPE missing_comp AS (a int, b int)", [])

    assert :ok =
             query(
               "CREATE TABLE missing_comp_table (a missing_comp, b missing_enum DEFAULT 'missing')",
               []
             )

    assert [["missing"]] =
             query("INSERT INTO missing_comp_table VALUES ($1, DEFAULT) RETURNING b", [{1, 2}])
  end

  test "new oids and their sub-type oids are bootstrapped", context do
    assert :ok = query("CREATE TYPE missing_enum AS ENUM ('missing')", [])
    assert :ok = query("CREATE TYPE missing_comp AS (a int, b missing_enum)", [])
    assert :ok = query("CREATE TABLE missing_comp_table (a missing_comp)", [])

    assert :ok = query("INSERT INTO missing_comp_table VALUES ($1)", [{1, "missing"}])
  end

  test "duplicate oid is bootstrapped", context do
    assert :ok = query("CREATE TYPE missing_comp AS (a int, b int)", [])
    assert :ok = query("CREATE TABLE missing_comp_table (a missing_comp, b missing_comp)", [])

    assert :ok = query("INSERT INTO missing_comp_table VALUES ($1, $2)", [{1, 2}, {3, 4}])
    assert [[{1, 2}, {3, 4}]] = query("SELECT a, b FROM missing_comp_table", [])
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

    assert transaction(fn conn ->
             named = Postgrex.prepare!(conn, "foo", "INSERT INTO missing_enum_table VALUES ($1)")

             %Postgrex.Result{rows: [[42]]} = Postgrex.execute!(conn, unnamed, [])

             assert %Postgrex.Result{command: :insert} =
                      Postgrex.execute!(conn, named, ["missing"])

             Postgrex.query!(conn, "CREATE TYPE missing_comp AS (a int, b int)", [])
             Postgrex.query!(conn, "CREATE TABLE missing_comp_table (a missing_comp)", [])

             unnamed = Postgrex.prepare!(conn, "", "SELECT 42")

             named2 =
               Postgrex.prepare!(conn, "bar", "INSERT INTO missing_comp_table VALUES ($1)",
                 mode: :savepoint
               )

             assert %Postgrex.Result{rows: [[42]]} =
                      Postgrex.execute!(conn, unnamed, [], mode: :savepoint)

             assert %Postgrex.Result{command: :insert} = Postgrex.execute!(conn, named2, [{1, 2}])
           end)
  end

  @tag prepare: :unnamed
  test "new oid is bootstrapped on prepare and prepared executes inside transaction with unnamed",
       context do
    assert :ok = query("CREATE TYPE missing_enum AS ENUM ('missing')", [])
    assert :ok = query("CREATE TABLE missing_enum_table (a missing_enum)", [])
    unnamed = prepare("", "SELECT 42")

    assert transaction(fn conn ->
             named = Postgrex.prepare!(conn, "foo", "INSERT INTO missing_enum_table VALUES ($1)")

             %Postgrex.Result{rows: [[42]]} = Postgrex.execute!(conn, unnamed, [])

             assert %Postgrex.Result{command: :insert} =
                      Postgrex.execute!(conn, named, ["missing"])

             Postgrex.query!(conn, "CREATE TYPE missing_comp AS (a int, b int)", [])
             Postgrex.query!(conn, "CREATE TABLE missing_comp_table (a missing_comp)", [])

             unnamed = Postgrex.prepare!(conn, "", "SELECT 42")

             named2 =
               Postgrex.prepare!(conn, "", "INSERT INTO missing_comp_table VALUES ($1)",
                 mode: :savepoint
               )

             assert %Postgrex.Result{rows: [[42]]} =
                      Postgrex.execute!(conn, unnamed, [], mode: :savepoint)

             assert %Postgrex.Result{command: :insert} = Postgrex.execute!(conn, named2, [{1, 2}])
           end)
  end

  test "new oid is bootstrapped inside transaction", context do
    assert :ok = query("CREATE TYPE missing_enum AS ENUM ('missing')", [])
    assert :ok = query("CREATE TABLE missing_enum_table (a missing_enum)", [])

    assert transaction(fn conn ->
             assert {:ok, %Postgrex.Result{num_rows: 1, command: :insert}} =
                      Postgrex.query(conn, "INSERT INTO missing_enum_table VALUES ($1)", [
                        "missing"
                      ])

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

    assert transaction(fn conn ->
             assert {:ok, %Postgrex.Result{num_rows: 1, command: :insert}} =
                      Postgrex.query(conn, "INSERT INTO missing_enum_table VALUES ($1)", [
                        "missing"
                      ])

             assert {:ok, %Postgrex.Result{rows: [["missing"]]}} =
                      Postgrex.query(conn, "SELECT a FROM missing_enum_table", [])

             Postgrex.query!(conn, "CREATE TYPE missing_comp AS (a int, b int)", [])
             Postgrex.query!(conn, "CREATE TABLE missing_comp_table (a missing_comp)", [])

             assert {:ok, %Postgrex.Result{num_rows: 1, command: :insert}} =
                      Postgrex.query(conn, "INSERT INTO missing_comp_table VALUES ($1)", [{1, 2}],
                        mode: :savepoint
                      )

             :done
           end) == {:ok, :done}
  end

  test "new oid is bootstrapped when preparing enumerable stream", context do
    assert :ok = query("CREATE TYPE missing_enum AS ENUM ('missing')", [])
    assert :ok = query("CREATE TABLE missing_enum_table (a missing_enum)", [])

    assert transaction(fn conn ->
             stream =
               Postgrex.stream(conn, "INSERT INTO missing_enum_table VALUES ($1)", ["missing"])

             assert [%Postgrex.Result{num_rows: 1, command: :insert}] = Enum.to_list(stream)

             Postgrex.query!(conn, "CREATE TYPE missing_comp AS (a int, b int)", [])
             Postgrex.query!(conn, "CREATE TABLE missing_comp_table (a missing_comp)", [])

             stream2 =
               Postgrex.stream(conn, "INSERT INTO missing_comp_table VALUES ($1)", [{1, 2}],
                 mode: :savepoint
               )

             assert [%Postgrex.Result{num_rows: 1, command: :insert}] = Enum.to_list(stream2)

             :done
           end) == {:ok, :done}
  end

  test "new oid is bootstrapped when preparing collectable stream", context do
    assert :ok = query("CREATE TYPE missing_enum AS ENUM ('missing')", [])
    assert :ok = query("CREATE TABLE missing_enum_table (a missing_enum)", [])

    assert transaction(fn conn ->
             stream =
               Postgrex.stream(conn, "INSERT INTO missing_enum_table VALUES ($1)", ["missing"])

             assert Enum.into(["foo"], stream) == stream

             Postgrex.query!(conn, "CREATE TYPE missing_comp AS (a int, b int)", [])
             Postgrex.query!(conn, "CREATE TABLE missing_comp_table (a missing_comp)", [])

             stream2 =
               Postgrex.stream(conn, "INSERT INTO missing_comp_table VALUES ($1)", [{1, 2}],
                 mode: :savepoint
               )

             assert Enum.into(["bar"], stream2) == stream2

             :done
           end) == {:ok, :done}
  end
end
