defmodule TransactionTest do
  use ExUnit.Case, async: true
  import Postgrex.TestHelper
  import ExUnit.CaptureLog
  alias Postgrex, as: P

  setup context do
    transactions =
      case context[:mode] do
        :transaction -> :strict
        :savepoint -> :naive
      end

    opts = [
      database: "postgrex_test",
      transactions: transactions,
      idle: :active,
      backoff_type: :stop,
      prepare: context[:prepare] || :named,
      max_restarts: 0,
      disconnect_on_error_codes: context[:disconnect_on_error_codes] || []
    ]

    {:ok, pid} = P.start_link(opts)
    {:ok, [pid: pid]}
  end

  @tag mode: :transaction
  test "connection works after failure during commit transaction", context do
    assert transaction(fn conn ->
             assert {:error, %Postgrex.Error{postgres: %{code: :unique_violation}}} =
                      P.query(conn, "insert into uniques values (1), (1);", [])

             assert {:error, %Postgrex.Error{postgres: %{code: :in_failed_sql_transaction}}} =
                      P.query(conn, "SELECT 42", [])

             :hi
           end) == {:error, :rollback}

    assert [[42]] = query("SELECT 42", [])
  end

  @tag mode: :transaction
  test "connection works after failure during rollback transaction", context do
    assert transaction(fn conn ->
             assert {:error, %Postgrex.Error{postgres: %{code: :unique_violation}}} =
                      P.query(conn, "insert into uniques values (1), (1);", [])

             assert {:error, %Postgrex.Error{postgres: %{code: :in_failed_sql_transaction}}} =
                      P.query(conn, "SELECT 42", [])

             P.rollback(conn, :oops)
           end) == {:error, :oops}

    assert [[42]] = query("SELECT 42", [])
  end

  @tag mode: :transaction
  test "query begin returns error", context do
    Process.flag(:trap_exit, true)

    assert capture_log(fn ->
             assert %Postgrex.Error{message: "unexpected postgres status: transaction"} =
                      query("BEGIN", [])

             pid = context[:pid]
             assert_receive {:EXIT, ^pid, :killed}
           end) =~ "** (Postgrex.Error) unexpected postgres status: transaction"
  end

  @tag mode: :transaction
  test "idle status during transaction returns error and disconnects", context do
    Process.flag(:trap_exit, true)

    assert transaction(fn conn ->
             assert capture_log(fn ->
                      assert {:error,
                              %Postgrex.Error{message: "unexpected postgres status: idle"}} =
                               P.query(conn, "ROLLBACK", [])

                      pid = context[:pid]
                      assert_receive {:EXIT, ^pid, :killed}
                    end) =~ "** (Postgrex.Error) unexpected postgres status: idle"

             :hi
           end) == {:error, :rollback}
  end

  @tag mode: :transaction
  @tag prepare: :unnamed
  test "transaction commits with unnamed queries", context do
    assert transaction(fn conn ->
             assert {:ok, %Postgrex.Result{rows: [[42]]}} = P.query(conn, "SELECT 42", [])
             :hi
           end) == {:ok, :hi}

    assert query("SELECT 42", []) == [[42]]
  end

  @tag mode: :transaction
  @tag prepare: :unnamed
  test "transaction rolls back with unnamed queries", context do
    assert transaction(fn conn ->
             P.rollback(conn, :oops)
           end) == {:error, :oops}

    assert query("SELECT 42", []) == [[42]]
  end

  @tag mode: :transaction
  @tag disconnect_on_error_codes: [:read_only_sql_transaction]
  test "transaction read-only only error disconnects with prepare and execute", context do
    Process.flag(:trap_exit, true)

    assert transaction(fn conn ->
             P.query!(conn, "SET TRANSACTION READ ONLY", []).connection_id

             {:ok, query} = P.prepare(conn, "query_1", "insert into uniques values (1);", [])

             assert capture_log(fn ->
                      {:error, %Postgrex.Error{postgres: %{code: :read_only_sql_transaction}}} =
                        P.execute(conn, query, [])

                      pid = context[:pid]
                      assert_receive {:EXIT, ^pid, :killed}
                    end) =~
                      "disconnected: ** (Postgrex.Error) ERROR 25006 (read_only_sql_transaction)"
           end)
  end

  @tag mode: :transaction
  @tag disconnect_on_error_codes: [:read_only_sql_transaction]
  test "transaction read-only only error disconnects with prepare, execute, and close", context do
    Process.flag(:trap_exit, true)

    assert transaction(fn conn ->
             P.query!(conn, "SET TRANSACTION READ ONLY", []).connection_id

             assert capture_log(fn ->
                      {:error, %Postgrex.Error{postgres: %{code: :read_only_sql_transaction}}} =
                        P.query(conn, "insert into uniques values (1);", [])

                      pid = context[:pid]
                      assert_receive {:EXIT, ^pid, :killed}
                    end) =~
                      "disconnected: ** (Postgrex.Error) ERROR 25006 (read_only_sql_transaction)"
           end)
  end

  @tag mode: :savepoint
  test "savepoint transaction releases savepoint", context do
    :ok = query("BEGIN", [])

    assert transaction(
             fn conn ->
               assert {:ok, %Postgrex.Result{rows: [[42]]}} = P.query(conn, "SELECT 42", [])
               :hi
             end,
             mode: :savepoint
           ) == {:ok, :hi}

    assert [[42]] = query("SELECT 42", [])

    assert %Postgrex.Error{postgres: %{code: :invalid_savepoint_specification}} =
             query("RELEASE SAVEPOINT postgrex_savepoint", [])

    assert :ok = query("ROLLBACK", [])
  end

  @tag mode: :savepoint
  test "savepoint transaction rolls back to savepoint and releases", context do
    assert :ok = query("BEGIN", [])

    assert transaction(
             fn conn ->
               assert {:error, %Postgrex.Error{postgres: %{code: :unique_violation}}} =
                        P.query(conn, "insert into uniques values (1), (1);", [])

               P.rollback(conn, :oops)
             end,
             mode: :savepoint
           ) == {:error, :oops}

    assert [[42]] = query("SELECT 42", [])

    assert %Postgrex.Error{postgres: %{code: :invalid_savepoint_specification}} =
             query("RELEASE SAVEPOINT postgrex_savepoint", [])

    assert :ok = query("ROLLBACK", [])
  end

  @tag mode: :savepoint
  @tag prepare: :unnamed
  test "savepoint transaction releases with unnamed queries", context do
    assert :ok = query("BEGIN", [])

    assert transaction(
             fn conn ->
               assert {:ok, %Postgrex.Result{rows: [[42]]}} = P.query(conn, "SELECT 42", [])
               :hi
             end,
             mode: :savepoint
           ) == {:ok, :hi}

    assert [[42]] = query("SELECT 42", [])

    assert %Postgrex.Error{postgres: %{code: :invalid_savepoint_specification}} =
             query("RELEASE SAVEPOINT postgrex_savepoint", [])

    assert :ok = query("ROLLBACK", [])
  end

  @tag mode: :savepoint
  @tag prepare: :unnamed
  test "savepoint transaction rolls back and releases with unnamed queries", context do
    assert :ok = query("BEGIN", [])

    assert transaction(
             fn conn ->
               P.rollback(conn, :oops)
             end,
             mode: :savepoint
           ) == {:error, :oops}

    assert [[42]] = query("SELECT 42", [])

    assert %Postgrex.Error{postgres: %{code: :invalid_savepoint_specification}} =
             query("RELEASE SAVEPOINT postgrex_savepoint", [])

    assert :ok = query("ROLLBACK", [])
  end

  @tag mode: :savepoint
  test "savepoint transaction rollbacks on failed", context do
    assert :ok = query("BEGIN", [])

    assert transaction(
             fn conn ->
               assert {:error, %Postgrex.Error{postgres: %{code: :unique_violation}}} =
                        P.query(conn, "insert into uniques values (1), (1);", [], [])

               assert {:error, %Postgrex.Error{postgres: %{code: :in_failed_sql_transaction}}} =
                        P.query(conn, "SELECT 42", [])

               :hi
             end,
             mode: :savepoint
           ) == {:error, :rollback}

    assert [[42]] = query("SELECT 42", [])
    assert :ok = query("ROLLBACK", [])
  end

  @tag mode: :savepoint
  @tag prepare: :unnamed
  test "savepoint transaction rollbacks on failed with unnamed queries", context do
    assert :ok = query("BEGIN", [])

    assert transaction(
             fn conn ->
               assert {:error, %Postgrex.Error{postgres: %{code: :unique_violation}}} =
                        P.query(conn, "insert into uniques values (1), (1);", [], [])

               :hi
             end,
             mode: :savepoint
           ) == {:error, :rollback}

    assert [[42]] = query("SELECT 42", [])
    assert :ok = query("ROLLBACK", [])
  end

  @tag mode: :transaction
  test "transaction works after encode failure in savepoint query", context do
    assert transaction(fn conn ->
             assert_raise DBConnection.EncodeError, fn ->
               P.query(conn, "SELECT $1::boolean", [:someatom], mode: :savepoint)
             end

             assert {:ok, %Postgrex.Result{rows: [[true]]}} = P.query(conn, "SELECT true", [])
             :hi
           end) == {:ok, :hi}

    assert [[42]] = query("SELECT 42", [])
  end

  @tag mode: :transaction
  test "transaction works after encode failure in savepoint query with cache_statement",
       context do
    assert transaction(fn conn ->
             stmt = "SELECT $1::boolean"
             opts = [mode: :savepoint, cache_statement: "select_boolean"]
             {:ok, _} = P.query(conn, stmt, [true], opts)

             assert_raise DBConnection.EncodeError, fn ->
               P.query(conn, stmt, [:someatom], opts)
             end

             {:ok, _} = P.query(conn, "SELECT true", [], [])

             assert_raise DBConnection.EncodeError, fn ->
               P.query(conn, stmt, [:someatom], opts)
             end

             {:ok, _} = P.query(conn, "SELECT true", [], [])
             :hi
           end) == {:ok, :hi}
  end

  @tag mode: :transaction
  test "transaction works after describe failure in savepoint query with cache_statement",
       context do
    transaction(fn conn ->
      opts = [mode: :savepoint, cache_statement: "select"]
      assert {:ok, _} = Postgrex.query(conn, "SELECT 1", [], opts)
      assert {:error, _} = Postgrex.query(conn, "SELECT 1 FROM unknown", [], opts)
      assert {:ok, _} = Postgrex.query(conn, "SELECT 1", [], opts)
    end)
  end

  @tag mode: :transaction
  test "transaction works after execute failure in savepoint query", context do
    assert transaction(fn conn ->
             assert {:error, %Postgrex.Error{postgres: %{code: :unique_violation}}} =
                      P.query(conn, "insert into uniques values (1), (1);", [], mode: :savepoint)

             assert {:ok, %Postgrex.Result{rows: [[42]]}} = P.query(conn, "SELECT 42", [])
             :hi
           end) == {:ok, :hi}

    assert [[42]] = query("SELECT 42", [])
  end

  @tag mode: :transaction
  test "transaction works after parse failure in savepoint query", context do
    assert transaction(fn conn ->
             assert {:error, %Postgrex.Error{postgres: %{code: :syntax_error}}} =
                      P.query(conn, "NOT SQL", [], mode: :savepoint)

             assert {:ok, %Postgrex.Result{rows: [[42]]}} = P.query(conn, "SELECT 42", [])
             :hi
           end) == {:ok, :hi}

    assert [[42]] = query("SELECT 42", [])
  end

  @tag mode: :transaction
  test "transaction works after parse failure in savepoint prepare", context do
    assert transaction(fn conn ->
             assert {:error, %Postgrex.Error{postgres: %{code: :syntax_error}}} =
                      P.prepare(conn, "", "NOT SQL", mode: :savepoint)

             assert {:ok, %Postgrex.Result{rows: [[42]]}} = P.query(conn, "SELECT 42", [])

             assert {:error, %Postgrex.Error{postgres: %{code: :syntax_error}}} =
                      P.prepare(conn, "insert", "NOT SQL", mode: :savepoint)

             assert {:ok, %Postgrex.Result{rows: [[42]]}} = P.query(conn, "SELECT 42", [])
             :hi
           end) == {:ok, :hi}

    assert [[42]] = query("SELECT 42", [])
  end

  @tag mode: :transaction
  test "savepoint query releases savepoint in transaction", context do
    assert transaction(fn conn ->
             assert {:ok, %Postgrex.Result{rows: [[42]]}} =
                      P.query(conn, "SELECT 42", [], mode: :savepoint)

             assert {:error, %Postgrex.Error{postgres: %{code: :invalid_savepoint_specification}}} =
                      P.query(conn, "RELEASE SAVEPOINT postgrex_query", [])

             P.rollback(conn, :oops)
           end) == {:error, :oops}

    assert [[42]] = query("SELECT 42", [])
  end

  @tag mode: :transaction
  test "savepoint query does not rollback on savepoint error", context do
    assert transaction(fn conn ->
             assert {:ok, _} = P.query(conn, "SAVEPOINT postgrex_query", [])

             assert {:error, %Postgrex.Error{postgres: %{code: :unique_violation}}} =
                      P.query(conn, "INSERT INTO uniques VALUES (1), (1)", [])

             assert {:error, %DBConnection.TransactionError{message: "transaction is aborted"}} =
                      P.query(conn, "SELECT 42", [], mode: :savepoint)

             assert {:error, %Postgrex.Error{postgres: %{code: :in_failed_sql_transaction}}} =
                      P.query(conn, "SELECT 42", [])

             P.rollback(conn, :oops)
           end) == {:error, :oops}

    assert [[42]] = query("SELECT 42", [])
  end

  @tag mode: :transaction
  test "savepoint query disconnects on release savepoint error", context do
    Process.flag(:trap_exit, true)

    assert transaction(fn conn ->
             assert {:error, %Postgrex.Error{postgres: %{code: :invalid_savepoint_specification}}} =
                      P.query(conn, "RELEASE SAVEPOINT postgrex_query", [], mode: :savepoint)

             assert {:error, %DBConnection.ConnectionError{message: "connection is closed" <> _}} =
                      P.query(conn, "SELECT 42", [])

             P.rollback(conn, :oops)
           end) == {:error, :oops}
  end

  @tag mode: :transaction
  test "savepoint query rolls back and releases savepoint in transaction", context do
    assert transaction(fn conn ->
             assert {:error, %Postgrex.Error{postgres: %{code: :unique_violation}}} =
                      P.query(conn, "insert into uniques values (1), (1);", [], mode: :savepoint)

             assert {:error, %Postgrex.Error{postgres: %{code: :invalid_savepoint_specification}}} =
                      P.query(conn, "RELEASE SAVEPOINT postgrex_query", [])

             P.rollback(conn, :oops)
           end) == {:error, :oops}

    assert [[42]] = query("SELECT 42", [])
  end

  @tag mode: :transaction
  @tag prepare: :unnamed
  test "unnamed savepoint query releases savepoint in transaction", context do
    assert transaction(fn conn ->
             assert {:ok, %Postgrex.Result{rows: [[42]]}} =
                      P.query(conn, "SELECT 42", [], mode: :savepoint)

             assert {:error, %Postgrex.Error{postgres: %{code: :invalid_savepoint_specification}}} =
                      P.query(conn, "RELEASE SAVEPOINT postgrex_query", [])

             P.rollback(conn, :oops)
           end) == {:error, :oops}

    assert [[42]] = query("SELECT 42", [])
  end

  @tag mode: :transaction
  test "unnamed savepoint query rolls back and releases savepoint in transaction", context do
    assert transaction(fn conn ->
             assert {:error, %Postgrex.Error{postgres: %{code: :unique_violation}}} =
                      P.query(conn, "insert into uniques values (1), (1);", [], mode: :savepoint)

             assert {:error, %Postgrex.Error{postgres: %{code: :invalid_savepoint_specification}}} =
                      P.query(conn, "RELEASE SAVEPOINT postgrex_query", [])

             P.rollback(conn, :oops)
           end) == {:error, :oops}

    assert [[42]] = query("SELECT 42", [])
  end

  @tag mode: :transaction
  test "transaction works after failure in savepoint query binding state", context do
    assert transaction(fn conn ->
             statement = "insert into uniques values (CAST($1::text AS int))"

             assert {:error, %Postgrex.Error{postgres: %{code: :invalid_text_representation}}} =
                      P.query(conn, statement, ["invalid"], mode: :savepoint)

             assert {:ok, %Postgrex.Result{rows: [[42]]}} = P.query(conn, "SELECT 42", [])
             :hi
           end) == {:ok, :hi}

    assert [[42]] = query("SELECT 42", [])
  end

  @tag mode: :transaction
  test "transaction works after failure in savepoint query executing state", context do
    assert transaction(fn conn ->
             assert {:error, %Postgrex.Error{postgres: %{code: :unique_violation}}} =
                      P.query(conn, "insert into uniques values (1), (1);", [], mode: :savepoint)

             assert {:ok, %Postgrex.Result{rows: [[42]]}} = P.query(conn, "SELECT 42", [])
             :hi
           end) == {:ok, :hi}

    assert [[42]] = query("SELECT 42", [])
  end

  @tag mode: :transaction
  @tag prepare: :unnamed
  test "transaction works after failure in unammed savepoint query parsing state", context do
    assert transaction(fn conn ->
             assert {:error, %Postgrex.Error{postgres: %{code: :unique_violation}}} =
                      P.query(conn, "insert into uniques values (1), (1);", [], mode: :savepoint)

             assert {:ok, %Postgrex.Result{rows: [[42]]}} = P.query(conn, "SELECT 42", [])
             :hi
           end) == {:ok, :hi}

    assert [[42]] = query("SELECT 42", [])
  end

  @tag mode: :transaction
  @tag prepare: :unnamed
  test "transaction works after failure in unnamed savepoint query binding state", context do
    assert transaction(fn conn ->
             statement = "insert into uniques values (CAST($1::text AS int))"

             assert {:error, %Postgrex.Error{postgres: %{code: :invalid_text_representation}}} =
                      P.query(conn, statement, ["invalid"], mode: :savepoint)

             assert {:ok, %Postgrex.Result{rows: [[42]]}} = P.query(conn, "SELECT 42", [])
             :hi
           end) == {:ok, :hi}

    assert [[42]] = query("SELECT 42", [])
  end

  @tag mode: :transaction
  @tag prepare: :unnamed
  test "transaction works after failure in unnamed savepoint query executing state", context do
    assert transaction(fn conn ->
             assert {:error, %Postgrex.Error{postgres: %{code: :unique_violation}}} =
                      P.query(conn, "insert into uniques values (1), (1);", [], mode: :savepoint)

             assert {:ok, %Postgrex.Result{rows: [[42]]}} = P.query(conn, "SELECT 42", [])
             :hi
           end) == {:ok, :hi}

    assert [[42]] = query("SELECT 42", [])
  end

  @tag mode: :savepoint
  test "savepoint transaction works after failure in savepoint query parsing state", context do
    assert :ok = query("BEGIN", [])

    assert transaction(
             fn conn ->
               assert {:error, %Postgrex.Error{postgres: %{code: :unique_violation}}} =
                        P.query(conn, "insert into uniques values (1), (1);", [], mode: :savepoint)

               assert {:ok, %Postgrex.Result{rows: [[42]]}} = P.query(conn, "SELECT 42", [])
               :hi
             end,
             mode: :savepoint
           ) == {:ok, :hi}

    assert [[42]] = query("SELECT 42", [])
    assert :ok = query("ROLLBACK", [])
  end

  @tag mode: :savepoint
  test "savepoint transaction works after failure in savepoint query binding state", context do
    assert :ok = query("BEGIN", [])

    assert transaction(
             fn conn ->
               statement = "insert into uniques values (CAST($1::text AS int))"

               assert {:error, %Postgrex.Error{postgres: %{code: :invalid_text_representation}}} =
                        P.query(conn, statement, ["invalid"], mode: :savepoint)

               assert {:ok, %Postgrex.Result{rows: [[42]]}} = P.query(conn, "SELECT 42", [])
               :hi
             end,
             mode: :savepoint
           ) == {:ok, :hi}

    assert [[42]] = query("SELECT 42", [])
    assert :ok = query("ROLLBACK", [])
  end

  @tag mode: :savepoint
  test "savepoint transaction works after failure in savepoint query executing state", context do
    assert :ok = query("BEGIN", [])

    assert transaction(
             fn conn ->
               assert {:error, %Postgrex.Error{postgres: %{code: :unique_violation}}} =
                        P.query(conn, "insert into uniques values (1), (1);", [], mode: :savepoint)

               assert {:ok, %Postgrex.Result{rows: [[42]]}} = P.query(conn, "SELECT 42", [])
               :hi
             end,
             mode: :savepoint
           ) == {:ok, :hi}

    assert [[42]] = query("SELECT 42", [])
    assert :ok = query("ROLLBACK", [])
  end

  @tag mode: :transaction
  test "transaction shows correct transaction status", context do
    pid = context[:pid]
    opts = [mode: :transaction]

    assert DBConnection.status(pid, opts) == :idle
    assert query("SELECT 42", []) == [[42]]
    assert DBConnection.status(pid, opts) == :idle

    assert DBConnection.transaction(
             pid,
             fn conn ->
               assert DBConnection.status(conn, opts) == :transaction

               assert {:error, %Postgrex.Error{postgres: %{code: :unique_violation}}} =
                        P.query(conn, "insert into uniques values (1), (1);", [], opts)

               assert DBConnection.status(conn, opts) == :error

               assert {:error, %Postgrex.Error{postgres: %{code: :in_failed_sql_transaction}}} =
                        P.query(conn, "SELECT 42", [], opts)

               assert DBConnection.status(conn, opts) == :error
             end,
             opts
           ) == {:error, :rollback}

    assert DBConnection.status(pid, opts) == :idle
    assert query("SELECT 42", []) == [[42]]
    assert DBConnection.status(pid) == :idle
  end

  @tag mode: :transaction
  test "commit log entries", context do
    assert transaction(
             fn conn ->
               P.query(conn, "SELECT 42", [])
             end,
             log: &send(self(), &1)
           )

    assert_receive %DBConnection.LogEntry{} = entry
    assert {:ok, %DBConnection{}, %Postgrex.Result{command: :begin}} = entry.result

    assert_receive %DBConnection.LogEntry{} = entry
    assert {:ok, %Postgrex.Result{command: :commit}} = entry.result
  end

  @tag mode: :transaction
  test "rollback log entries", context do
    assert transaction(
             fn conn ->
               P.rollback(conn, :hi)
             end,
             log: &send(self(), &1)
           ) == {:error, :hi}

    assert_receive %DBConnection.LogEntry{} = entry
    assert {:ok, %DBConnection{}, %Postgrex.Result{command: :begin}} = entry.result

    assert_receive %DBConnection.LogEntry{} = entry
    assert {:ok, %Postgrex.Result{command: :rollback}} = entry.result
  end

  @tag mode: :transaction
  test "rollback from failed query log entries", context do
    assert transaction(
             fn conn ->
               assert {:error, %Postgrex.Error{postgres: %{code: :unique_violation}}} =
                        P.query(conn, "insert into uniques values (1), (1);", [])

               :hi
             end,
             log: &send(self(), &1)
           ) == {:error, :rollback}

    assert_receive %DBConnection.LogEntry{} = entry
    assert {:ok, %DBConnection{}, %Postgrex.Result{command: :begin}} = entry.result

    assert_receive %DBConnection.LogEntry{} = entry
    assert {:ok, %Postgrex.Result{command: :rollback}} = entry.result
  end
end
