defmodule TransactionTest do
  use ExUnit.Case, async: true
  import Postgrex.TestHelper
  alias Postgrex.Connection, as: P

  setup context do
    transactions =
      case context[:mode] do
        :transaction -> :strict
        :savepoint   -> :naive
      end
    opts = [ database: "postgrex_test", transactions: transactions,
             backoff_type: :stop ]
    {:ok, pid} = P.start_link(opts)
    {:ok, [pid: pid]}
  end

  @tag mode: :transaction
  test "connection works after failure during commit transaction", context do
    assert transaction(fn(conn) ->
      assert {:error, %Postgrex.Error{postgres: %{code: :unique_violation}}} =
       P.query(conn, "insert into uniques values (1), (1);", [])
     assert {:error, %Postgrex.Error{postgres: %{code: :in_failed_sql_transaction}}} =
       P.query(conn, "SELECT 42", [])
      :hi
    end) == {:ok, :hi}
    assert [[42]] = query("SELECT 42", [])
  end

  @tag mode: :transaction
  test "connection works after failure during rollback transaction", context do
    assert transaction(fn(conn) ->
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

    capture_log fn ->
      assert (err = %Postgrex.Error{message: "unexpected postgres status: transaction"}) = query("BEGIN", [])

      pid = context[:pid]
      assert_receive {:EXIT, ^pid, {^err, _}}
    end
  end

  @tag mode: :transaction
  test "query commit returns error", context do
    Process.flag(:trap_exit, true)

    assert transaction(fn(conn) ->
      capture_log fn ->
        assert {:error, err = %Postgrex.Error{message: "unexpected postgres status: idle"}} =
          P.query(conn, "ROLLBACK", [])

        pid = context[:pid]
        assert_receive {:EXIT, ^pid, {^err, _}}
      end
      :hi
    end) == {:ok, :hi}
  end

  @tag mode: :transaction
  test "checkout when in transaction disconnects", context do
    Process.flag(:trap_exit, true)

    pid = context[:pid]
    :sys.replace_state(pid,
      fn(%{mod_state: %{state: state} = mod} = conn) ->
        %{conn | mod_state: %{mod | state: %{state | postgres: :transaction}}}
      end)
    capture_log fn ->
      assert {{%Postgrex.Error{message: "unexpected postgres status: transaction"}, [_|_]}, _} =
        catch_exit(query("SELECT 42", []))
    end
  end

  @tag mode: :transaction
  test "ping when transaction state mismatch disconnects" do
    Process.flag(:trap_exit, true)

    opts = [ database: "postgrex_test", transactions: :strict,
             idle_timeout: 10, backoff_type: :stop ]
    {:ok, pid} = P.start_link(opts)

    capture_log fn ->
      :sys.replace_state(pid,
        fn(%{mod_state: %{state: state} = mod} = conn) ->
          %{conn | mod_state: %{mod | state: %{state | postgres: :transaction}}}
        end)
      assert_receive {:EXIT, ^pid, {%Postgrex.Error{message: "unexpected postgres status: idle"}, [_|_]}}
    end
  end

  @tag mode: :transaction
  test "execute_many rolls back on error", context do
    query = prepare("unique", "insert into uniques values (1), (1);")
    assert %Postgrex.Error{postgres: %{code: :unique_violation}} =
      execute_many([{query, [], []}, {query, [], []}])
    assert [[42]] = query("SELECT 42", [])
  end

  @tag mode: :savepoint
  test "savepoint transaction releases savepoint", context do
    :ok = query("BEGIN", [])
    assert transaction(fn(conn) ->
      assert {:ok, _} = P.query(conn, "SELECT 42", [])
      :hi
    end, [mode: :savepoint]) == {:ok, :hi}
    assert [[42]] = query("SELECT 42", [])
    assert %Postgrex.Error{postgres: %{code: :invalid_savepoint_specification}} =
      query("RELEASE SAVEPOINT postgrex_savepoint", [])
    assert :ok = query("ROLLBACK", [])
  end

  @tag mode: :savepoint
  test "savepoint transaction rolls back to savepoint and releases", context do
    assert :ok = query("BEGIN", [])
    assert transaction(fn(conn) ->
      assert {:error, %Postgrex.Error{postgres: %{code: :unique_violation}}} =
        P.query(conn, "insert into uniques values (1), (1);", [])
      P.rollback(conn, :oops)
    end, [mode: :savepoint]) == {:error, :oops}
    assert [[42]] = query("SELECT 42", [])
    assert %Postgrex.Error{postgres: %{code: :invalid_savepoint_specification}} =
      query("RELEASE SAVEPOINT postgrex_savepoint", [])
    assert :ok = query("ROLLBACK", [])
  end
end
