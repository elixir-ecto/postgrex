defmodule TransactionTest do
  use ExUnit.Case, async: true
  import Postgrex.TestHelper
  alias Postgrex.Connection, as: P

  setup do
    opts = [ database: "postgrex_test", transactions: :strict,
             backoff_type: :stop ]
    {:ok, pid} = P.start_link(opts)
    {:ok, [pid: pid]}
  end

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

  test "query begin returns error", context do
    Process.flag(:trap_exit, true)

    capture_log fn ->
      assert (err = %Postgrex.Error{message: "unexpected postgres status: transaction"}) = query("BEGIN", [])

      pid = context[:pid]
      assert_receive {:EXIT, ^pid, {^err, _}}
    end
  end

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
end
