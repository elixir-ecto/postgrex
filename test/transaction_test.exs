defmodule TransactionTest do
  use ExUnit.Case, async: false
  import Postgrex.TestHelper
  alias Postgrex.Connection, as: P

  setup do
    opts = [ database: "postgrex_test" ]
    {:ok, pid} = P.start_link(opts)
    {:ok, _} = P.query(pid, "DROP TABLE IF EXISTS transaction")
    {:ok, _} = P.query(pid, "CREATE TABLE transaction (data text)")

    on_exit fn ->
      P.stop(pid)
    end

    {:ok, [pid: pid]}
  end

  test "transaction returns value", context do
    assert 42 == P.in_transaction(context[:pid], fn -> 42 end)
  end

  test "one level transaction commits", context do
    P.in_transaction(context[:pid], fn ->
      :ok = query("INSERT INTO transaction VALUES ('hey')")
    end)
    assert [{"hey"}] = query("SELECT * FROM transaction")
  end

  test "two level transaction commits", context do
    P.in_transaction(context[:pid], fn ->
      P.in_transaction(context[:pid], fn ->
        :ok = query("INSERT INTO transaction VALUES ('hey')")
      end)
    end)
    assert [{"hey"}] = query("SELECT * FROM transaction")
  end

  test "one level transaction rollbacks on error", context do
    assert_raise(RuntimeError, "test", fn ->
      P.in_transaction(context[:pid], fn ->
        :ok = query("INSERT INTO transaction VALUES ('hey')")
        raise "test"
      end)
    end)
    assert [] = query("SELECT * FROM transaction")
  end

  test "two level transaction rollbacks on error 1", context do
    assert_raise(RuntimeError, "test", fn ->
      P.in_transaction(context[:pid], fn ->
        P.in_transaction(context[:pid], fn ->
          :ok = query("INSERT INTO transaction VALUES ('hey')")
          raise "test"
        end)
      end)
    end)
    assert [] = query("SELECT * FROM transaction")
  end

  test "two level transaction rollbacks on error 2", context do
    assert_raise(RuntimeError, "test", fn ->
      P.in_transaction(context[:pid], fn ->
        P.in_transaction(context[:pid], fn ->
          :ok = query("INSERT INTO transaction VALUES ('hey')")
        end)
        raise "test"
      end)
    end)
    assert [] = query("SELECT * FROM transaction")
  end

  test "two level transaction partly rollback", context do
    P.in_transaction(context[:pid], fn ->
      :ok = query("INSERT INTO transaction VALUES ('hey')")
      try do
        P.in_transaction(context[:pid], fn ->
          :ok = query("INSERT INTO transaction VALUES ('you')")
          raise "test"
        end)
      rescue _ -> :ok
      end
      :ok = query("INSERT INTO transaction VALUES ('there')")
    end)
    assert [{"hey"}, {"there"}] = query("SELECT * FROM transaction")
  end
end
