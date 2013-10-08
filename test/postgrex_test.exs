defmodule PostgrexTest do
  use ExUnit.Case, async: false
  import Postgrex.TestHelper

  setup do
    { :ok, pid } = Postgrex.connect("localhost", "postgres", "postgres", "postgrex_test", [], [])
    { :ok, _ } = Postgrex.query(pid, "DROP TABLE IF EXISTS transaction")
    { :ok, _ } = Postgrex.query(pid, "CREATE TABLE transaction (data text)")
    { :ok, [pid: pid] }
  end

  teardown context do
    :ok = Postgrex.disconnect(context[:pid])
  end

  test "transaction returns value", context do
    assert 42 == Postgrex.in_transaction(context[:pid], fn -> 42 end)
  end

  test "one level transaction commits", context do
    Postgrex.in_transaction(context[:pid], fn ->
      :ok = query("INSERT INTO transaction VALUES ('hey')")
    end)
    assert [{"hey"}] = query("SELECT * FROM transaction")
  end

  test "two level transaction commits", context do
    Postgrex.in_transaction(context[:pid], fn ->
      Postgrex.in_transaction(context[:pid], fn ->
        :ok = query("INSERT INTO transaction VALUES ('hey')")
      end)
    end)
    assert [{"hey"}] = query("SELECT * FROM transaction")
  end

  test "one level transaction rollbacks on error", context do
    assert_raise(RuntimeError, "test", fn ->
      Postgrex.in_transaction(context[:pid], fn ->
        :ok = query("INSERT INTO transaction VALUES ('hey')")
        raise "test"
      end)
    end)
    assert [] = query("SELECT * FROM transaction")
  end

  test "two level transaction rollbacks on error 1", context do
    assert_raise(RuntimeError, "test", fn ->
      Postgrex.in_transaction(context[:pid], fn ->
        Postgrex.in_transaction(context[:pid], fn ->
          :ok = query("INSERT INTO transaction VALUES ('hey')")
          raise "test"
        end)
      end)
    end)
    assert [] = query("SELECT * FROM transaction")
  end

  test "two level transaction rollbacks on error 2", context do
    assert_raise(RuntimeError, "test", fn ->
      Postgrex.in_transaction(context[:pid], fn ->
        Postgrex.in_transaction(context[:pid], fn ->
          :ok = query("INSERT INTO transaction VALUES ('hey')")
        end)
        raise "test"
      end)
    end)
    assert [] = query("SELECT * FROM transaction")
  end

  test "two level transaction partly rollback", context do
    Postgrex.in_transaction(context[:pid], fn ->
      :ok = query("INSERT INTO transaction VALUES ('hey')")
      try do
        Postgrex.in_transaction(context[:pid], fn ->
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
