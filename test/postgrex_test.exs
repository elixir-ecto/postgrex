defmodule PostgrexTest do
  use ExUnit.Case, async: false

  setup do
    { :ok, pid } = Postgrex.connect("localhost", "postgres", "postgres", "postgrex_test", [])
    :ok = Postgrex.query(pid, "DROP TABLE IF EXISTS transaction")
    :ok = Postgrex.query(pid, "CREATE TABLE transaction (data text)")
    { :ok, [pid: pid] }
  end

  teardown context do
    :ok = Postgrex.disconnect(context[:pid])
  end

  test "one level transaction commits", context do
    Postgrex.in_transaction(context[:pid], fn ->
      :ok = Postgrex.query(context[:pid], "INSERT INTO transaction VALUES ('hey')")
    end)
    assert { :ok, [{"hey"}] } = Postgrex.query(context[:pid], "SELECT * FROM transaction")
  end

  test "two level transaction commits", context do
    Postgrex.in_transaction(context[:pid], fn ->
      Postgrex.in_transaction(context[:pid], fn ->
        :ok = Postgrex.query(context[:pid], "INSERT INTO transaction VALUES ('hey')")
      end)
    end)
    assert { :ok, [{"hey"}] } = Postgrex.query(context[:pid], "SELECT * FROM transaction")
  end

  test "one level transaction rollbacks on error", context do
    assert_raise(RuntimeError, "test", fn ->
      Postgrex.in_transaction(context[:pid], fn ->
        :ok = Postgrex.query(context[:pid], "INSERT INTO transaction VALUES ('hey')")
        raise "test"
      end)
    end)
    assert { :ok, [] } = Postgrex.query(context[:pid], "SELECT * FROM transaction")
  end

  test "two level transaction rollbacks on error 1", context do
    assert_raise(RuntimeError, "test", fn ->
      Postgrex.in_transaction(context[:pid], fn ->
        Postgrex.in_transaction(context[:pid], fn ->
          :ok = Postgrex.query(context[:pid], "INSERT INTO transaction VALUES ('hey')")
          raise "test"
        end)
      end)
    end)
    assert { :ok, [] } = Postgrex.query(context[:pid], "SELECT * FROM transaction")
  end

  test "two level transaction rollbacks on error 2", context do
    assert_raise(RuntimeError, "test", fn ->
      Postgrex.in_transaction(context[:pid], fn ->
        Postgrex.in_transaction(context[:pid], fn ->
          :ok = Postgrex.query(context[:pid], "INSERT INTO transaction VALUES ('hey')")
        end)
        raise "test"
      end)
    end)
    assert { :ok, [] } = Postgrex.query(context[:pid], "SELECT * FROM transaction")
  end

  test "two level transaction partly rollback", context do
    Postgrex.in_transaction(context[:pid], fn ->
      :ok = Postgrex.query(context[:pid], "INSERT INTO transaction VALUES ('hey')")
      try do
        Postgrex.in_transaction(context[:pid], fn ->
          :ok = Postgrex.query(context[:pid], "INSERT INTO transaction VALUES ('you')")
          raise "test"
        end)
      rescue _ -> :ok
      end
      :ok = Postgrex.query(context[:pid], "INSERT INTO transaction VALUES ('there')")
    end)
    assert { :ok, [{"hey"}, {"there"}] } = Postgrex.query(context[:pid], "SELECT * FROM transaction")
  end
end
