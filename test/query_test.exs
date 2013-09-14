defmodule QueryTest do
  use ExUnit.Case, async: true

  setup do
    { :ok, pid } = Postgrex.connect("localhost", "postgres", "postgres", "postgrex_test")
    { :ok, [pid: pid] }
  end

  teardown context do
    :ok = Postgrex.disconnect(context[:pid])
  end

  test "select query", context do
    assert { :ok, [{ "42" }] } = Postgrex.query(context[:pid], "SELECT 42")
  end
end
