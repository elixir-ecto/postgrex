defmodule QueryTest do
  use ExUnit.Case

  setup do
    { :ok, pid } = Postgrex.connect("localhost", "postgres", "postgres", "postgrex_test")
    { :ok, [pid: pid] }
  end

  teardown _context do
    :ok# TODO
  end

  test "select query", context do
    assert { :ok, [{ "42" }] } = Postgrex.query(context[:pid], "SELECT 42")
  end
end
