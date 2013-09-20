defmodule QueryTest do
  use ExUnit.Case, async: true

  setup do
    { :ok, pid } = Postgrex.connect("localhost", "postgres", "postgres", "postgrex_test")
    { :ok, [pid: pid] }
  end

  teardown context do
    :ok = Postgrex.disconnect(context[:pid])
  end

  test "decode basic types", context do
    assert { :ok, [{ nil }] } = Postgrex.query(context[:pid], "SELECT NULL")
    assert { :ok, [{ true, false }] } = Postgrex.query(context[:pid], "SELECT true, false")
    assert { :ok, [{ ?e }] } = Postgrex.query(context[:pid], "SELECT 'e'::char")
    assert { :ok, [{ 42 }] } = Postgrex.query(context[:pid], "SELECT 42")
    assert { :ok, [{ 42.0 }] } = Postgrex.query(context[:pid], "SELECT 42::float")
  end

  test "decode arrays", context do
    assert { :ok, [{ [] }] } = Postgrex.query(context[:pid], "SELECT ARRAY[]::integer[]")
    assert { :ok, [{ [1] }] } = Postgrex.query(context[:pid], "SELECT ARRAY[1]")
    assert { :ok, [{ [1,2] }] } = Postgrex.query(context[:pid], "SELECT ARRAY[1,2]")
    assert { :ok, [{ [[0],[1]] }] } = Postgrex.query(context[:pid], "SELECT ARRAY[[0],[1]]")
    assert { :ok, [{ [[0]] }] } = Postgrex.query(context[:pid], "SELECT ARRAY[ARRAY[0]]")
  end
end
