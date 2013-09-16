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
    assert { :ok, [{ nil }] } = Postgrex.query(context[:pid], "SELECT NULL")
    assert { :ok, [{ true, false }] } = Postgrex.query(context[:pid], "SELECT true, false")
    assert { :ok, [{ ?e }] } = Postgrex.query(context[:pid], "SELECT 'e'::char")
    assert { :ok, [{ 42 }] } = Postgrex.query(context[:pid], "SELECT 42")
    assert { :ok, [{ 42.0 }] } = Postgrex.query(context[:pid], "SELECT 42::float")
  end
end
