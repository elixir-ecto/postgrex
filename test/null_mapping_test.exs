defmodule NullMappingTest do
  use ExUnit.Case, async: true
  import Postgrex.TestHelper
  alias Postgrex, as: P

  setup do
    opts = [ database: "postgrex_test",
             backoff_type: :stop,
             null: :custom ]
    {:ok, pid} = P.start_link(opts)
    {:ok, [pid: pid]}
  end

  test "decode null with correct mapping", context do
    assert [[:custom]] = query("SELECT NULL", [])
    assert [[true, false, :custom]] = query("SELECT true, false, NULL", [])
    assert [[true, :custom, false]] = query("SELECT true, NULL, false", [])
    assert [[:custom, true, false]] = query("SELECT NULL, true, false", [])
    assert [[[:custom, true, false]]] = query("SELECT ARRAY[NULL, true, false]", [])
    assert [[{:custom, true, false}]] = query("SELECT ROW(NULL, true, false)", [])
    assert [[%Postgrex.Range{lower: :custom, upper: 1}]] = query("SELECT int4range(NULL, 1)", [])
  end

  test "encode null with correct mapping", context do
    assert [[:custom, :custom]] = query("SELECT $1::text, $2::int", [:custom, :custom])
    assert [[true, false, :custom]] = query("SELECT $1::bool, $2::bool, $3::bool", [true, false, :custom])
    assert [[true, :custom, false]] = query("SELECT $1::bool, $2::bool, $3::bool", [true, :custom, false])
    assert [[:custom, true, false]] = query("SELECT $1::bool, $2::bool, $3::bool", [:custom, true, false])
    assert [["{NULL,t,f}"]] = query("SELECT ($1::bool[])::text", [[:custom, true, false]])
    assert [["[1,)"]] = query("SELECT ($1::int4range)::text", [%Postgrex.Range{lower: 1, upper: :custom}])
  end

  test "prepare and execute query with connection mapping", context do
    assert (%Postgrex.Query{} = query) = prepare("null", "SELECT $1::text")
    assert [[:custom]] = execute(query, [:custom])
  end
end
