defmodule NullMappingTest do
  use ExUnit.Case, async: true
  import Postgrex.TestHelper
  alias Postgrex, as: P

  setup do
    opts = [ database: "postgrex_test",
             backoff_type: :stop ]
    {:ok, pid} = P.start_link(opts)
    {:ok, [pid: pid]}
  end

  test "decode null with correct mapping", context do
    assert [[:custom]] = query("SELECT NULL", [], null: :custom)
    assert [[true, false, :custom]] = query("SELECT true, false, NULL", [], null: :custom)
    assert [[true, :custom, false]] = query("SELECT true, NULL, false", [], null: :custom)
    assert [[:custom, true, false]] = query("SELECT NULL, true, false", [], null: :custom)
  end

  test "encode null with correct mapping", context do
    assert [[:custom, :custom]] = query("SELECT $1::text, $2::int", [:custom, :custom], null: :custom)
    assert [[true, false, :custom]] = query("SELECT $1::bool, $2::bool, $3::bool", [true, false, :custom], null: :custom)
    assert [[true, :custom, false]] = query("SELECT $1::bool, $2::bool, $3::bool", [true, :custom, false], null: :custom)
    assert [[:custom, true, false]] = query("SELECT $1::bool, $2::bool, $3::bool", [:custom, true, false], null: :custom)
  end
end
