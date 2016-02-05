defmodule QueryLevelNullMappingTest do
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

  test "prepare and execute query with correct mapping", context do
    assert (%Postgrex.Query{} = query) = prepare("null", "SELECT $1::text", null: :custom)
    assert [[:custom]] = execute(query, [:custom])
  end

  test "prepared mapping not overriden by execute", context do
    assert (%Postgrex.Query{} = query) = prepare("null", "SELECT $1::text", null: :custom)
    assert [[:custom]] = execute(query, [:custom], null: nil)
    assert [[:custom]] = execute(query, [:custom], null: :undefined)
  end
end

defmodule ConnLevelNullMappingTest do
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
  end

  test "decode null with overriden connection mapping", context do
    assert [[nil]] = query("SELECT NULL", [], null: nil)
    assert [[:undefined]] = query("SELECT NULL", [], null: :undefined)
  end

  test "encode null with correct mapping", context do
    assert [[:custom, :custom]] = query("SELECT $1::text, $2::int", [:custom, :custom])
    assert [[true, false, :custom]] = query("SELECT $1::bool, $2::bool, $3::bool", [true, false, :custom])
    assert [[true, :custom, false]] = query("SELECT $1::bool, $2::bool, $3::bool", [true, :custom, false])
    assert [[:custom, true, false]] = query("SELECT $1::bool, $2::bool, $3::bool", [:custom, true, false])
  end
  
  test "encode null with overriden connection mapping", context do
    assert [[nil]] = query("SELECT $1::text", [nil], null: nil)
    assert [[:undefined]] = query("SELECT $1::text", [:undefined], null: :undefined)
  end

  test "prepare and execute query with connection mapping", context do
    assert (%Postgrex.Query{} = query) = prepare("null", "SELECT $1::text")
    assert [[:custom]] = execute(query, [:custom])
  end

  test "prepare and execute query with overriden connection mapping", context do
    assert (%Postgrex.Query{} = nil_query) = prepare("null", "SELECT $1::text", null: :nil)
    assert (%Postgrex.Query{} = undef_query) = prepare("null", "SELECT $1::text", null: :undefined)
    assert [[:nil]] = execute(nil_query, [:nil])
    assert [[:undefined]] = execute(undef_query, [:undefined])
  end
  
  test "prepared connection mapping not overriden by execute", context do
    assert (%Postgrex.Query{} = query) = prepare("null", "SELECT $1::text")
    assert [[:custom]] = execute(query, [:custom], null: nil)
    assert [[:custom]] = execute(query, [:custom], null: :undefined)
  end
end
