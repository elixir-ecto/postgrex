defmodule TypeModuleTest do
  use ExUnit.Case, async: true
  import Postgrex.TestHelper
  alias Postgrex, as: P

  @types TestTypes

  setup_all do
    on_exit(fn ->
      :code.delete(@types)
      :code.purge(@types)
    end)

    opts = [decode_binary: :reference, null: :custom, json: :fake_json]
    Postgrex.TypeModule.define(@types, [], opts)
    :ok
  end

  setup do
    opts = [database: "postgrex_test", backoff_type: :stop, types: @types]
    {:ok, pid} = P.start_link(opts)
    {:ok, [pid: pid]}
  end

  @tag min_pg_version: "9.0"
  test "hstore references binaries when decode_binary: :reference", context do
    # For OTP 20+ refc binaries up to 64 bytes might be copied during a GC
    text = String.duplicate("hello world", 6)
    assert [[bin]] = query("SELECT $1::text", [text])
    assert :binary.referenced_byte_size(bin) > byte_size(text)

    assert [[%{"hello" => value}]] = query("SELECT $1::hstore", [%{"hello" => text}])
    assert :binary.referenced_byte_size(value) > byte_size(text)
  end

  test "decode null with custom mapping", context do
    assert [[:custom]] = query("SELECT NULL", [])
    assert [[true, false, :custom]] = query("SELECT true, false, NULL", [])
    assert [[true, :custom, false]] = query("SELECT true, NULL, false", [])
    assert [[:custom, true, false]] = query("SELECT NULL, true, false", [])
    assert [[[:custom, true, false]]] = query("SELECT ARRAY[NULL, true, false]", [])
    assert [[{:custom, true, false}]] = query("SELECT ROW(NULL, true, false)", [])
  end

  test "encode null with custom mapping", context do
    assert [[:custom, :custom]] = query("SELECT $1::text, $2::int", [:custom, :custom])

    assert [[true, false, :custom]] =
             query("SELECT $1::bool, $2::bool, $3::bool", [true, false, :custom])

    assert [[true, :custom, false]] =
             query("SELECT $1::bool, $2::bool, $3::bool", [true, :custom, false])

    assert [[:custom, true, false]] =
             query("SELECT $1::bool, $2::bool, $3::bool", [:custom, true, false])

    assert [["{NULL,t,f}"]] = query("SELECT ($1::bool[])::text", [[:custom, true, false]])
  end

  test "prepare and execute query with connection mapping", context do
    assert (%Postgrex.Query{} = query) = prepare("null", "SELECT $1::text")
    assert [[:custom]] = execute(query, [:custom])
  end
end
