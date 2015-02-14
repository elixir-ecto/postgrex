defmodule CustomExtensions do
  use ExUnit.Case, async: true
  import Postgrex.TestHelper
  alias Postgrex.Connection, as: P
  alias Postgrex.TypeInfo

  defmodule BinaryExtension do
    @behaviour Postgrex.Extension

    def init(%{}, {}),
      do: []

    def matching([]),
      do: [send: "int4send"]

    def format([]),
      do: :binary

    def encode(%TypeInfo{send: "int4send", oid: oid}, value, types, []),
      do: Postgrex.Types.encode(Postgrex.Extensions.Binary, oid, value + 1, types)

    def decode(%TypeInfo{send: "int4send", oid: oid}, binary, types, []),
      do: Postgrex.Types.decode(Postgrex.Extensions.Binary, oid, binary, types) + 1
  end

  defmodule TextExtension do
    @behaviour Postgrex.Extension

    def init(%{}, []),
      do: {}

    def matching({}),
      do: [send: "float8send", send: "oidsend"]

    def format({}),
      do: :text

    def encode(_info, value, _types, {}),
      do: value

    def decode(_info, binary, _types, {}),
      do: binary
  end

  setup do
    {:ok, pid} = P.start_link(
      database: "postgrex_test",
      extensions: [{BinaryExtension, {}}, {TextExtension, []}])
    {:ok, [pid: pid]}
  end

  test "encode and decode", context do
    assert [{44}] = query("SELECT $1::int4", [42])
    assert [{[44]}] = query("SELECT $1::int4[]", [[42]])
  end

  test "encode and decode unknown type", context do
    assert [{"23"}] =
           query("SELECT $1::oid", ["23"])
  end

  test "dont decode text format", context do
    assert [{"123.45"}] = query("SELECT 123.45::float8", [])
  end
end
