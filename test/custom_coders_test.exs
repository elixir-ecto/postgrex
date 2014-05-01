defmodule CustomCoders do
  use ExUnit.Case, async: true
  import Postgrex.TestHelper
  alias Postgrex.Connection, as: P
  alias Postgrex.TypeInfo

  defp encoder(TypeInfo[sender: "int4"], default, value) do
    default.(value + 1)
  end

  defp encoder(TypeInfo[], default, value) do
    default.(value)
  end

  defp decoder(TypeInfo[sender: "int4"], _format, default, bin) do
    default.(bin) + 10
  end

  defp decoder(TypeInfo[], _format, default, bin) do
    default.(bin)
  end

  defp formatter(TypeInfo[sender: "float8"]), do: :text
  defp formatter(TypeInfo[]), do: nil

  setup do
    opts = [ database: "postgrex_test",
             encoder: &encoder/3, decoder: &decoder/4, formatter: &formatter/1]
    {:ok, pid} = P.start_link(opts)
    {:ok, [pid: pid]}
  end

  teardown context do
    :ok = P.stop(context[:pid])
  end

  test "encode and decode", context do
    assert [{53}] = query("SELECT $1::int4", [42])
    assert [{[53]}] = query("SELECT $1::int4[]", [[42]])
  end

  test "encode and decode unknown type", context do
    assert [{"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"}] =
           query("SELECT $1::uuid", ["a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"])
  end

  test "dont decode text format", context do
    assert [{"123.45"}] = query("SELECT 123.45::float8")
  end
end
