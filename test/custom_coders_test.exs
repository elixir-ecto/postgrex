defmodule CustomCoders do
  use ExUnit.Case, async: true
  import Postgrex.TestHelper
  alias Postgrex.Connection, as: P
  alias Postgrex.TypeInfo

  defp encoder(TypeInfo[sender: :int4], default, value) do
    default.(value + 1)
  end

  defp encoder(_info, default, value) do
    default.(value)
  end

  defp decoder(TypeInfo[sender: :int4], _format, default, bin) do
    default.(bin) + 10
  end

  defp decoder(_info, _format, default, bin) do
    default.(bin)
  end

  setup do
    opts = [ hostname: "localhost", username: "postgres",
             password: "postgres", database: "postgrex_test",
             encoder: &encoder/3, decoder: &decoder/4]
    { :ok, pid } = P.start_link(opts)
    { :ok, [pid: pid] }
  end

  teardown context do
    :ok = P.stop(context[:pid])
  end

  test "encode and decode", context do
    assert [{53}] = query("SELECT $1::int4", [42])
    assert [{[53]}] = query("SELECT $1::int4[]", [[42]])
  end
end
