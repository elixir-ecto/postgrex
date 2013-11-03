defmodule CustomCoders do
  use ExUnit.Case, async: true
  import Postgrex.TestHelper
  alias Postgrex.Connection, as: P

  defp encoder(:int4, _type, _oid, default, value) do
    default.(value + 1)
  end

  defp encoder(_sender, _type, _oid, default, value) do
    default.(value)
  end

  defp decoder(:int4, _type, _oid, _format, default, bin) do
    default.(bin) + 10
  end

  defp decoder(_sender, _type, _oid, _format, default, bin) do
    default.(bin)
  end

  setup do
    opts = [ hostname: "localhost", username: "postgres",
             password: "postgres", database: "postgrex_test",
             encoder: &encoder/5, decoder: &decoder/6]
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
