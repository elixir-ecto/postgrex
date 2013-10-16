defmodule CustomCoders do
  use ExUnit.Case, async: true
  import Postgrex.TestHelper
  alias Postgrex.Connection, as: P

  defp encoder(_type, :int4, _oid, default, param) do
    default.(param + 1)
  end

  defp encoder(_type, _sender, _oid, default, param) do
    default.(param)
  end

  defp decoder(_type, :int4, _oid, default, bin) do
    default.(bin) + 10
  end

  defp decoder(_type, _sender, _oid, default, bin) do
    default.(bin)
  end

  setup do
    opts = [ hostname: "localhost", username: "postgres",
             password: "postgres", database: "postgrex_test",
             encoder: &encoder/5, decoder: &decoder/5]
    { :ok, pid } = P.start_link(opts)
    { :ok, [pid: pid] }
  end

  teardown context do
    :ok = P.stop(context[:pid])
  end

  test "encode and decode", context do
    assert [{53}] = query("SELECT $1::int4", [42])
  end
end
