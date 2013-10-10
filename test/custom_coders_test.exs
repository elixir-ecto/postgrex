defmodule CustomCoders do
  use ExUnit.Case, async: true
  import Postgrex.TestHelper
  alias Postgrex.Connection, as: P

  defmodule Coder do
    @behaviour Postgrex.Encoder
    @behaviour Postgrex.Decoder

    def pre_encode(_type, :int4, _oid, param) do
      param+10
    end

    def post_encode(_type, _sender, _oid, _param, encoded) do
      encoded
    end

    def decode(_type, :int4, _oid, _param, decoded) do
      decoded+10
    end

    def decode(_type, _sender, _oid, _param, decoded) do
      decoded
    end
  end

  setup do
    opts = [ hostname: "localhost", username: "postgres",
             password: "postgres", database: "postgrex_test",
             encoders: [Coder], decoders: [Coder]]
    { :ok, pid } = P.start_link(opts)
    { :ok, [pid: pid] }
  end

  teardown context do
    :ok = P.stop(context[:pid])
  end

  test "encode and decode", context do
    assert [{62}] = query("SELECT $1::int4", [42])
  end
end
