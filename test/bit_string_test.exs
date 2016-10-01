defmodule BitStringTest do
  use ExUnit.Case, async: true
  import Postgrex.TestHelper
  alias Postgrex, as: P

  setup do
    opts = [database: "postgrex_test", backoff_type: :stop,
            extensions: [{Postgrex.Extensions.BitString, []}]]
    {:ok, pid} = P.start_link(opts)
    {:ok, [pid: pid]}
  end

  test "decode bit string", context do
    assert [[<<1::1,0::1,1::1>>]] == query("SELECT bit '101'", [])
    assert [[<<1::1,1::1,0::1>>]] == query("SELECT bit '110'", [])
    assert [[<<1::1,0::1,1::1,1::1,0::1>>]] == query("SELECT bit '10110'", [])
    assert [[<<1::1,0::1,1::1,0::1,0::1>>]] ==
      query("SELECT bit '101' :: bit(5)", [])
    assert [[<<1::1,0::1,0::1,0::1,0::1,0::1,0::1,0::1,
             1::1,0::1,1::1>>]] ==
      query("SELECT bit '10000000101'", [])
    assert [[<<0::1,0::1,0::1,0::1,0::1,0::1,0::1,0::1,
             0::1,0::1,0::1,0::1,0::1,0::1,0::1,0::1,
             1::1,0::1,1::1>>]] ==
      query("SELECT bit '0000000000000000101'", [])
    assert [[<<1::1,0::1,0::1,0::1,0::1,0::1,0::1,0::1,
             0::1,0::1,0::1,0::1,0::1,0::1,0::1,0::1,
             1::1,0::1,1::1>>]] ==
      query("SELECT bit '1000000000000000101'", [])
    assert [[<<1::1,0::1,0::1,0::1,0::1,0::1,0::1,1::1,
             1::1,0::1,0::1,0::1,0::1,0::1,0::1,0::1,
             1::1,0::1,1::1>>]] ==
      query("SELECT bit '1000000110000000101'", [])
  end

  test "encode bit string", context do
    assert [["110"]] == query("SELECT $1::bit(3)::text", [<<1::1, 1::1, 0::1>>])
    assert [["101"]] == query("SELECT $1::bit(3)::text", [<<1::1, 0::1, 1::1>>])
    assert [["11010"]] ==
      query("SELECT $1::bit(5)::text", [<<1::1, 1::1, 0::1, 1::1>>])
    assert [["10000000101"]] ==
      query("SELECT $1::bit(11)::text",
        [<<1::1,0::1,0::1,0::1,0::1,0::1,0::1,0::1,
         1::1,0::1,1::1>>])
    assert [["0000000000000000101"]] ==
      query("SELECT $1::bit(19)::text",
        [<<0::1,0::1,0::1,0::1,0::1,0::1,0::1,0::1,
         0::1,0::1,0::1,0::1,0::1,0::1,0::1,0::1,
         1::1,0::1,1::1>>])
    assert [["1000000000000000101"]] ==
      query("SELECT $1::bit(19)::text",
        [<<1::1,0::1,0::1,0::1,0::1,0::1,0::1,0::1,
         0::1,0::1,0::1,0::1,0::1,0::1,0::1,0::1,
         1::1,0::1,1::1>>])
    assert [["1000000110000000101"]] ==
      query("SELECT $1::bit(19)::text",
        [<<1::1,0::1,0::1,0::1,0::1,0::1,0::1,1::1,
         1::1,0::1,0::1,0::1,0::1,0::1,0::1,0::1,
         1::1,0::1,1::1>>])
  end

  test "persist bit string", context do
    a = << 1::1, 0::1, 1::1, 1::1, 0::1 >>
    astring = "10110"
    b = << 1::1, 1::1, 0::1, 0::1, 0::1, 1::1, 1::1, 0::1, 0::1, 0::1 >>
    bstring = "1100011000"
    assert :ok = query("INSERT INTO bitstring_test (a, b) VALUES ($1, $2)",
      [a, b])
    assert [[astring, bstring]] ==
      query("SELECT a :: text, b :: text FROM bitstring_test", [])
    assert [[a, b]] == query("SELECT a, b FROM bitstring_test", [])
  end
end
