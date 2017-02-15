defmodule PgpassTest do
  use ExUnit.Case
  alias Postgrex.Pgpass, as: P

  test "obtains credentials via .pgpass" do
    opts = [ hostname: "localhost", database: "somedb", port: 5432 ]
    assert "foo" == P.username(opts)
    assert "bar" == P.password(opts)
  end

  test "matches wildcards" do
    opts = [ hostname: "localhost", database: "somedb", port: 5555 ]
    assert "baz" == P.username(opts)
    assert "bat" == P.password(opts)
  end

  test "matches the username when preset" do
    opts = [ hostname: "localhost", database: "somedb", port: 5432, username: "root" ]
    assert "secret" == P.password(opts)
  end

  test "returns nil if there is no match" do
    opts = [ hostname: "doesnt", database: "exist", port: 5432 ]
    assert nil == P.username(opts)
    assert nil == P.password(opts)
  end

end
