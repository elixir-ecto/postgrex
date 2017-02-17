defmodule PgpassTest do
  use ExUnit.Case
  alias Postgrex.Pgpass, as: P

  setup do
    with path <- Path.join(__DIR__, "support/pgpass"),
         :ok <- System.put_env("PGPASSFILE", path),
      do: File.chmod!(path, 0o0600)
  end

  test "obtains credentials via .pgpass" do
    opts = [hostname: "localhost", database: "somedb", port: 5432, username: "foo"]
    assert "bar" == P.password(opts)
  end

  test "matches wildcards" do
    opts = [hostname: "localhost", database: "somedb", port: 5555, username: "baz"]
    assert "bat" == P.password(opts)
  end

  test "matches the given username" do
    opts = [hostname: "localhost", database: "somedb", port: 5432, username: "root"]
    assert "secret" == P.password(opts)
  end

  test "returns nil if there is no match" do
    opts = [hostname: "doesnt", database: "exist", port: 5432]
    assert nil == P.password(opts)
  end

  test "raises if the passfile option is unreadable" do
    opts = [hostname: "doesnt", database: "exist", port: 5432, username: "foo", passfile: "invalid"]
    assert_raise RuntimeError, ~r/does not exist/, fn ->
      P.password(opts)
    end
  end

  test "raises if the passfile has incorrect permissions" do
    opts = [hostname: "doesnt", database: "exist", port: 5432, username: "foo", passfile: Path.join(__DIR__, "support/pgpass-wrong-permissions")]
    assert_raise RuntimeError, ~r/must have permissions 0600/, fn ->
      P.password(opts)
    end
  end

end
