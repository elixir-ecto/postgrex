defmodule QueryTest do
  use ExUnit.Case, async: true
  import Postgrex.TestHelper
  alias Postgrex.Connection, as: P

  setup do
    opts = [ hostname: "localhost", username: "postgres",
             password: "postgres", database: "postgrex_test" ]
    { :ok, pid } = P.start_link(opts)
    { :ok, [pid: pid] }
  end

  teardown context do
    :ok = P.stop(context[:pid])
  end

  test "simple query", context do
    assert { :ok, res } = P.simple_query(context[:pid], "SELECT 5; SELECT 123 AS a, 456 AS b; SELECT 5;")
    assert Postgrex.Result[] = res
    assert res.command == :select
    assert res.columns == ["a", "b"]
    assert res.num_rows == 1
  end
end
