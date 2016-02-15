defmodule ErrorTest do
  use ExUnit.Case, async: true

  alias Postgrex, as: P

  test "connection works after failure in executing state" do
    opts = [database: "postgrex_test", backoff_type: :stop]
    {:ok, pid} = P.start_link(opts)

    {:error, error} = P.query(pid, "insert into uniques values (1), (1);", [])
    message = Exception.message(error)
    assert message =~ "duplicate key value violates unique constraint"
    assert message =~ "table: uniques"
    assert message =~ "constraint: uniques_a_key"
  end
end
