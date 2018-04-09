defmodule ErrorTest do
  use ExUnit.Case, async: true

  alias Postgrex, as: P

  @tag min_pg_version: "9.3"
  test "encodes code, detail, table and constraint" do
    opts = [database: "postgrex_test", backoff_type: :stop]
    {:ok, pid} = P.start_link(opts)

    {:error, error} = P.query(pid, "insert into uniques values (1), (1);", [])
    message = Exception.message(error)
    assert message =~ "duplicate key value violates unique constraint"
    assert message =~ "table: uniques"
    assert message =~ "constraint: uniques_a_key"
    assert message =~ "ERROR 23505"
  end

  @tag min_pg_version: "9.3"
  test "encodes custom hint" do
    opts = [database: "postgrex_test", backoff_type: :stop]
    {:ok, pid} = P.start_link(opts)

    query = """
    DO language plpgsql $$ BEGIN
      RAISE EXCEPTION 'oops' USING HINT = 'custom hint';
    END;
    $$;
    """

    {:error, error} = P.query(pid, query, [])
    message = Exception.message(error)
    assert message =~ "oops"
    assert message =~ "hint: custom hint"
  end
end
