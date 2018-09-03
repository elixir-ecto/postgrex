defmodule ErrorTest do
  use ExUnit.Case, async: true

  alias Postgrex, as: P

  setup do
    opts = [database: "postgrex_test", backoff_type: :stop]
    {:ok, pid} = P.start_link(opts)
    {:ok, pid: pid}
  end

  @tag min_pg_version: "9.3"
  test "encodes code, detail, table and constraint", config do
    {:error, error} = P.query(config.pid, "insert into uniques values (1), (1);", [])
    message = Exception.message(error)
    assert message =~ "duplicate key value violates unique constraint"
    assert message =~ "table: uniques"
    assert message =~ "constraint: uniques_a_key"
    assert message =~ "ERROR 23505"
  end

  @tag min_pg_version: "9.3"
  test "encodes custom hint", config do
    query = """
    DO language plpgsql $$ BEGIN
      RAISE EXCEPTION 'oops' USING HINT = 'custom hint';
    END;
    $$;
    """

    {:error, error} = P.query(config.pid, query, [])
    message = Exception.message(error)
    assert message =~ "oops"
    assert message =~ "hint: custom hint"
  end

  @tag min_pg_version: "9.3"
  test "includes query on invalid syntax", config do
    {:error, error} = P.query(config.pid, "SELCT true;", [])
    message = Exception.message(error)
    assert message =~ "ERROR 42601 (syntax_error) syntax error at or near \"SELCT\""
    assert message =~ "query: SELCT true"
  end
end
