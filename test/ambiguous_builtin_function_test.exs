defmodule AmbiguousBuiltinFunctionTest do
  use ExUnit.Case, async: false
  alias Postgrex, as: P

  setup do
    opts = [database: "postgrex_test", backoff_type: :stop, max_restarts: 0]
    {:ok, pid} = Postgrex.start_link(opts)

    P.query!(
      pid,
      "CREATE OR REPLACE FUNCTION public.record_send(integer) RETURNS bytea LANGUAGE sql AS $$ SELECT ''::bytea $$",
      []
    )

    P.query!(pid, "DROP TYPE IF EXISTS my_type CASCADE", [])
    P.query!(pid, "CREATE TYPE my_type AS (a int, b text)", [])

    {:ok, [pid: pid, options: opts]}
  end

  test "composite type loads when a user function duplicates built-in record_send", %{pid: pid} do
    assert {:ok, %Postgrex.Result{rows: [[[]]]}} =
             P.query(pid, "SELECT $1::my_type[]", [[]])
  end

  test "regproc raises when a user function duplicates built-in record_send", %{pid: pid} do
    assert_raise Postgrex.Error, ~r/ambiguous/, fn ->
      P.query!(pid, "SELECT 'record_send'::regproc", [])
    end
  end
end
