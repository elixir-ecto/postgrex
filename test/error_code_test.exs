defmodule ErrorCodeTest do
  use ExUnit.Case, async: true
  import Postgrex.ErrorCode

  doctest Postgrex.ErrorCode

  test "code to name" do
    assert code_to_name("23505") == :unique_violation
    assert code_to_name("2F003") == :prohibited_sql_statement_attempted
    assert code_to_name("38003") == :prohibited_sql_statement_attempted
    assert catch_error(code_to_name("nope"))
  end

  test "name to codes" do
    assert name_to_code(:unique_violation) == "23505"
    assert name_to_code(:prohibited_sql_statement_attempted) == "2F003"
    assert catch_error(name_to_code(:nope))
  end
end
