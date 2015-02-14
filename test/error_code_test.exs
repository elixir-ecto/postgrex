defmodule ErrorCodeTest do
  use ExUnit.Case, async: true
  alias Postgrex.ErrorCode, as: EC

  test "Postgrex.ErrorCode.code_to_name/1" do
    assert EC.code_to_name("23505") == :unique_violation
    assert EC.code_to_name("2F003") == :prohibited_sql_statement_attempted
    assert EC.code_to_name("38003") == :prohibited_sql_statement_attempted
    assert catch_error(EC.code_to_name("nope"))
  end

  test "Postgrex.ErrorCode.name_to_codes/1" do
    assert EC.name_to_codes(:unique_violation) == ["23505"]
    assert EC.name_to_codes(:prohibited_sql_statement_attempted) == ["2F003", "38003"]
    assert catch_error(EC.name_to_codes(:nope))
  end
end
