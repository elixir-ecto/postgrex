defmodule ErrorCodeTest do
  use ExUnit.Case, async: true
  import Postgrex.ErrorCode

  doctest Postgrex.ErrorCode

  test "code to name" do
    assert code_to_name("23505") == :unique_violation
    assert code_to_name("2F003") == :prohibited_sql_statement_attempted
    assert code_to_name("38003") == :prohibited_sql_statement_attempted
    assert code_to_name("nope") == nil
  end

  test "name to codes" do
    assert name_to_code(:unique_violation) == "23505"
    assert name_to_code(:prohibited_sql_statement_attempted) == "2F003"
    assert catch_error(name_to_code(:nope))
  end

  test "new codes from 9.5" do
    # https://github.com/postgres/postgres/commit/73206812cd97436cffd8f331dbb09d38a2728162
    assert code_to_name("39P03") == :event_trigger_protocol_violated
    assert name_to_code(:event_trigger_protocol_violated) == "39P03"

    # https://github.com/postgres/postgres/commit/a4847fc3ef139ba9a8ffebb6ffa06ee72078ffa2
    assert name_to_code(:assert_failure) == "P0004"
    assert code_to_name("P0004") == :assert_failure
  end
end
