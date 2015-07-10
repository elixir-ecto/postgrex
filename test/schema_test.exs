defmodule SchemaTest do
  use ExUnit.Case, async: true
  import Postgrex.TestHelper
  alias Postgrex.Connection, as: P

  setup do
    opts = [ database: "postgrex_test_with_schemas" ]
    {:ok, pid} = P.start_link(opts)
    {:ok, [pid: pid]}
  end


  @tag min_pg_version: "9.1"
  test "encode hstore", context do
    assert [[%{"name" => "Frank", "bubbles" => "7", "limit" => nil, "chillin"=> "true", "fratty"=> "false", "atom" => "bomb"}]] =
           query ~s(SELECT $1::test.hstore), [%{"name" => "Frank", "bubbles" => "7", "limit" => nil, "chillin"=> "true", "fratty"=> "false", "atom" => "bomb"}]
  end

  @tag min_pg_version: "9.1"
  test "decode hstore inside a schema", context do
    assert [[%{}]] = query(~s{SELECT ''::test.hstore}, [])
    assert [[%{"Bubbles" => "7", "Name" => "Frank"}]] = query(~s{SELECT '"Name" => "Frank", "Bubbles" => "7"'::test.hstore}, [])
    assert [[%{"non_existant" => nil, "present" => "&accounted_for"}]] = query(~s{SELECT '"non_existant" => NULL, "present" => "&accounted_for"'::test.hstore}, [])
    assert [[%{"spaces in the key" => "are easy!", "floats too" => "66.6"}]] = query(~s{SELECT '"spaces in the key" => "are easy!", "floats too" => "66.6"'::test.hstore}, [])
    assert [[%{"this is true" => "true", "though not this" => "false"}]] = query(~s{SELECT '"this is true" => "true", "though not this" => "false"'::test.hstore}, [])
  end
end
