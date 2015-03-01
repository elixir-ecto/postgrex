defmodule HstoreTest do
  use ExUnit.Case, async: true
  import Postgrex.TestHelper
  alias Postgrex.Connection, as: P
  alias Postgrex.Extensions.Hstore
  alias Postgrex.Extensions.Hstore.Decoder
  alias Postgrex.Extensions.Hstore.Encoder

  setup do
    {:ok, pid} = P.start_link(
      database: "postgrex_test",
      extensions: [{Hstore, {}}])
    {:ok, [pid: pid]}
  end

  test "decode hstore", context do
    assert [{%{}}] = query(~s{SELECT ''::hstore}, [])
    assert [{%{"Bubbles" => 7, "Name" => "Frank"}}] = query(~s{SELECT '"Name" => "Frank", "Bubbles" => "7"'::hstore}, [])
    assert [{%{"non_existant" => nil, "present" => "&accounted_for"}}] = query(~s{SELECT '"non_existant" => NULL, "present" => "&accounted_for"'::hstore}, [])
    assert [{%{"spaces in the key" => "are easy!"}}] = query(~s{SELECT '"spaces in the key" => "are easy!"'::hstore}, [])
  end

  test "it can decode into a map" do
    result_map = Decoder.decode ~s("name"=>"Frank","bubbles"=>"seven")
    assert result_map == %{"name" => "Frank", "bubbles" => "seven"}
  end

  test "it can decode special values" do
    result_map = Decoder.decode ~s("limit"=>NULL,"chillin"=>"true","fratty"=>"false")
    assert result_map == %{
      "limit" => nil,
      "chillin"=> true,
      "fratty"=> false
    }
  end

  test "it can decode integers" do
    result_map = Decoder.decode ~s("bubbles"=>"7")
    assert result_map == %{"bubbles" => 7}
  end

  test "it can decode floats" do
    result_map = Decoder.decode ~s("bubbles"=>"7.5")
    assert result_map == %{"bubbles" => 7.5}
  end

  test "it can encode a map" do
    input_map = Encoder.encode %{"name" => "Frank", "bubbles" => "seven"}
    assert input_map == ~s("bubbles"=>"seven","name"=>"Frank")
  end

  test "it can encode with nils and booleans" do
    input_map = Encoder.encode(%{
      "limit" => nil,
      "chillin"=> true,
      "fratty"=> false
    })
    assert input_map == ~s("chillin"=>"true","fratty"=>"false","limit"=>NULL)
  end

  test "it can encode integers and floats" do
    input_map = Encoder.encode %{
      "bubbles" => 7,
      "fragmentation grenades" => 3.5
    }
    assert input_map == ~s("bubbles"=>"7","fragmentation grenades"=>"3.5")
  end

end
