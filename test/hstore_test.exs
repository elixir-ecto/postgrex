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
    assert [{%{"spaces in the key" => "are easy!", "floats too" => 66.6}}] = query(~s{SELECT '"spaces in the key" => "are easy!", "floats too" => "66.6"'::hstore}, [])
    assert [{%{"this is true" => true, "though not this" => false}}] = query(~s{SELECT '"this is true" => true, "though not this" => false'::hstore}, [])
  end

  test "it can decode into a map" do
    result_map = Decoder.decode <<0, 0, 0, 2>> <>               # Number of key/value pairs
                                <<0, 0, 0, 4>> <> "name" <>     # Length of key + key
                                <<0, 0, 0, 5>> <> "Frank" <>    # Length of value + value
                                <<0, 0, 0, 7>> <> "bubbles" <>  # Length of key + key
                                <<0, 0, 0, 5>> <> "seven"       # Length of value + value
    assert result_map = %{"name" => "Frank", "bubbles" => "seven"}
  end

  test "it can decode special values" do
    result_map = Decoder.decode <<0, 0, 0, 3>> <>               # Number of key/value pairs
                                <<0, 0, 0, 5>> <> "limit" <>    # Length of key + key
                                <<255, 255, 255, 255>> <>       # NULL
                                <<0, 0, 0, 7>> <> "chillin" <>  # Length of key + key
                                <<0, 0, 0, 4>> <> "true" <>     # Length of value + value
                                <<0, 0, 0, 6>> <> "fratty" <>   # Length of key + key
                                <<0, 0, 0, 5>> <> "false"       # Length of value + value
    assert result_map == %{
      "limit" => nil,
      "chillin"=> true,
      "fratty"=> false
    }
  end

  test "it can decode integers" do
    result_map = Decoder.decode <<0, 0, 0, 2>> <>               # Number of key/value pairs
                                <<0, 0, 0, 7>> <> "bubbles" <>  # Length of key + key
                                <<0, 0, 0, 1>> <> "7"           # Length of value + value
    assert result_map == %{"bubbles" => 7}
  end

  test "it can decode floats" do
    result_map = Decoder.decode <<0, 0, 0, 2>> <>               # Number of key/value pairs
                                <<0, 0, 0, 7>> <> "bubbles" <>  # Length of key + key
                                <<0, 0, 0, 3>> <> "7.5"         # Length of value + value
    assert result_map == %{"bubbles" => 7.5}
  end

  test "it can encode a map" do
    input_map = Encoder.encode %{"name" => "Frank", "bubbles" => "seven"}
    assert input_map == <<0, 0, 0, 2>> <>               # Number of key/value pairs
                        <<0, 0, 0, 7>> <> "bubbles" <>  # Length of key + key
                        <<0, 0, 0, 5>> <> "seven" <>    # Length of value + value
                        <<0, 0, 0, 4>> <> "name" <>     # Length of key + key
                        <<0, 0, 0, 5>> <> "Frank"       # Length of value + value
  end

  test "it can encode with nils and booleans" do
    input_map = Encoder.encode(%{
      "limit" => nil,
      "chillin"=> true,
      "fratty"=> false
    })
    assert input_map == <<0, 0, 0, 3>> <>               # Number of key/value pairs
                        <<0, 0, 0, 7>> <> "chillin" <>  # Length of key + key
                        <<0, 0, 0, 4>> <> "true" <>     # Length of value + value
                        <<0, 0, 0, 6>> <> "fratty" <>   # Length of key + key
                        <<0, 0, 0, 5>> <> "false" <>    # Length of value + value
                        <<0, 0, 0, 5>> <> "limit" <>    # Length of key + key
                        <<255, 255, 255, 255>>          # NULL
  end

  test "it can encode integers and floats" do
    input_map = Encoder.encode %{
      "bubbles" => 7,
      "fragmentation grenades" => 3.5
    }
    assert input_map == <<0, 0, 0, 2>> <>                               # Number of key/value pairs
                        <<0, 0, 0, 7>> <> "bubbles" <>                  # Length of key + key
                        <<0, 0, 0, 1>> <> "7" <>                        # Length of value + value
                        <<0, 0, 0, 22>> <> "fragmentation grenades" <>  # Length of key + key
                        <<0, 0, 0, 3>> <> "3.5"                         # Length of value + value
  end

end
