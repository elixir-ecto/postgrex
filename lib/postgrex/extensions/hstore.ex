defmodule Postgrex.Extensions.HStore do
  @moduledoc false
  import Postgrex.BinaryUtils, warn: false
  use Postgrex.BinaryExtension, type: "hstore"

  def init(opts), do: Keyword.fetch!(opts, :decode_binary)

  def encode(_) do
    quote location: :keep do
      %{} = map ->
        data = unquote(__MODULE__).encode_hstore(map)
        [<<IO.iodata_length(data) :: int32>> | data]
      other ->
        raise ArgumentError, Postgrex.Utils.encode_msg(other, "a map")
    end
  end

  def decode(mode) do
    quote do
      <<len :: int32, data :: binary-size(len)>> ->
        unquote(__MODULE__).decode_hstore(data, unquote(mode))
    end
  end

  ## Helpers

  def encode_hstore(hstore_map) do
    keys_and_values = Enum.reduce hstore_map, "", fn ({key, value}, acc) ->
        [acc, encode_hstore_key(key), encode_hstore_value(value)]
    end
    [<<Map.size(hstore_map)::int32>> | keys_and_values]
  end

  defp encode_hstore_key(key) when is_binary(key) do
    encode_hstore_value key
  end

  defp encode_hstore_key(key) when is_nil(key) do
    raise ArgumentError, "hstore keys cannot be nil!"
  end

  defp encode_hstore_value(nil) do
    <<-1::int32>>
  end

  defp encode_hstore_value(value) when is_binary(value) do
    value_byte_size = byte_size(value)
    <<value_byte_size::int32>> <> value
  end

  def decode_hstore(<<_length::int32, pairs::binary>>, :reference) do
    decode_hstore_ref(pairs, %{})
  end
  def decode_hstore(<<_length::int32, pairs::binary>>, :copy) do
    decode_hstore_copy(pairs, %{})
  end

  defp decode_hstore_ref(<<>>, acc) do
    acc
  end

  # in the case of a NULL value, there won't be a length
  defp decode_hstore_ref(<<key_length::int32, key::binary(key_length),
                                 -1::int32, rest::binary>>, acc) do
    decode_hstore_ref(rest, Map.put(acc, key, nil))
  end

  defp decode_hstore_ref(<<key_length::int32, key::binary(key_length),
                        value_length::int32, value::binary(value_length), rest::binary>>, acc) do
    decode_hstore_ref(rest, Map.put(acc, key, value))
  end

  defp decode_hstore_copy(<<>>, acc) do
    acc
  end

  # in the case of a NULL value, there won't be a length
  defp decode_hstore_copy(<<key_length::int32, key::binary(key_length),
                                 -1::int32, rest::binary>>, acc) do
    decode_hstore_copy(rest, Map.put(acc, :binary.copy(key), nil))
  end

  defp decode_hstore_copy(<<key_length::int32, key::binary(key_length),
                        value_length::int32, value::binary(value_length), rest::binary>>, acc) do
    decode_hstore_copy(rest, Map.put(acc, :binary.copy(key), :binary.copy(value)))
  end
end
