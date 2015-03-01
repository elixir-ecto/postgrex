defmodule Postgrex.Extensions.Hstore.Encoder do

  def encode(source) when is_map(source) do
    keys_and_values = Enum.reduce source, "", fn ({key, value}, acc) ->
        acc <> encode_key(key) <> encode_value(value)
    end
    <<Map.size(source)::size(32)>> <> keys_and_values
  end

  defp encode_key(key) when is_nil(key) do
    raise ArgumentError, message: "Hstore keys cannot be nil!"
  end

  defp encode_key(key) do
    encode_value key
  end

  defp encode_value(nil) do
    <<255, 255, 255, 255>>
  end

  defp encode_value(true),
    do: encode_value("true")

  defp encode_value(false),
    do: encode_value("false")

  defp encode_value(value) when is_integer(value) do
    encode_value to_string(value)
  end

  defp encode_value(value) when is_float(value) do
    encode_value to_string(value)
  end

  defp encode_value(value) do
    string_value = to_string(value)
    string_length = String.length(string_value)
    <<string_length::size(32)>> <> string_value
  end
end
