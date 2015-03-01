defmodule Postgrex.Extensions.Hstore.Encoder do
  # Punctuation
  @single_quote "'"
  @e_single_quote "E'"
  @double_quote ~s(")
  @hashrocket "=>"
  @comma ","
  @slash "\\"

  def encode(source) when is_map(source) do
    mapped = Enum.map source, fn ({key, value}) ->
        encode_key(key) <>
        @hashrocket <>
        encode_value(value)
    end
    Enum.join(mapped, @comma)
  end

  defp encode_key(key) when is_nil(key) do
    raise ArgumentError, message: "Hstore keys cannot be nil!"
  end

  defp encode_key(key) do
    double_quote(key)
  end

  defp encode_value(nil) do
    "NULL"
  end

  defp encode_value(value) when is_integer(value) do
    double_quote(to_string(value))
  end

  defp encode_value(value) when is_float(value) do
    double_quote(to_string(value))
  end

  defp encode_value(value) do
    double_quote(to_string(value))
  end

  defp double_quote(str) do
    @double_quote <> str <> @double_quote
  end
end
