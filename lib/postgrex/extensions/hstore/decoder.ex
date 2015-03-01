defmodule Postgrex.Extensions.Hstore.Decoder do
  # Types
  @integer ~r/\A\d+\z/
  @float ~r/\A\d+\.\d+\z/
  @null ~r/\ANULL\z/

  @null_value <<255, 255, 255, 255>>
  # HStore with length of 0
  def decode(<<0,0,0,0>>) do
    %{}
  end

  def decode(<<length::integer-size(32), pairs::binary>> = payload) do
    decode_payload(%{}, pairs)
  end

  defp decode_payload(acc, <<>>) do
    acc
  end

  # in the case of a NULL value, there won't be a length
  defp decode_payload(acc, <<key_length::integer-size(32), key::size(key_length)-binary,
                             @null_value, rest::binary>>) do
    decode_payload(Dict.put(acc, parse_key(key), nil), rest)
  end

  defp decode_payload(acc, <<key_length::integer-size(32), key::size(key_length)-binary,
                        value_length::integer-size(32), value::size(value_length)-binary, rest::binary>>) do
    decode_payload(Dict.put(acc, parse_key(key), parse_value(value)), rest)
  end

  defp parse_key(key) do
    parse_types key
  end

  defp parse_value(value) do
    parse_constant(parse_types(value))
  end

  defp parse_constant("true") do
    true
  end

  defp parse_constant("false") do
    false
  end

  defp parse_constant(value) do
    value
  end

  defp parse_types(value) do
    cond do
      Regex.match?(@integer, value) ->
        String.to_integer(value)
      Regex.match?(@float, value) ->
        String.to_float(value)
      true ->
        value
    end
  end
end
