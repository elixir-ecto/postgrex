defmodule Postgrex.Extensions.Hstore.Decoder do
  # Types
  @integer ~r/\A\d+\z/
  @float ~r/\A\d+\.\d+\z/
  @null ~r/\ANULL\z/

  # Escaped Characters
  @escaped_char ~r/\\(.)/
  @escaped_single_quote "\\\'"
  @escaped_double_quote "\\\""
  @escaped_slash "\\\\"

  @double_quoted_string ~r/\A"(.+)"\z/

  # Literals
  @quoted_literal ~r/"[^"\\]*(?:\\.[^"\\]*)*"/
  @unquoted_literal ~r/[^\s=,][^\s=,\\]*(?:\\.[^\s=,\\]*|=[^,>])*/
  @literal ~r/(#{Regex.source(@quoted_literal)}|#{Regex.source(@unquoted_literal)})/

  # Bring it all together! This is a full hstore hash/map/dict
  @pair ~r/#{Regex.source(@literal)}\s*=>\s*#{Regex.source(@literal)}/

  def decode(bin) do
    Enum.reduce Regex.scan(@pair, bin), %{}, fn ([_raw, key, value], result_map) ->
      Dict.put(result_map, parse_key(key), parse_value(value))
    end
  end

  defp parse_key(key) do
    unescape un_double_quote key
  end

  defp parse_value(value) do
    if Regex.match?(@null, value) do
      nil
    else
      parse_constant(unescape(un_double_quote(value)))
    end
  end

  defp un_double_quote(value) do
    if Regex.match?(@double_quoted_string, value) do
      Regex.replace(@double_quoted_string, value, "\\1")
    else
      value
    end
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

  defp unescape(value) do
    parse_types Regex.replace(@escaped_char, value, "\\1")
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
