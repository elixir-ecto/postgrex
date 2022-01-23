defmodule Postgrex.Extensions.TSVector do
  @moduledoc false

  import Postgrex.BinaryUtils, warn: false
  use Postgrex.BinaryExtension, send: "tsvectorsend"

  def encode(_) do
    quote location: :keep do
      values when is_list(values) ->
        encoded_tsvectors = unquote(__MODULE__).encode_tsvector(values)
        <<byte_size(encoded_tsvectors)::int32, encoded_tsvectors::binary>>

      other ->
        raise DBConnection.EncodeError, Postgrex.Utils.encode_msg(other, "a list of tsvectors")
    end
  end

  def decode(_) do
    quote do
      <<len::int32, value::binary-size(len)>> ->
        <<nb_lexemes::int32, words::binary>> = value
        unquote(__MODULE__).decode_tsvector_values(words)
    end
  end

  ## Helpers

  def encode_tsvector(values) do
    <<length(values)::int32, encode_lexemes(values)::binary>>
  end

  defp encode_lexemes(values) do
    values |> Enum.map(fn x -> encode_positions(x) end) |> IO.iodata_to_binary()
  end

  defp encode_positions(%Postgrex.Lexeme{word: word, positions: positions}) do
    positions =
      Enum.map(positions, fn {position, weight} ->
        <<encode_weight_binary(weight)::2, position::14>>
      end)

    [word, 0, <<length(positions)::16>> | positions]
  end

  def decode_tsvector_values("") do
    []
  end

  def decode_tsvector_values(words) do
    [word, <<positions_count::16, rest::binary>>] = :binary.split(words, <<0>>)
    positions_bytes = positions_count * 2
    <<positions::binary-size(positions_bytes), remaining_data::binary>> = rest

    positions =
      for <<weight::2, position::14 <- positions>>, do: {position, decode_weight(weight)}

    [%Postgrex.Lexeme{word: word, positions: positions} | decode_tsvector_values(remaining_data)]
  end

  defp encode_weight_binary(:A) do
    3
  end

  defp encode_weight_binary(:B) do
    2
  end

  defp encode_weight_binary(:C) do
    1
  end

  defp encode_weight_binary(nil) do
    0
  end

  defp decode_weight(0), do: nil
  defp decode_weight(1), do: :C
  defp decode_weight(2), do: :B
  defp decode_weight(3), do: :A
end
