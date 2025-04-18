defmodule Postgrex.Extensions.Vector do
  @moduledoc """
  Extension for the PostgreSQL vector type from pgvector extension.
  This extension handles conversion between Elixir lists of floats and
  PostgreSQL's vector type.
  """
  import Postgrex.BinaryUtils, warn: false
  use Postgrex.BinaryExtension, send: "vector_send"

  def init(_), do: nil

  def encode(_) do
    quote location: :keep do
      list when is_list(list) and length(list) > 0 ->
        dim = length(list)
        floats = for num <- list do
          cond do
            is_float(num) -> num
            is_integer(num) -> num * 1.0
            true -> raise ArgumentError, "vector values must be floats or integers, got: #{inspect(num)}"
          end
        end
        header = <<dim::uint16(), 0::uint16()>>
        float_binaries = for f <- floats, do: <<f::float-32>>
        payload = [header | float_binaries]
        [<<IO.iodata_length(payload)::int32()>> | payload]

      [] ->
        raise ArgumentError, "vector must have at least 1 dimension"

      "[" <> _ = vector_string when is_binary(vector_string) ->
        float_list =
          vector_string
          |> String.trim_leading("[")
          |> String.trim_trailing("]")
          |> String.split(",")
          |> Enum.map(&String.trim/1)
          |> Enum.map(fn v ->
            case Float.parse(v) do
              {f, _} -> f
              :error -> raise ArgumentError, "invalid vector value: #{inspect(v)}"
            end
          end)
        dim = length(float_list)
        header = <<dim::uint16(), 0::uint16()>>
        float_binaries = for f <- float_list, do: <<f::float-32>>
        payload = [header | float_binaries]
        [<<IO.iodata_length(payload)::int32()>> | payload]

      other ->
        raise ArgumentError,
              "expected vector to be a list of numbers or a string like '[1,2,3]', got: #{inspect(other)}"
    end
  end

  def decode(_) do
    quote location: :keep do
      <<len::int32(), data::binary-size(len)>> ->
        <<dim::uint16(), _unused::uint16(), float_data::binary>> = data
        {result, _} =
          Enum.reduce(1..dim, {[], float_data}, fn _, {acc, rest} ->
            <<val::float-32, new_rest::binary>> = rest
            {[val | acc], new_rest}
          end)
        Enum.reverse(result)
    end
  end
end
