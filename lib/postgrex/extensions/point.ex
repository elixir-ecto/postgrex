defmodule Postgrex.Extensions.Point do
  @moduledoc false
  import Postgrex.BinaryUtils
  use Postgrex.BinaryExtension, send: "point_send"

  def encode(_, %Postgrex.Point{x: x, y: y}, _, _),
    do: <<x::float64, y::float64>>

  def encode(type_info, value, _, _) do
    raise ArgumentError,
      Postgrex.Utils.encode_msg(type_info, value, Postgrex.Point)
  end

  def decode(_, <<x::float64, y::float64>>, _, _),
    do: %Postgrex.Point{x: x, y: y}

  def inline(_type_info, _types, _opts) do
    {__MODULE__, inline_encode(), inline_decode()}
  end

  defp inline_encode() do
    quote location: :keep do
      %Postgrex.Point{x: x, y: y} ->
        <<16::int32, x::float64, y::float64>>
      other ->
        Postgrex.Utils.encode_msg(other, Postgrex.Point)
    end
  end

  defp inline_decode() do
    quote location: :keep do
      <<16::int32, x::float64, y::float64>> -> %Postgrex.Point{x: x, y: y}
    end
  end
end
