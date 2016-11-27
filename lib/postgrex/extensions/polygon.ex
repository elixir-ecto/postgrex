defmodule Postgrex.Extensions.Polygon do
  @moduledoc false
  import Postgrex.BinaryUtils
  use Postgrex.BinaryExtension, send: "poly_send"

  def encode(type_info, points, _, _) when is_list(points) do
    [<<length(points)::int32>>,
     Enum.map(points, fn(point) -> encode_point(type_info, point) end)]
  end
  def encode(type_info, value, _, _) do
    raise ArgumentError,
      Postgrex.Utils.encode_msg(type_info, value, [Postgrex.Point])
  end

  def decode(_, data, _, _) do
    <<length::int32, rest::bits>> = data
    decode_point(length, [], rest)
  end

  defp encode_point(_, %Postgrex.Point{x: x, y: y}),
    do: <<x::float64, y::float64>>
  defp encode_point(type_info, point) do
    raise ArgumentError,
      Postgrex.Utils.encode_msg(type_info, point, [Postgrex.Point])
  end

  defp decode_point(0, points, _), do: points
  defp decode_point(n, points, <<x::float64, y::float64, rest::bits>>) do
    decode_point(n - 1, points ++ [%Postgrex.Point{x: x, y: y}], rest)
  end
end
