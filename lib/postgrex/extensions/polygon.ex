defmodule Postgrex.Extensions.Polygon do
  @moduledoc false
  import Postgrex.BinaryUtils
  use Postgrex.BinaryExtension, send: "poly_send"

  def encode(type_info, polygon = %Postgrex.Polygon{vertices: vertices}, _, _)
  when is_list(vertices) do
    [
      <<length(polygon.vertices)::int32>>,
      Enum.map(polygon.vertices,
        fn(point) -> encode_point(type_info, point) end)
    ]
  end
  def encode(type_info, value, _, _) do
    raise ArgumentError,
      Postgrex.Utils.encode_msg(type_info, value, Postgrex.Polygon)
  end

  def decode(_, data, _, _) do
    <<length::int32, rest::bits>> = data
    decode_point(length, %Postgrex.Polygon{vertices: []}, rest)
  end

  defp encode_point(_, %Postgrex.Point{x: x, y: y}),
    do: <<x::float64, y::float64>>
  defp encode_point(type_info, point) do
    raise ArgumentError,
      Postgrex.Utils.encode_msg(type_info, point, Postgrex.Polygon)
  end

  defp decode_point(0, polygon, _), do: polygon
  defp decode_point(n, polygon, <<x::float64, y::float64, rest::bits>>) do
    decode_point(
      n - 1,
      %{polygon| vertices: polygon.vertices ++ [%Postgrex.Point{x: x, y: y}]},
      rest)
  end
end
