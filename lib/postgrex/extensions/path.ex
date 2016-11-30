defmodule Postgrex.Extensions.Path do
  @moduledoc false
  import Postgrex.BinaryUtils
  use Postgrex.BinaryExtension, send: "path_send"

  def encode(
    type_info,
    path = %Postgrex.Path{points: points, open: open},
    _, _)
  when is_list(points) and is_boolean(open) do
    [
      open_to_byte(open),
      <<length(path.points)::int32>>,
      Enum.map(path.points,
        fn(point) -> encode_point(type_info, point) end)
    ]
  end
  def encode(type_info, value, _, _) do
    raise ArgumentError,
      Postgrex.Utils.encode_msg(type_info, value, Postgrex.Path)
  end

  def decode(_, data, _, _) do
    <<open::int8, length::int32, rest::bits>> = data
    decode_point(length, %Postgrex.Path{points: [], open: open > 0}, rest)
  end

  defp encode_point(_, %Postgrex.Point{x: x, y: y}),
    do: <<x::float64, y::float64>>
  defp encode_point(type_info, point) do
    raise ArgumentError,
      Postgrex.Utils.encode_msg(type_info, point, Postgrex.Path)
  end

  defp decode_point(0, path, _), do: path
  defp decode_point(n, path, <<x::float64, y::float64, rest::bits>>) do
    decode_point(
      n - 1,
      %{path| points: path.points ++ [%Postgrex.Point{x: x, y: y}]},
      rest)
  end

  defp open_to_byte(true), do: << 1 :: int8 >>
  defp open_to_byte(false), do: << 0 :: int8 >>
end
