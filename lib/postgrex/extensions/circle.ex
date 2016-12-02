defmodule Postgrex.Extensions.Circle do
  @moduledoc false
  import Postgrex.BinaryUtils
  use Postgrex.BinaryExtension, send: "circle_send"

  def encode(
    _,
    %Postgrex.Circle{center: %Postgrex.Point{x: x, y: y}, radius: radius},
    _, _)
  when is_number(x) and is_number(y) and is_number(radius) and radius >= 0 do
    <<x::float64, y::float64, radius::float64>>
  end
  def encode(type_info, value, _, _) do
    raise ArgumentError,
      Postgrex.Utils.encode_msg(type_info, value, Postgrex.Circle)
  end

  def decode(_, data, _, _) do
    <<centerx::float64, centery::float64, radius::float64>> = data
    %Postgrex.Circle{
      center: %Postgrex.Point{x: centerx, y: centery},
      radius: radius
    }
  end
end
