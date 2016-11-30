defmodule Postgrex.Extensions.Circle do
  @moduledoc false
  import Postgrex.BinaryUtils
  use Postgrex.BinaryExtension, send: "circle_send"

  def encode(
    _,
    circle = %Postgrex.Circle{center: %Postgrex.Point{}, radius: radius},
    _, _)
  when is_number(radius) and radius >= 0 do
    <<circle.center.x::float64, circle.center.y::float64, radius::float64>>
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
