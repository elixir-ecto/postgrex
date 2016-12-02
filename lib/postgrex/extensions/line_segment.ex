defmodule Postgrex.Extensions.LineSegment do
  @moduledoc false

  import Postgrex.BinaryUtils
  use Postgrex.BinaryExtension, send: "lseg_send"

  def encode(_,
    %Postgrex.LineSegment{
      point1: %Postgrex.Point{x: x1, y: y1},
      point2: %Postgrex.Point{x: x2, y: y2}
    }, _, _)
  when is_number(x1) and is_number(x2) and is_number(y1) and is_number(y2) do
    <<x1::float64, y1::float64, x2::float64, y2::float64>>
  end
  def encode(type_info, value, _, _) do
    raise ArgumentError,
      Postgrex.Utils.encode_msg(type_info, value, Postgrex.LineSegment)
  end

  def decode(_, data, _, _) do
    <<x1::float64, y1::float64, x2::float64, y2::float64>> = data
    %Postgrex.LineSegment{
      point1: %Postgrex.Point{x: x1, y: y1},
      point2: %Postgrex.Point{x: x2, y: y2},
    }
  end
end
