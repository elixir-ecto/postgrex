defmodule Postgrex.Extensions.Box do
  @moduledoc false

  import Postgrex.BinaryUtils
  use Postgrex.BinaryExtension, send: "box_send"

  def encode(_,
    %Postgrex.Box{
      upper_right: %Postgrex.Point{x: x1, y: y1},
      bottom_left: %Postgrex.Point{x: x2, y: y2}
    }, _, _)
  when is_number(x1) and is_number(x2) and is_number(y1) and is_number(y2) do
    <<x1::float64, y1::float64, x2::float64, y2::float64>>
  end
  def encode(type_info, value, _, _) do
    raise ArgumentError,
      Postgrex.Utils.encode_msg(type_info, value, Postgrex.Box)
  end

  def decode(_, data, _, _) do
    <<x1::float64, y1::float64, x2::float64, y2::float64>> = data
    %Postgrex.Box{
      upper_right: %Postgrex.Point{x: x1, y: y1},
      bottom_left: %Postgrex.Point{x: x2, y: y2}
    }
  end
end
