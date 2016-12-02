defmodule Postgrex.Extensions.Point do
  @moduledoc false
  import Postgrex.BinaryUtils
  use Postgrex.BinaryExtension, send: "point_send"

  def encode(type_info, point, _, _) do
    encode_point(type_info, point, Postgrex.Point)
  end

  # used by other geometric types to encode a single point
  #   - wanted_type should be the type we put in the error message if
  #     the supplied data is invalid
  def encode_point(
    _type_info,
    %Postgrex.Point{x: x, y: y},
    _wanted_type
  ) when is_number(x) and is_number(y) do
    <<x::float64, y::float64>>
  end
  def encode_point(type_info, value, wanted_type) do
    raise ArgumentError,
      Postgrex.Utils.encode_msg(type_info, value, wanted_type)
  end

  def decode(_, <<x::float64, y::float64>>, _, _),
    do: %Postgrex.Point{x: x, y: y}
end
