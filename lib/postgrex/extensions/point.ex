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
end
