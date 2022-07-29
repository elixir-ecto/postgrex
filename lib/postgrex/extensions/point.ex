defmodule Postgrex.Extensions.Point do
  @moduledoc false
  import Postgrex.BinaryUtils, warn: false
  use Postgrex.BinaryExtension, send: "point_send"

  def encode(_) do
    quote location: :keep do
      %Postgrex.Point{x: x, y: y} ->
        <<16::int32(), x::float64(), y::float64()>>

      other ->
        raise DBConnection.EncodeError, Postgrex.Utils.encode_msg(other, Postgrex.Point)
    end
  end

  def decode(_) do
    quote location: :keep do
      <<16::int32(), x::float64(), y::float64()>> -> %Postgrex.Point{x: x, y: y}
    end
  end

  # used by other extensions
  def encode_point(%Postgrex.Point{x: x, y: y}, _) do
    <<x::float64(), y::float64()>>
  end

  def encode_point(other, wanted) do
    raise DBConnection.EncodeError, Postgrex.Utils.encode_msg(other, wanted)
  end
end
