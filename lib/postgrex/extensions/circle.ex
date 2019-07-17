defmodule Postgrex.Extensions.Circle do
  @moduledoc false
  import Postgrex.BinaryUtils, warn: false
  use Postgrex.BinaryExtension, send: "circle_send"

  def encode(_) do
    quote location: :keep do
      %Postgrex.Circle{center: %Postgrex.Point{x: x, y: y}, radius: r}
      when is_number(x) and is_number(y) and is_number(r) and r >= 0 ->
        <<24::int32, x::float64, y::float64, r::float64>>

      other ->
        raise DBConnection.EncodeError, Postgrex.Utils.encode_msg(other, Postgrex.Path)
    end
  end

  def decode(_) do
    quote location: :keep do
      <<24::int32, x::float64, y::float64, r::float64>> ->
        %Postgrex.Circle{center: %Postgrex.Point{x: x, y: y}, radius: r}
    end
  end
end
