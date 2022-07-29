defmodule Postgrex.Extensions.Box do
  @moduledoc false
  import Postgrex.BinaryUtils, warn: false
  use Postgrex.BinaryExtension, send: "box_send"
  alias Postgrex.Extensions.Point

  def encode(_) do
    quote location: :keep, generated: true do
      %Postgrex.Box{upper_right: p1, bottom_left: p2} ->
        encoded_p1 = Point.encode_point(p1, Postgrex.Box)
        encoded_p2 = Point.encode_point(p2, Postgrex.Box)
        # 2 points -> 16 bytes each
        [<<32::int32()>>, encoded_p1 | encoded_p2]

      other ->
        raise DBConnection.EncodeError, Postgrex.Utils.encode_msg(other, Postgrex.Line)
    end
  end

  def decode(_) do
    quote location: :keep do
      # 2 points -> 16 bytes each
      <<32::int32(), x1::float64(), y1::float64(), x2::float64(), y2::float64()>> ->
        p1 = %Postgrex.Point{x: x1, y: y1}
        p2 = %Postgrex.Point{x: x2, y: y2}
        %Postgrex.Box{upper_right: p1, bottom_left: p2}
    end
  end
end
