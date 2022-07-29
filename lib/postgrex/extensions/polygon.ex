defmodule Postgrex.Extensions.Polygon do
  @moduledoc false
  import Postgrex.BinaryUtils, warn: false
  use Postgrex.BinaryExtension, send: "poly_send"
  alias Postgrex.Extensions.Polygon
  alias Postgrex.Extensions.Point

  def encode(_) do
    quote location: :keep, generated: true do
      %Postgrex.Polygon{vertices: vertices} when is_list(vertices) ->
        len = length(vertices)

        vert =
          Enum.reduce(vertices, [], fn v, acc ->
            [acc | Point.encode_point(v, Postgrex.Polygon)]
          end)

        # 32 bits for len, 64 for each x and each y
        nbytes = 4 + 16 * len

        [<<nbytes::int32()>>, <<len::int32()>> | vert]

      other ->
        raise DBConnection.EncodeError, Postgrex.Utils.encode_msg(other, Postgrex.Polygon)
    end
  end

  def decode(_) do
    quote location: :keep do
      <<nbytes::int32(), polygon_data::binary-size(nbytes)>> ->
        vertices = Polygon.decode_vertices(polygon_data)
        %Postgrex.Polygon{vertices: vertices}
    end
  end

  # n vertices, 128 bits for each vertex - 64 for x, 64 for y
  def decode_vertices(<<n::int32(), vert_data::binary-size(n)-unit(128)>>) do
    decode_vertices(vert_data, [])
  end

  defp decode_vertices(<<>>, v), do: Enum.reverse(v)

  defp decode_vertices(<<x::float64(), y::float64(), rest::bits>>, v) do
    decode_vertices(rest, [%Postgrex.Point{x: x, y: y} | v])
  end
end
