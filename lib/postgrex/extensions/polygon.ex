defmodule Postgrex.Extensions.Polygon do
  @moduledoc false
  import Postgrex.BinaryUtils, warn: false
  use Postgrex.BinaryExtension, send: "poly_send"
  alias Postgrex.Extensions.Polygon
  alias Postgrex.Extensions.Point

  def encode(_) do
    quote location: :keep do
      %Postgrex.Polygon{vertices: vertices} when is_list(vertices) ->
        len = << length(vertices)::int32 >>
        vert = vertices
        |> Enum.map(fn(p) -> Point.encode_point(p, Postgrex.Polygon) end)

        # 32 bits for len, 64 for each x and each y
        nbytes = 4 + 16 * length(vertices)

        [<<nbytes :: int32>>, len, vert]
      other ->
        raise ArgumentError, Postgrex.Utils.encode_msg(other, Postgrex.Polygon)
    end
  end

  def decode(_) do
    quote location: :keep do
      <<nbytes::int32, polygon_data::binary-size(nbytes)>> ->
        vertices = Polygon.decode_vertices(polygon_data)
        %Postgrex.Polygon{vertices: vertices}
    end
  end

  # n vertices, 128 bits for each vertex - 64 for x, 64 for y
  def decode_vertices(<<n::int32, vert_data::binary-size(n)-unit(128)>>) do
    decode_vertices(n, vert_data, [])
  end

  defp decode_vertices(0, _, v), do: Enum.reverse(v)
  defp decode_vertices(n, <<x::float64, y::float64, rest::bits>>, v) do
    decode_vertices(n-1, rest, [%Postgrex.Point{x: x, y: y} | v])
  end
end
