defmodule Postgrex.Extensions.Path do
  @moduledoc false
  import Postgrex.BinaryUtils, warn: false
  use Postgrex.BinaryExtension, send: "path_send"
  alias Postgrex.Extensions.Path
  alias Postgrex.Extensions.Point

  def encode(_) do
    quote location: :keep, generated: true do
      %Postgrex.Path{open: o, points: ps} when is_list(ps) and is_boolean(o) ->
        open_byte = Path.open_to_byte(o)
        len = length(ps)

        encoded_points =
          Enum.reduce(ps, [], fn p, acc -> [acc | Point.encode_point(p, Postgrex.Path)] end)

        # 1 byte for open/closed flag, 4 for length, 16 for each point
        nbytes = 5 + 16 * len
        [<<nbytes::int32>>, open_byte, <<len::int32>> | encoded_points]

      other ->
        raise DBConnection.EncodeError, Postgrex.Utils.encode_msg(other, Postgrex.Path)
    end
  end

  def decode(_) do
    quote location: :keep do
      <<nbytes::int32, path_data::binary-size(nbytes)>> ->
        Path.decode_path(path_data)
    end
  end

  def decode_path(<<o::int8, n::int32, point_data::binary-size(n)-unit(128)>>) do
    open = o == 0
    points = decode_points(point_data, [])
    %Postgrex.Path{open: open, points: points}
  end

  def open_to_byte(true), do: 0
  def open_to_byte(false), do: 1

  defp decode_points(<<>>, points), do: Enum.reverse(points)

  defp decode_points(<<x::float64, y::float64, rest::bits>>, points) do
    decode_points(rest, [%Postgrex.Point{x: x, y: y} | points])
  end
end
