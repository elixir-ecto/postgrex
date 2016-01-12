defmodule Postgrex.Extensions.MACADDR do
  @moduledoc false
  use Postgrex.BinaryExtension, [send: "macaddr_send"]

  def encode(_, %Postgrex.MACADDR{address: {a, b, c, d, e, f}}, _, _),
    do: <<a, b, c, d, e, f>>
  def encode(type_info, value, _, _) do
    raise ArgumentError,
      Postgrex.Utils.encode_msg(type_info, value, Postgrex.MACADDR)
  end

  def decode(_, <<a::8, b::8, c::8, d::8, e::8, f::8>>, _, _),
    do: %Postgrex.MACADDR{address: {a, b, c, d, e, f}}
end
