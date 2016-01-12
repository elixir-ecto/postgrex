defmodule Postgrex.Extensions.Network do
  @moduledoc false
  alias Postgrex.TypeInfo
  use Postgrex.BinaryExtension, [send: "inet_send", send: "cidr_send"]

  def encode(%TypeInfo{type: "inet"}, %Postgrex.INET{} = inet, _, _),
    do: encode_network(inet)
  def encode(%TypeInfo{type: "cidr"}, %Postgrex.CIDR{} = cidr, _, _),
    do: encode_network(cidr)
  def encode(%TypeInfo{type: type} = type_info, value, _, _) do
    struct = if type == "inet", do: Postgrex.INET, else: Postgrex.CIDR
    raise ArgumentError,
      Postgrex.Utils.encode_msg(type_info, value, struct)
  end

  def decode(_, binary, _, _),
    do: decode_network(binary)

  ## Helpers

  defp encode_network(%Postgrex.INET{address: {_, _, _, _} = addr}),
    do: <<2, 32, 0, 4>> <> tuple_to_binary(addr)
  defp encode_network(%Postgrex.CIDR{address: {_, _, _, _} = addr, netmask: n}),
    do: <<2, n, 1, 4>> <> tuple_to_binary(addr)
  defp encode_network(%Postgrex.INET{address: {_, _, _, _, _, _, _, _} = addr}),
    do: <<3, 128, 0, 16>> <> tuple_to_binary(addr)
  defp encode_network(%Postgrex.CIDR{address: {_, _, _, _, _, _, _, _} = addr, netmask: n}),
    do: <<3, n, 1, 16>> <> tuple_to_binary(addr)

  defp tuple_to_binary({a, b, c, d}),
    do: <<a::8, b::8, c::8, d::8>>
  defp tuple_to_binary({a, b, c, d, e, f, g, h}),
    do: <<a::16, b::16, c::16, d::16, e::16, f::16, g::16, h::16>>

  defp decode_network(<<2, 32, 0, 4, addr::binary>>),
    do: %Postgrex.INET{address: binary_to_tuple(addr)}
  defp decode_network(<<2, netmask::8, 1, 4, addr::binary>>),
    do: %Postgrex.CIDR{address: binary_to_tuple(addr), netmask: netmask}
  defp decode_network(<<3, 128, 0, 16, addr::binary>>),
    do: %Postgrex.INET{address: binary_to_tuple(addr)}
  defp decode_network(<<3, netmask::8, 1, 16, addr::binary>>),
    do: %Postgrex.CIDR{address: binary_to_tuple(addr), netmask: netmask}

  defp binary_to_tuple(<<a::8, b::8, c::8, d::8>>),
    do: {a, b, c, d}
  defp binary_to_tuple(<<a::16, b::16, c::16, d::16, e::16, f::16, g::16, h::16>>),
    do: {a, b, c, d, e, f, g, h}
end
