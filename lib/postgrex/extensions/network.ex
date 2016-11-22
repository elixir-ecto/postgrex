defmodule Postgrex.Extensions.Network do
  @moduledoc false
  alias Postgrex.TypeInfo
  use Postgrex.BinaryExtension, [send: "inet_send", send: "cidr_send"]

  def encode(%TypeInfo{send: "inet_send"}, %Postgrex.INET{} = inet, _, _),
    do: encode_network(inet)
  def encode(%TypeInfo{send: "cidr_send"}, %Postgrex.CIDR{} = cidr, _, _),
    do: encode_network(cidr)
  def encode(%TypeInfo{send: send} = type_info, value, _, _) do
    struct = if send == "inet_send", do: Postgrex.INET, else: Postgrex.CIDR
    raise ArgumentError,
      Postgrex.Utils.encode_msg(type_info, value, struct)
  end

  def decode(_, binary, _, _),
    do: decode_network(binary)

  def inline(%TypeInfo{send: send}, _types, _opts) do
    case send do
      "inet_send" -> {Postgrex.INET, inline_inet_encode(), inline_inet_decode()}
      "cidr_send" -> {Postgrex.CIDR, inline_cidr_encode(), inline_cidr_decode()}
    end
  end

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

  defp inline_inet_encode() do
    quote location: :keep do
      %Postgrex.INET{address: {a, b, c, d}} ->
        <<8 :: int32, 2, 32, 0, 4, a, b, c, d>>
      %Postgrex.INET{address: {a, b, c, d, e, f, g, h}} ->
        <<20 :: int32, 3, 128, 0, 16,
          a::16, b::16, c::16, d::16, e::16, f::16, g::16, h::16>>
      other ->
        raise ArgumentError, Postgrex.Utils.encode_msg(other, Postgrex.INET)
    end
  end

  defp inline_inet_decode() do
    quote location: :keep do
      <<8 :: int32, 2, 32, 0, 4, a, b, c, d>> ->
        %Postgrex.INET{address: {a, b, c, d}}
      <<20 :: int32, 3, 128, 0, 16,
        a::16, b::16, c::16, d::16, e::16, f::16, g::16, h::16>> ->
          %Postgrex.INET{address: {a, b, c, d, e, f, g, h}}
    end
  end

  defp inline_cidr_encode() do
    quote location: :keep do
      %Postgrex.CIDR{address: {a, b, c, d}, netmask: n} ->
        <<8 :: int32, 2, n, 1, 4, a, b, c, d>>
      %Postgrex.CIDR{address: {a, b, c, d, e, f, g, h}, netmask: n} ->
        <<20 :: int32, 3, n, 1, 16,
          a::16, b::16, c::16, d::16, e::16, f::16, g::16, h::16>>
      other ->
        raise ArgumentError, Postgrex.Utils.encode_msg(other, Postgrex.CIDR)
    end
  end

  defp inline_cidr_decode() do
    quote location: :keep do
      <<8 :: int32, 2, n, 1, 4, a, b, c, d>> ->
        %Postgrex.CIDR{address: {a, b, c, d}, netmask: n}
      <<20 :: int32, 3, n, 1, 16,
        a::16, b::16, c::16, d::16, e::16, f::16, g::16, h::16>> ->
        %Postgrex.CIDR{address: {a, b, c, d, e, f, g, h}, netmask: n}
    end
  end
end
