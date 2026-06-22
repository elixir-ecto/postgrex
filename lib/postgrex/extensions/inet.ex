defmodule Postgrex.Extensions.INET do
  @moduledoc false

  import Postgrex.BinaryUtils, warn: false
  use Postgrex.BinaryExtension, send: "cidr_send", send: "inet_send"

  @octet_range 0..255
  @hextet_range 0..65535
  @ipv4_netmask_range 0..32
  @ipv6_netmask_range 0..128

  def encode(_) do
    octet_range = Macro.escape(@octet_range)
    hextet_range = Macro.escape(@hextet_range)
    ipv4_netmask_range = Macro.escape(@ipv4_netmask_range)
    ipv6_netmask_range = Macro.escape(@ipv6_netmask_range)

    quote location: :keep do
      %Postgrex.INET{address: {a, b, c, d}, netmask: nil}
      when a in unquote(octet_range) and b in unquote(octet_range) and
             c in unquote(octet_range) and d in unquote(octet_range) ->
        <<8::int32(), 2, 32, 0, 4, a, b, c, d>>

      %Postgrex.INET{address: {a, b, c, d}, netmask: n}
      when a in unquote(octet_range) and b in unquote(octet_range) and
             c in unquote(octet_range) and d in unquote(octet_range) and
             n in unquote(ipv4_netmask_range) ->
        <<8::int32(), 2, n, 1, 4, a, b, c, d>>

      %Postgrex.INET{address: {a, b, c, d, e, f, g, h}, netmask: nil}
      when a in unquote(hextet_range) and b in unquote(hextet_range) and
             c in unquote(hextet_range) and d in unquote(hextet_range) and
             e in unquote(hextet_range) and f in unquote(hextet_range) and
             g in unquote(hextet_range) and h in unquote(hextet_range) ->
        <<20::int32(), 3, 128, 0, 16, a::16, b::16, c::16, d::16, e::16, f::16, g::16, h::16>>

      %Postgrex.INET{address: {a, b, c, d, e, f, g, h}, netmask: n}
      when a in unquote(hextet_range) and b in unquote(hextet_range) and
             c in unquote(hextet_range) and d in unquote(hextet_range) and
             e in unquote(hextet_range) and f in unquote(hextet_range) and
             g in unquote(hextet_range) and h in unquote(hextet_range) and
             n in unquote(ipv6_netmask_range) ->
        <<20::int32(), 3, n, 1, 16, a::16, b::16, c::16, d::16, e::16, f::16, g::16, h::16>>

      other ->
        raise DBConnection.EncodeError,
              Postgrex.Utils.encode_msg(
                other,
                "a %Postgrex.INET{} with octets in 0..255 and netmask in 0..32 (IPv4) " <>
                  "or groups in 0..65535 and netmask in 0..128 (IPv6)"
              )
    end
  end

  def decode(_) do
    quote location: :keep do
      <<8::int32(), 2, n, cidr?, 4, a, b, c, d>> ->
        n = if(cidr? == 1 or n != 32, do: n, else: nil)
        %Postgrex.INET{address: {a, b, c, d}, netmask: n}

      <<20::int32(), 3, n, cidr?, 16, a::16, b::16, c::16, d::16, e::16, f::16, g::16, h::16>> ->
        n = if(cidr? == 1 or n != 128, do: n, else: nil)
        %Postgrex.INET{address: {a, b, c, d, e, f, g, h}, netmask: n}
    end
  end
end
