defmodule Postgrex.Extensions.CIDR do
  @moduledoc false

  import Postgrex.BinaryUtils, warn: false
  use Postgrex.BinaryExtension, [send: "cidr_send"]

  def encode(_) do
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

  def decode(_) do
    quote location: :keep do
      <<8 :: int32, 2, n, 1, 4, a, b, c, d>> ->
        %Postgrex.CIDR{address: {a, b, c, d}, netmask: n}
      <<20 :: int32, 3, n, 1, 16,
        a::16, b::16, c::16, d::16, e::16, f::16, g::16, h::16>> ->
        %Postgrex.CIDR{address: {a, b, c, d, e, f, g, h}, netmask: n}
    end
  end
end
