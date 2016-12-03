defmodule Postgrex.Extensions.INET do
  @moduledoc false

  import Postgrex.BinaryUtils, warn: false
  use Postgrex.BinaryExtension, [send: "inet_send"]

  def encode(_) do
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

  def decode(_) do
    quote location: :keep do
      <<8 :: int32, 2, 32, 0, 4, a, b, c, d>> ->
        %Postgrex.INET{address: {a, b, c, d}}
      <<20 :: int32, 3, 128, 0, 16,
        a::16, b::16, c::16, d::16, e::16, f::16, g::16, h::16>> ->
        %Postgrex.INET{address: {a, b, c, d, e, f, g, h}}
    end
  end
end
