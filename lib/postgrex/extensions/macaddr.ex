defmodule Postgrex.Extensions.MACADDR do
  @moduledoc false
  import Postgrex.BinaryUtils, warn: false
  use Postgrex.BinaryExtension, send: "macaddr_send"

  def encode(_) do
    quote location: :keep do
      %Postgrex.MACADDR{address: {a, b, c, d, e, f}} ->
        <<6::int32, a, b, c, d, e, f>>

      other ->
        raise DBConnection.EncodeError, Postgrex.Utils.encode_msg(other, Postgrex.MACADDR)
    end
  end

  def decode(_) do
    quote location: :keep do
      <<6::int32, a::8, b::8, c::8, d::8, e::8, f::8>> ->
        %Postgrex.MACADDR{address: {a, b, c, d, e, f}}
    end
  end
end
