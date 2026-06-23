defmodule Postgrex.Extensions.MACADDR do
  @moduledoc false
  import Postgrex.BinaryUtils, warn: false
  use Postgrex.BinaryExtension, send: "macaddr_send"

  @octet_range 0..255

  def encode(_) do
    octet_range = Macro.escape(@octet_range)

    quote location: :keep do
      %Postgrex.MACADDR{address: {a, b, c, d, e, f}}
      when a in unquote(octet_range) and b in unquote(octet_range) and
             c in unquote(octet_range) and d in unquote(octet_range) and
             e in unquote(octet_range) and f in unquote(octet_range) ->
        <<6::int32(), a, b, c, d, e, f>>

      other ->
        raise DBConnection.EncodeError,
              Postgrex.Utils.encode_msg(other, "a %Postgrex.MACADDR{} with octets in 0..255")
    end
  end

  def decode(_) do
    quote location: :keep do
      <<6::int32(), a::8, b::8, c::8, d::8, e::8, f::8>> ->
        %Postgrex.MACADDR{address: {a, b, c, d, e, f}}
    end
  end
end
