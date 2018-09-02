defmodule Postgrex.Extensions.TID do
  @moduledoc false
  import Postgrex.BinaryUtils, warn: false
  use Postgrex.BinaryExtension, send: "tidsend"

  def encode(_) do
    quote location: :keep do
      {block, tuple} ->
        <<6 :: int32, block :: uint32, tuple :: uint16>>
      other ->
        raise DBConnection.EncodeError, Postgrex.Utils.encode_msg(other, "a tuple of 2 integers")
    end
  end

  def decode(_) do
    quote location: :keep do
      <<6 :: int32, block :: uint32, tuple :: uint16>> ->
        {block, tuple}
    end
  end
end
