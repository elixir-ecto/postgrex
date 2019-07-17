defmodule Postgrex.Extensions.VoidBinary do
  @moduledoc false
  import Postgrex.BinaryUtils, warn: false
  use Postgrex.BinaryExtension, send: "void_send"

  def encode(_) do
    quote location: :keep do
      :void ->
        <<0::int32>>

      other ->
        raise DBConnection.EncodeError, Postgrex.Utils.encode_msg(other, "the atom :void")
    end
  end

  def decode(_) do
    quote location: :keep do
      <<0::int32>> -> :void
    end
  end
end
