defmodule Postgrex.Extensions.UUID do
  @moduledoc false
  import Postgrex.BinaryUtils
  use Postgrex.BinaryExtension, send: "uuid_send"

  def encode(_, <<_ :: binary(16)>> = bin, _, _),
    do: bin
  def encode(type_info, value, _, _) do
    raise ArgumentError,
      Postgrex.Utils.encode_msg(type_info, value, "a binary of 16 bytes")
  end

  def decode(_, bin, _, _),
    do: bin
end
