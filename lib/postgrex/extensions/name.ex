defmodule Postgrex.Extensions.Name do
  @moduledoc false
  use Postgrex.BinaryExtension, send: "namesend"

  def encode(_, bin, _, _) when is_binary(bin) and byte_size(bin) < 64,
    do: bin

  def encode(type_info, value, _, _) do
    raise ArgumentError,
      Postgrex.Utils.encode_msg(type_info, value, "a binary string of less than 64 bytes")
  end

  def decode(_, bin, _, _),
    do: bin
end
