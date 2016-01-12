defmodule Postgrex.Extensions.TID do
  @moduledoc false
  import Postgrex.BinaryUtils
  use Postgrex.BinaryExtension, send: "tidsend"

  def encode(_, {block, tuple}, _, _),
    do: <<block :: uint32, tuple :: uint16>>
  def encode(type_info, value, _, _) do
    raise ArgumentError,
      Postgrex.Utils.encode_msg(type_info, value, "a tuple of 2 integers")
  end

  def decode(_, <<block :: uint32, tuple :: uint16>>, _, _),
    do: {block, tuple}
end
