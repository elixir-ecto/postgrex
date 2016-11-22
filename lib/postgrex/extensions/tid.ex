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

  def inline(_type_info, _types, _opts) do
    {__MODULE__, inline_encode(), inline_decode()}
  end

  defp inline_encode() do
    quote location: :keep do
      {block, tuple} ->
        <<6 :: int32, block :: uint32, tuple :: uint16>>
      other ->
        raise ArgumentError,
          Postgrex.Utils.encode_msg(other, "a tuple of 2 integers")
    end
  end

  defp inline_decode() do
    quote location: :keep do
      <<6 :: int32, block :: uint32, tuple :: uint16>> ->
        {block, tuple}
    end
  end
end
