defmodule Postgrex.Extensions.TID do
  @moduledoc false
  import Postgrex.BinaryUtils, warn: false
  use Postgrex.BinaryExtension, send: "tidsend"

  @block_range 0..4_294_967_295
  @tuple_range 0..65_535

  def encode(_) do
    block_range = Macro.escape(@block_range)
    tuple_range = Macro.escape(@tuple_range)

    quote location: :keep do
      {block, tuple}
      when is_integer(block) and block in unquote(block_range) and
             is_integer(tuple) and tuple in unquote(tuple_range) ->
        <<6::int32(), block::uint32(), tuple::uint16()>>

      other ->
        raise DBConnection.EncodeError,
              Postgrex.Utils.encode_msg(
                other,
                "a tuple of a 32-bit integer and a 16-bit integer"
              )
    end
  end

  def decode(_) do
    quote location: :keep do
      <<6::int32(), block::uint32(), tuple::uint16()>> ->
        {block, tuple}
    end
  end
end
