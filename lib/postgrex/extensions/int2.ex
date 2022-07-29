defmodule Postgrex.Extensions.Int2 do
  @moduledoc false
  import Postgrex.BinaryUtils, warn: false
  use Postgrex.BinaryExtension, send: "int2send"

  @int2_range -32768..32767

  def encode(_) do
    range = Macro.escape(@int2_range)

    quote location: :keep do
      int when is_integer(int) and int in unquote(range) ->
        <<2::int32(), int::int16()>>

      other ->
        raise DBConnection.EncodeError, Postgrex.Utils.encode_msg(other, unquote(range))
    end
  end

  def decode(_) do
    quote location: :keep do
      <<2::int32(), int::int16()>> -> int
    end
  end
end
