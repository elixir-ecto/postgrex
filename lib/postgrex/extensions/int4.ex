defmodule Postgrex.Extensions.Int4 do
  @moduledoc false
  import Postgrex.BinaryUtils, warn: false
  use Postgrex.BinaryExtension, send: "int4send"

  @int4_range -2147483648..2147483647

  def encode(_) do
    range = Macro.escape(@int4_range)
    quote location: :keep do
      int when is_integer(int) and int in unquote(range) ->
        <<4 :: int32, int :: int32>>
      other ->
        raise DBConnection.EncodeError, Postgrex.Utils.encode_msg(other, unquote(range))
    end
  end

  def decode(_) do
    quote location: :keep do
      <<4 :: int32, int :: int32>> -> int
    end
  end
end
