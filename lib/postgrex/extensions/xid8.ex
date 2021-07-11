defmodule Postgrex.Extensions.Xid8 do
  @moduledoc false
  import Postgrex.BinaryUtils, warn: false
  use Postgrex.BinaryExtension, send: "xid8send"

  @xid8_range 0..18_446_744_073_709_551_615

  def encode(_) do
    range = Macro.escape(@xid8_range)

    quote location: :keep do
      int when int in unquote(range) ->
        <<8::int32, int::uint64>>

      other ->
        raise DBConnection.EncodeError, Postgrex.Utils.encode_msg(other, unquote(range))
    end
  end

  def decode(_) do
    quote location: :keep do
      <<8::int32, int::uint64>> -> int
    end
  end
end
