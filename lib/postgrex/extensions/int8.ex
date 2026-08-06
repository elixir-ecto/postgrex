defmodule Postgrex.Extensions.Int8 do
  @moduledoc false
  import Postgrex.BinaryUtils, warn: false
  # CockroachDB names pg_lsn's send proc without an underscore (pg_lsnsend),
  # so match its spelling in addition to Postgres'.
  use Postgrex.BinaryExtension, send: "int8send", send: "pg_lsn_send", send: "pg_lsnsend"

  @int8_range -9_223_372_036_854_775_808..9_223_372_036_854_775_807

  def encode(_) do
    range = Macro.escape(@int8_range)

    quote location: :keep do
      int when is_integer(int) and int in unquote(range) ->
        <<8::int32(), int::int64()>>

      other ->
        raise DBConnection.EncodeError, Postgrex.Utils.encode_msg(other, unquote(range))
    end
  end

  def decode(_) do
    quote location: :keep do
      <<8::int32(), int::int64()>> -> int
    end
  end
end
