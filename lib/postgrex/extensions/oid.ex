defmodule Postgrex.Extensions.OID do
  @moduledoc false
  @oid_senders ~w(oidsend regprocsend regproceduresend regopersend
                  regoperatorsend regclasssend regtypesend xidsend cidsend)

  import Postgrex.BinaryUtils, warn: false
  use Postgrex.BinaryExtension, Enum.map(@oid_senders, &{:send, &1})

  @oid_range 0..4_294_967_295

  def encode(_) do
    range = Macro.escape(@oid_range)

    quote location: :keep do
      oid when is_integer(oid) and oid in unquote(range) ->
        <<4::int32, oid::uint32>>

      binary when is_binary(binary) ->
        msg =
          "you tried to use a binary for an oid type " <>
            "(#{binary}) when an integer was expected. See " <>
            "https://github.com/elixir-ecto/postgrex#oid-type-encoding"

        raise ArgumentError, msg

      other ->
        raise DBConnection.EncodeError, Postgrex.Utils.encode_msg(other, unquote(range))
    end
  end

  def decode(_) do
    quote location: :keep do
      <<4::int32, oid::uint32>> -> oid
    end
  end
end
