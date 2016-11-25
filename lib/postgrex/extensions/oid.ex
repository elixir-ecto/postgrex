defmodule Postgrex.Extensions.OID do
  @moduledoc false
  @oid_senders ~w(oidsend regprocsend regproceduresend regopersend
                  regoperatorsend regclasssend regtypesend xidsend cidsend)

  import Postgrex.BinaryUtils
  use Postgrex.BinaryExtension, Enum.map(@oid_senders, &{:send, &1})

  @oid_range 0..4294967295

  def encode(_) do
    range = Macro.escape(@oid_range)
    quote location: :keep do
      oid when is_integer(oid) and oid in unquote(range) ->
        <<4 :: int32, oid :: uint32>>
      other ->
        raise ArgumentError, Postgrex.Utils.encode_msg(other, unquote(range))
    end
  end

  def decode(_) do
    quote location: :keep do
      <<4 :: int32, oid :: uint32>> -> oid
    end
  end
end
