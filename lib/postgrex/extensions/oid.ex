defmodule Postgrex.Extensions.OID do
  @moduledoc false
  @oid_senders ~w(oidsend regprocsend regproceduresend regopersend
                  regoperatorsend regclasssend regtypesend xidsend cidsend)

  import Postgrex.BinaryUtils
  use Postgrex.BinaryExtension, Enum.map(@oid_senders, &{:send, &1})

  @oid_range 0..4294967295

  def encode(_, n, _, _) when is_integer(n) and n in @oid_range,
    do: <<n :: uint32>>
  def encode(type_info, value, _, _) do
    raise ArgumentError,
      Postgrex.Utils.encode_msg(type_info, value, @oid_range)
  end

  def decode(_, <<n :: uint32>>, _, _),
    do: n

  def inline(_type_info, _types, _opts) do
    {__MODULE__, inline_encode(), inline_decode()}
  end

  defp inline_encode() do
    range = Macro.escape(@oid_range)
    quote location: :keep do
      oid when is_integer(oid) and oid in unquote(range) ->
        <<4 :: int32, oid :: uint32>>
      other ->
        raise ArgumentError, Postgrex.Utils.encode_msg(other, unquote(range))
    end
  end

  defp inline_decode() do
    quote location: :keep do
      <<4 :: int32, oid :: uint32>> -> oid
    end
  end
end
