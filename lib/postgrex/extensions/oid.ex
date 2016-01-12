defmodule Postgrex.Extensions.OID do
  @moduledoc false
  @oid_senders ~w(oidsend regprocsend regproceduresend regopersend
                  regoperatorsend regclasssend regtypesend xidsend cidsend)

  import Postgrex.BinaryUtils
  use Postgrex.BinaryExtension, Enum.map(@oid_senders, &{:send, &1})

  @oid_range 0..4294967295

  def encode(_, n, _, _) when is_integer(n) and n in @oid_range,
    do: <<n :: uint32>>
  def encode(%Postgrex.TypeInfo{send: sender}, value, _, _) when is_binary(value) do
    raise Postgrex.Error, message: """
    you tried to use a binary instead for an oid type (#{sender}) when an
    integer was expected. See https://github.com/ericmj/postgrex#oid-type-encoding
    """
  end
  def encode(type_info, value, _, _) do
    raise ArgumentError,
      Postgrex.Utils.encode_msg(type_info, value, @oid_range)
  end

  def decode(_, <<n :: uint32>>, _, _),
    do: n
end
