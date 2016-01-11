defmodule Postgrex.Extensions.Record do
  @moduledoc false
  alias Postgrex.TypeInfo
  alias Postgrex.Types
  import Postgrex.BinaryUtils
  use Postgrex.BinaryExtension, [] # send: "record_send" hard cored in types

  def encode(%TypeInfo{comp_elems: elem_oids}, tuple, types, _) when is_tuple(tuple),
    do: encode_record(tuple, elem_oids, types)
  def encode(type_info, value, _, _) do
    raise ArgumentError, Postgrex.Utils.encode_msg(type_info, value, "a tuple")
  end

  def decode(_, bin, types, _),
    do: decode_record(bin, types)

  ## Helpers

  defp encode_record(tuple, elem_oids, types) do
    list = Tuple.to_list(tuple)
    zipped = :lists.zip(list, elem_oids)

    {data, count} =
      Enum.map_reduce(zipped, 0, fn
        {nil, oid}, count ->
          {<<oid::uint32, -1::int32>>, count + 1}
        {value, oid}, count ->
          data = Types.encode(oid, value, types)
          data = [<<oid::uint32>>, <<IO.iodata_length(data)::int32>>, data]
          {data, count + 1}
      end)

    [<<count :: int32>>, data]
  end

  defp decode_record(<<num :: int32, rest :: binary>>, types) do
    decoder = &Types.decode(&1, &2, types)
    record_elements(rest, num, decoder) |> List.to_tuple
  end

  defp record_elements(<<>>, 0, _decoder) do
    []
  end

  defp record_elements(<<_oid :: uint32, -1 :: int32, rest :: binary>>, num, decoder) do
    [nil | record_elements(rest, num-1, decoder)]
  end

  defp record_elements(<<oid :: uint32, size :: int32, elem :: binary(size), rest :: binary>>,
                       num, decoder) do
    value = decoder.(oid, elem)
    [value | record_elements(rest, num-1, decoder)]
  end
end
