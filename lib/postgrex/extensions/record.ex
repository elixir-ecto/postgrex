defmodule Postgrex.Extensions.Record do
  @moduledoc false
  alias Postgrex.TypeInfo
  alias Postgrex.Types
  import Postgrex.BinaryUtils
  use Postgrex.BinaryExtension, [] # send: "record_send" hard cored in types

  def init(_, opts), do: Keyword.fetch!(opts, :null)

  def encode(%TypeInfo{comp_elems: elem_oids}, tuple, types, null)
      when is_tuple(tuple),
    do: encode_record(tuple, elem_oids, types, null)
  def encode(type_info, value, _, _) do
    raise ArgumentError, Postgrex.Utils.encode_msg(type_info, value, "a tuple")
  end

  def decode(_, bin, types, null),
    do: decode_record(bin, types, null)

  ## Helpers

  defp encode_record(tuple, elem_oids, types, null) do
    list = Tuple.to_list(tuple)
    zipped = :lists.zip(list, elem_oids)

    {data, count} =
      Enum.map_reduce(zipped, 0, fn
        {^null, oid}, count ->
          {<<oid::uint32, -1::int32>>, count + 1}
        {value, oid}, count ->
          data = Types.encode(oid, value, types)
          data = [<<oid::uint32>>, <<IO.iodata_length(data)::int32>>, data]
          {data, count + 1}
      end)

    [<<count :: int32>>, data]
  end

  defp decode_record(<<num :: int32, rest :: binary>>, types, null) do
    decoder = &Types.decode(&1, &2, types)
    record_elements(rest, num, decoder, null) |> List.to_tuple
  end

  defp record_elements(<<>>, 0, _decoder, _null) do
    []
  end

  defp record_elements(<<_oid :: uint32, -1 :: int32, rest :: binary>>, num,
                       decoder, null) do
    [null | record_elements(rest, num-1, decoder, null)]
  end

  defp record_elements(<<oid :: uint32, size :: int32, elem :: binary(size), rest :: binary>>,
                       num, decoder, null) do
    value = decoder.(oid, elem)
    [value | record_elements(rest, num-1, decoder, null)]
  end
end
