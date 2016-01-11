defmodule Postgrex.Extensions.Array do
  @moduledoc false
  alias Postgrex.TypeInfo
  alias Postgrex.Types
  import Postgrex.BinaryUtils
  use Postgrex.BinaryExtension, [] # send: "array_send" hard coded in types

  def encode(%TypeInfo{array_elem: elem_oid}, list, types, _) when is_list(list),
    do: encode_array(list, elem_oid, types)
  def encode(type_info, value, _, _) do
    raise ArgumentError, Postgrex.Utils.encode_msg(type_info, value, "a list")
  end

  def decode(_, bin, types, _),
    do: decode_array(bin, types)

  ## Helpers

  defp encode_array(list, elem_oid, types) do
    encoder = &Types.encode(elem_oid, &1, types)

    {data, ndims, lengths} = encode_array(list, 0, [], encoder)
    lengths = for len <- Enum.reverse(lengths), do: <<len :: int32, 1 :: int32>>
    [<<ndims :: int32, 0 :: int32, elem_oid :: uint32>>, lengths, data]
  end

  defp encode_array([], ndims, lengths, _encoder) do
    {"", ndims, lengths}
  end

  defp encode_array([head|tail]=list, ndims, lengths, encoder)
  when is_list(head) do
    lengths = [length(list)|lengths]
    {data, ndims, lengths} = encode_array(head, ndims, lengths, encoder)
    [dimlength|_] = lengths

    rest = Enum.reduce(tail, [], fn sublist, acc ->
      {data, _, [len|_]} = encode_array(sublist, ndims, lengths, encoder)
      if len != dimlength do
        raise ArgumentError, "nested lists must have lists with matching lengths"
      end
        [acc|data]
      end)

    {[data|rest], ndims+1, lengths}
  end

  defp encode_array(list, ndims, lengths, encoder) do
    {data, length} =
    Enum.map_reduce(list, 0, fn
      nil, length ->
        {<<-1::int32>>, length + 1}
      elem, length ->
        data = encoder.(elem)
        data = [<<IO.iodata_length(data)::int32>>, data]
        {data, length + 1}
    end)
    {data, ndims+1, [length|lengths]}
  end

  defp decode_array(<<ndims :: int32, _has_null :: int32, oid :: uint32,
                      dims :: size(ndims)-binary-unit(64), rest :: binary>>, types) do
    lengths = for <<len :: int32, _lbound :: int32 <- dims>>, do: len
    decoder = &Types.decode(oid, &1, types)

    {array, ""} = decode_array(rest, lengths, decoder)
    array
  end

  defp decode_array("", [], _decoder) do
    {[], ""}
  end

  defp decode_array(rest, [len], decoder) do
    array_elements(rest, len, [], decoder)
  end

  defp decode_array(rest, [len|lens], decoder) do
    Enum.map_reduce(1..len, rest, fn _, rest ->
      decode_array(rest, lens, decoder)
    end)
  end

  defp array_elements(rest, 0, acc, _decoder) do
    {Enum.reverse(acc), rest}
  end

  defp array_elements(<<-1 :: int32, rest :: binary>>, count, acc, decoder) do
    array_elements(rest, count-1, [nil|acc], decoder)
  end

  defp array_elements(<<size :: int32, elem :: binary(size), rest :: binary>>,
                       count, acc, decoder) do
    value = decoder.(elem)
    array_elements(rest, count-1, [value|acc], decoder)
  end
end


