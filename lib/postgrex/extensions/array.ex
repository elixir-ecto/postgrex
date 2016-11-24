defmodule Postgrex.Extensions.Array do
  @moduledoc false
  alias Postgrex.TypeInfo
  alias Postgrex.Types
  import Postgrex.BinaryUtils
  use Postgrex.BinaryExtension, [] # send: "array_send" hard coded in types

  def init(_, opts), do: Keyword.fetch!(opts, :null)

  def encode(%TypeInfo{array_elem: elem_oid}, list, types, null) when is_list(list),
    do: encode_array(list, elem_oid, types, null)
  def encode(type_info, value, _, _) do
    raise ArgumentError, Postgrex.Utils.encode_msg(type_info, value, "a list")
  end

  def decode(_, bin, types, null),
    do: decode_array(bin, types, null)

  ## Helpers

  defp encode_array(list, elem_oid, types, null) do
    encoder =
      fn
        value when value == null ->
          <<-1::int32>>
        value ->
          data = Types.encode(elem_oid, value, types)
          [<<IO.iodata_length(data)::int32>> | data]
      end

    {data, ndims, lengths} = do_encode_array(list, 0, [], encoder)
    lengths = for len <- Enum.reverse(lengths), do: <<len :: int32, 1 :: int32>>
    [<<ndims :: int32, 0 :: int32, elem_oid :: uint32>>, lengths, data]
  end

  defp do_encode_array([], ndims, lengths, _encoder) do
    {"", ndims, lengths}
  end

  defp do_encode_array([head|tail]=list, ndims, lengths, encoder)
  when is_list(head) do
    lengths = [length(list)|lengths]
    {data, ndims, lengths} = do_encode_array(head, ndims, lengths, encoder)
    [dimlength|_] = lengths

    rest = Enum.reduce(tail, [], fn sublist, acc ->
      {data, _, [len|_]} = do_encode_array(sublist, ndims, lengths, encoder)
      if len != dimlength do
        raise ArgumentError, "nested lists must have lists with matching lengths"
      end
        [acc|data]
      end)

    {[data|rest], ndims+1, lengths}
  end

  defp do_encode_array(list, ndims, lengths, encoder) do
    {data, length} = Enum.map_reduce(list, 0, &{encoder.(&1), &2+1})
    {data, ndims+1, [length|lengths]}
  end

  defp decode_array(<<ndims :: int32, _has_null :: int32, oid :: uint32,
                      dims :: size(ndims)-binary-unit(64), rest :: binary>>,
                      types, null) do
    lengths = for <<len :: int32, _lbound :: int32 <- dims>>, do: len
    decoder = &Types.decode(oid, &1, types)

    {array, ""} = decode_array(rest, lengths, decoder, null)
    array
  end

  defp decode_array("", [], _decoder, _null) do
    {[], ""}
  end

  defp decode_array(rest, [len], decoder, null) do
    array_elements(rest, len, [], decoder, null)
  end

  defp decode_array(rest, [len|lens], decoder, null) do
    Enum.map_reduce(1..len, rest, fn _, rest ->
      decode_array(rest, lens, decoder, null)
    end)
  end

  defp array_elements(rest, 0, acc, _decoder, _null) do
    {Enum.reverse(acc), rest}
  end

  defp array_elements(<<-1 :: int32, rest :: binary>>, count, acc, decoder, null) do
    array_elements(rest, count-1, [null|acc], decoder, null)
  end

  defp array_elements(<<size :: int32, elem :: binary(size), rest :: binary>>,
                       count, acc, decoder, null) do
    value = decoder.(elem)
    array_elements(rest, count-1, [value|acc], decoder, null)
  end
end


