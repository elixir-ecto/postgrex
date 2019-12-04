defmodule Postgrex.Extensions.Array do
  @moduledoc false
  import Postgrex.BinaryUtils, warn: false
  @behaviour Postgrex.SuperExtension

  def init(_), do: nil

  def matching(_),
    do: [send: "array_send"]

  def format(_),
    do: :super_binary

  def oids(%Postgrex.TypeInfo{array_elem: elem_oid}, _),
    do: [elem_oid]

  def encode(_) do
    quote location: :keep do
      list, [oid], [type] when is_list(list) ->
        # encode_list/2 defined by TypeModule
        encoder = &encode_list(&1, type)
        unquote(__MODULE__).encode(list, oid, encoder)

      other, _, _ ->
        raise DBConnection.EncodeError, Postgrex.Utils.encode_msg(other, "a list")
    end
  end

  def decode(_) do
    quote location: :keep do
      <<len::int32, binary::binary-size(len)>>, [oid], [type] ->
        <<ndims::int32, _has_null::int32, ^oid::uint32, dims::size(ndims)-binary-unit(64),
          data::binary>> = binary

        # decode_list/2 defined by TypeModule
        flat = decode_list(data, type)

        unquote(__MODULE__).decode(dims, flat)
    end
  end

  ## Helpers

  # Special case for empty lists. This treats an empty list as an empty 1-dim array.
  # While libpq will decode an payload encoded for a 0-dim array, CockroachDB will not.
  # Also, this is how libpq actually encodes 0-dim arrays.
  def encode([], elem_oid, _encoder) do
    <<20::int32, 1::int32, 0::int32, elem_oid::uint32, 0::int32, 1::int32>>
  end

  def encode(list, elem_oid, encoder) do
    {data, ndims, lengths} = encode(list, 0, [], encoder)
    lengths = for len <- Enum.reverse(lengths), do: <<len::int32, 1::int32>>
    iodata = [<<ndims::int32, 0::int32, elem_oid::uint32>>, lengths, data]
    [<<IO.iodata_length(iodata)::int32>> | iodata]
  end

  defp encode([], ndims, lengths, _encoder) do
    {"", ndims, lengths}
  end

  defp encode([head | tail] = list, ndims, lengths, encoder) when is_list(head) do
    lengths = [length(list) | lengths]
    {data, ndims, lengths} = encode(head, ndims, lengths, encoder)
    [dimlength | _] = lengths

    rest =
      Enum.reduce(tail, [], fn sublist, acc ->
        {data, _, [len | _]} = encode(sublist, ndims, lengths, encoder)

        if len != dimlength do
          raise ArgumentError, "nested lists must have lists with matching lengths"
        end

        [acc | data]
      end)

    {[data | rest], ndims + 1, lengths}
  end

  defp encode(list, ndims, lengths, encoder) do
    {encoder.(list), ndims + 1, [length(list) | lengths]}
  end

  def decode(dims, elems) do
    case decode_dims(dims, []) do
      [] when elems == [] ->
        []

      [length] when length(elems) == length ->
        Enum.reverse(elems)

      lengths ->
        {array, []} = nest(elems, lengths)
        array
    end
  end

  defp decode_dims(<<len::int32, _lbound::int32, rest::binary>>, acc) do
    decode_dims(rest, [len | acc])
  end

  defp decode_dims(<<>>, acc) do
    Enum.reverse(acc)
  end

  # elems and lengths in reverse order
  defp nest(elems, [len]) do
    nest_inner(elems, len, [])
  end

  defp nest(elems, [len | lengths]) do
    nest(elems, len, lengths, [])
  end

  defp nest(elems, 0, _, acc) do
    {acc, elems}
  end

  defp nest(elems, n, lengths, acc) do
    {row, elems} = nest(elems, lengths)
    nest(elems, n - 1, lengths, [row | acc])
  end

  defp nest_inner(elems, 0, acc) do
    {acc, elems}
  end

  defp nest_inner([elem | elems], n, acc) do
    nest_inner(elems, n - 1, [elem | acc])
  end
end
