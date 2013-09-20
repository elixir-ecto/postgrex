defmodule Postgrex.Types do
  import Postgrex.BinaryUtils

  def build_types(rows) do
    Enum.reduce(rows, HashDict.new, fn row, acc ->
      [oid, send] = row
      { oid, "" } = String.to_integer(oid)
      send_size = String.length(send)

      send =
        try do
          cond do
          String.ends_with?(send, "_send") ->
            String.slice(send, 0, send_size - 5) |> binary_to_existing_atom
          String.ends_with?(send, "send") ->
            String.slice(send, 0, send_size - 4) |> binary_to_existing_atom
          true ->
            nil
          end
        catch
          :error, :badarg -> nil
        end

      if send, do: Dict.put(acc, oid, send), else: acc
    end)
  end

  def bootstrap_query do
    "SELECT oid, typsend FROM pg_type"
  end

  def decode(:bool, << 1 :: int8 >>, _), do: true
  def decode(:bool, << 0 :: int8 >>, _), do: false
  def decode(:bpchar, << c :: int8 >>, _), do: c
  def decode(:int2, << n :: int16 >>, _), do: n
  def decode(:int4, << n :: int32 >>, _), do: n
  def decode(:int8, << n :: int64 >>, _), do: n
  def decode(:float4, << n :: float32 >>, _), do: n
  def decode(:float8, << n :: float64 >>, _), do: n
  def decode(:array, bin, types), do: decode_array(bin, types)
  def decode(_, bin, _), do: bin

  defp decode_array(<< ndims :: int32, _has_null :: int32, oid :: int32, rest :: binary >>, types) do
    { dims, rest } = :erlang.split_binary(rest, ndims * 2 * 4)
    lengths = lc << len :: int32, _lbound :: int32 >> inbits dims, do: len
    type = Dict.get(types, oid)
    { array, "" } = decode_array(rest, type, types, lengths)
    array
  end

  defp decode_array("", _type, _types, []) do
    { [], "" }
  end

  defp decode_array(rest, type, types, [len]) do
    decode_elements(rest, type, types, [], len)
  end

  defp decode_array(rest, type, types, [len|lens]) do
    Enum.map_reduce(1..len, rest, fn _, rest ->
      decode_array(rest, type, types, lens)
    end)
  end

  defp decode_elements(rest, _type, _types, acc, 0) do
    { Enum.reverse(acc), rest }
  end

  defp decode_elements(<< -1 :: int32, rest :: binary >>, type, types, acc, count) do
    decode_elements(rest, type, types, [nil|acc], count-1)
  end

  defp decode_elements(<< length :: int32, value :: binary(length), rest :: binary >>,
                       type, types, acc, count) do
    value = decode(type, value, types)
    decode_elements(rest, type, types, [value|acc], count-1)
  end
end
