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

  def decode(:bool, << 1 :: int8 >>), do: true
  def decode(:bool, << 0 :: int8 >>), do: false
  def decode(:bpchar, << c :: int8 >>), do: c
  def decode(:int2, << n :: int16 >>), do: n
  def decode(:int4, << n :: int32 >>), do: n
  def decode(:int8, << n :: int64 >>), do: n
  def decode(:float4, << n :: float32 >>), do: n
  def decode(:float8, << n :: float64 >>), do: n
  def decode(_, bin), do: bin
end
