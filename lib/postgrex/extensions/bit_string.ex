defmodule Postgrex.Extensions.BitString do
  use Postgrex.BinaryExtension,
    [receive: "bit_recv", send: "bit_send", type: "bit",
     receive: "varbit_recv", send: "varbit_send", type: "varbit"]
  
  def encode(_, value, _, _) do
    bit_count = bit_size(value)
    out = encode_binary(value, [])
    << << bit_count :: size(32) >>, << bits_to_binary(out) :: bits >> >>
  end

  def decode(_, value, _, _) do
    << bit_count :: size(32), bytes :: binary >> = value
    bits = decode_binary(byte_size(bytes), [], bytes)
    |> fix_length(bit_count)

    bits_to_binary(bits)
  end

  defp decode_binary(1, so_far, rest) do
    << last :: 8 >> = rest
    so_far ++ last_bits(last)
  end
  defp decode_binary(bytes_left, so_far, bytes) do
    << int_value :: 8, rest :: binary >> = bytes
    decode_binary(bytes_left - 1, so_far ++ bits8(int_value), rest)
  end

  defp encode_binary(value, so_far) when bit_size(value) < 8 do
    bits = bit_size(value)
    << v :: size(bits) >> = value
    out = Integer.digits(v, 2)
    so_far ++ out ++ List.duplicate(0, 8 - length(out))
  end
  defp encode_binary(value, so_far) do
    << v :: size(8), rest :: bits >> = value
    out = Integer.digits(v, 2)
    out = so_far ++ out ++ List.duplicate(0, 8 - length(out))
    encode_binary(rest, out)
  end

  defp bits8(x) do
    digits = Integer.digits(x, 2)
    List.duplicate(0, 8 - length(digits)) ++ digits
  end

  defp last_bits(x) do
    x
    |> Integer.digits(2)
    |> Enum.reverse
    |> Enum.drop_while(fn(b) -> b == 0 end)
    |> Enum.reverse
  end

  defp fix_length(b, bit_count) when length(b) == bit_count, do: b
  defp fix_length(b, bit_count) when length(b) < bit_count do
    b ++ List.duplicate(0, bit_count - length(b))
  end
  defp fix_length(b, bit_count) when length(b) > bit_count do
    Enum.slice(b, (0..bit_count))
  end

  defp bits_to_binary(bits) do
    bits
    |> Enum.chunk(8, 8, [])
    |> Enum.map(fn(w) ->
      len = length(w)
      << Integer.undigits(w, 2) :: size(len) >>
    end)
    |> Enum.reduce(<<>>, fn(x, acc) -> << << acc :: bits >>, << x :: bits >> >> end)
  end
end
