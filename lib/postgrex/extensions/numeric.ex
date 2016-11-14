defmodule Postgrex.Extensions.Numeric do
  @moduledoc false
  import Postgrex.BinaryUtils
  use Postgrex.BinaryExtension, send: "numeric_send"

  @numeric_base 10_000
  @numeric_float_pad String.length(Integer.to_string(@numeric_base))

  def encode(_, n, _, _) when is_number(n),
    do: encode_numeric(Decimal.new(n))
  def encode(_, %Decimal{} = n, _, _),
    do: encode_numeric(n)
  def encode(type_info, value, _, _) do
    raise ArgumentError,
      Postgrex.Utils.encode_msg(type_info, value, {"a number", Decimal})
  end

  def decode(_, bin, _, _),
    do: decode_numeric(bin)

  ## Helpers

  defp encode_numeric(dec) do
    if Decimal.nan?(dec) do
      <<0 :: int16, 0 :: int16, 0xC000 :: uint16, 0 :: int16>>
    else
      string = Decimal.to_string(dec, :normal) |> :binary.bin_to_list

      {string, sign} =
        case string do
          [?-|tail] -> {tail, 0x4000}
          _ -> {string, 0x0000}
        end

      {int, float} = Enum.split_while(string, &(&1 != ?.))
      {weight, int_digits} = Enum.reverse(int) |> encode_numeric_int(0, [])

      {scale, float_digits} =
        if float != [] do
          [_|float] = float
          scale = length(float)
          {scale,
           pad_to_numeric_base(float, scale) |> encode_numeric_float([])}
        else
          {0, []}
        end

      digits = int_digits ++ float_digits
      bin = for digit <- digits, into: "", do: <<digit :: uint16>>
      ndigits = div(byte_size(bin), 2)

      [<<ndigits :: int16, weight :: int16, sign :: uint16, scale :: int16>> | bin]
    end
  end

  defp pad_to_numeric_base(float, scale) do
    if scale < @numeric_float_pad do
      float ++ List.duplicate(?0, @numeric_float_pad - scale)
    else
      float
    end
  end

  defp encode_numeric_float([], digits) do
    digits
    |> trim_zeros
    |> Enum.reverse
  end

  defp encode_numeric_float(list, acc) do
    {list, rest} = Enum.split(list, 4)

    digit =
      list
      |> Kernel.++(List.duplicate(?0, 4 - length(list)))
      |> List.to_integer

    encode_numeric_float(rest, [digit|acc])
  end

  defp encode_numeric_int([], weight, acc) do
    {weight, acc}
  end

  defp encode_numeric_int(list, weight, acc) do
    {list, rest} = Enum.split(list, 4)
    digit = Enum.reverse(list) |> List.to_integer
    weight = if rest != [], do: weight + 1, else: weight
    encode_numeric_int(rest, weight, [digit|acc])
  end

  defp trim_zeros([0|tail]), do: trim_zeros(tail)
  defp trim_zeros(list), do: list

  defp decode_numeric(<<ndigits :: int16, weight :: int16, sign :: uint16, scale :: int16, tail :: binary>>) do
    decode_numeric(ndigits, weight, sign, scale, tail)
  end

  defp decode_numeric(0, _weight, 0xC000, _scale, "") do
    Decimal.new(1, :qNaN, 0)
  end

  defp decode_numeric(_num_digits, weight, sign, scale, bin) do
    {value, weight} = decode_numeric_int(bin, weight, 0)

    sign =
      case sign do
        0x0000 -> 1
        0x4000 -> -1
      end

    coef = scale(value, (weight + 1) * 4 + scale)
    Decimal.new(sign, coef, -scale)
  end

  defp scale(coef, 0), do: coef
  defp scale(coef, diff) when diff < 0, do: div(coef, pow10(-diff))
  defp scale(coef, diff) when diff > 0, do: coef * pow10(diff)

  Enum.reduce 0..100, 1, fn x, acc ->
    defp pow10(unquote(x)), do: unquote(acc)
    acc * 10
  end

  defp pow10(num) when num > 100, do: pow10(100) * pow10(num-100)

  defp decode_numeric_int("", weight, acc), do: {acc, weight}

  defp decode_numeric_int(<<digit :: int16, tail :: binary>>, weight, acc) do
    acc = (acc * @numeric_base) + digit
    decode_numeric_int(tail, weight - 1, acc)
  end
end
