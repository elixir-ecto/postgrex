defmodule Postgrex.Extensions.Numeric do
  @moduledoc false
  import Postgrex.BinaryUtils, warn: false
  use Postgrex.BinaryExtension, send: "numeric_send"

  def encode(_) do
    quote location: :keep do
      %Decimal{} = decimal ->
        data = unquote(__MODULE__).encode_numeric(decimal)
        [<<IO.iodata_length(data)::int32>> | data]
      n when is_number(n) ->
        data = unquote(__MODULE__).encode_numeric(Decimal.new(n))
        [<<IO.iodata_length(data)::int32>> | data]
    end
  end

  def decode(_) do
    quote location: :keep do
      <<len :: int32, data :: binary-size(len)>> ->
        unquote(__MODULE__).decode_numeric(data)
    end
  end

  ## Helpers

  def encode_numeric(%Decimal{coef: coef}) when coef in [:qNaN, :sNaN] do
    <<0 :: int16, 0 :: int16, 0xC000 :: uint16, 0 :: int16>>
  end

  def encode_numeric(%Decimal{sign: sign, coef: coef, exp: exp}) do
    sign = encode_sign(sign)
    scale = -exp

    {integer, float, scale} = split_parts(coef, scale)
    integer_digits = encode_digits(integer, [])
    float_digits = encode_float(float, scale)
    digits = integer_digits ++ float_digits

    num_digits = length(digits)
    weight = max(length(integer_digits) - 1, 0)

    bin = for digit <- digits, into: "", do: <<digit :: uint16>>
    [<<num_digits :: int16, weight :: int16, sign :: uint16, scale :: int16>> | bin]
  end

  defp encode_sign(1), do: 0x0000
  defp encode_sign(-1), do: 0x4000

  defp split_parts(coef, scale) when scale >= 0 do
    integer_base = pow10(scale)
    {div(coef, integer_base), rem(coef, integer_base), scale}
  end
  defp split_parts(coef, scale) when scale < 0 do
    integer_base = pow10(-scale)
    {coef * integer_base, 0, 0}
  end

  defp encode_float(float, scale) do
    {float_prefix, float_suffix} = float_padding(float, scale)
    leading_zeros = List.duplicate(0, float_prefix)
    float = float * pow10(float_suffix)
    leading_zeros ++ encode_digits(float, [])
  end

  defp float_padding(num, scale) when num < 10000 and scale > 0 do
    prefix = div(scale, 4)
    suffix = rem(scale, 4)
    if suffix == 0 do
      {prefix - 1, 0}
    else
      {prefix, 4 - suffix}
    end
  end
  defp float_padding(num, _scale) when num < 10, do: {0, 3}
  defp float_padding(num, _scale) when num < 100, do: {0, 2}
  defp float_padding(num, _scale) when num < 1000, do: {0, 1}
  defp float_padding(num, _scale) when num < 10000, do: {0, 0}
  defp float_padding(num, scale), do: float_padding(div(num, 10000), scale - 4)

  defp encode_digits(coef, digits) when coef < 10000 do
    [coef|digits]
  end
  defp encode_digits(coef, digits) do
    digit = rem(coef, 10000)
    coef = div(coef, 10000)
    encode_digits(coef, [digit|digits])
  end

  def decode_numeric(<<ndigits :: int16, weight :: int16, sign :: uint16, scale :: int16, tail :: binary>>) do
    decode_numeric(ndigits, weight, sign, scale, tail)
  end

  defp decode_numeric(0, _weight, 0xC000, _scale, "") do
    Decimal.new(1, :qNaN, 0)
  end

  defp decode_numeric(_num_digits, weight, sign, scale, bin) do
    {value, weight} = decode_numeric_int(bin, weight, 0)
    sign = decode_sign(sign)
    coef = scale(value, (weight + 1) * 4 + scale)
    Decimal.new(sign, coef, -scale)
  end

  defp decode_sign(0x0000), do: 1
  defp decode_sign(0x4000), do: -1

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
    acc = (acc * 10000) + digit
    decode_numeric_int(tail, weight - 1, acc)
  end
end
