defmodule Postgrex.Extensions.Float8 do
  @moduledoc false
  import Postgrex.BinaryUtils
  use Postgrex.BinaryExtension, send: "float8send"

  def encode(_, :NaN, _, _),
    do: <<0::1, 2047::11, 1::1, 0::51>>
  def encode(_, :inf, _, _),
    do: <<0::1, 2047::11, 0::52>>
  def encode(_, :"-inf", _, _),
    do: <<1::1, 2047::11, 0::52>>
  def encode(_, n, _, _) when is_number(n),
    do: <<n :: float64>>
  def encode(type_info, value, _, _) do
    raise ArgumentError, Postgrex.Utils.encode_msg(type_info, value, "a float")
  end

  def decode(_, value, _, _),
    do: decode(value)

  def decode(<<0::1, 2047::11, 0::52>>),
    do: :inf
  def decode(<<1::1, 2047::11, 0::52>>),
    do: :"-inf"
  def decode(<<_::1, 2047::11, _::52>>),
    do: :NaN
  def decode(<<n :: float64>>),
    do: n

  def inline(_type_info, _types, _opts) do
    {__MODULE__, inline_encode(), inline_decode()}
  end

  defp inline_encode() do
    quote location: :keep do
      n when is_number(n) ->
        <<8 :: int32, n :: float64>>
      :NaN ->
        <<8 :: int32, 0::1, 2047::11, 1::1, 0::51>>
      :inf ->
        <<8 :: int32, 0::1, 2047::11, 0::52>>
      :"-inf" ->
        <<8 :: int32, 1::1, 2047::11, 0::52>>
      other ->
        raise ArgumentError, Postgrex.Utils.encode_msg(other, "a float")
    end
  end

  defp inline_decode() do
    quote location: :keep do
      <<8::int32, 0::1, 2047::11, 0::52>> -> :inf
      <<8::int32, 1::1, 2047::11, 0::52>> -> :"-inf"
      <<8::int32, _::1, 2047::11, _::52>> -> :NaN
      <<8::int32, float::float64>>        -> float
    end
  end
end
