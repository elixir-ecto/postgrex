defmodule Postgrex.Extensions.Float4 do
  @moduledoc false
  import Postgrex.BinaryUtils
  use Postgrex.BinaryExtension, send: "float4send"

  def encode(_, :NaN, _, _),
    do: <<0::1, 255, 1::1, 0::22>>
  def encode(_, :inf, _, _),
    do: <<0::1, 255, 0::23>>
  def encode(_, :"-inf", _, _),
    do: <<1::1, 255, 0::23>>
  def encode(_, n, _, _) when is_number(n),
    do: <<n :: float32>>
  def encode(type_info, value, _, _) do
    raise ArgumentError, Postgrex.Utils.encode_msg(type_info, value, "a float")
  end

  def decode(_, value, _, _),
    do: decode(value)

  def decode(<<0::1, 255, 0::23>>),
    do: :inf
  def decode(<<1::1, 255, 0::23>>),
    do: :"-inf"
  def decode(<<_::1, 255, _::23>>),
    do: :NaN
  def decode(<<n :: float32>>),
    do: n

  def inline(_type_info, _types, _opts) do
    {__MODULE__, inline_encode(), inline_decode()}
  end

  defp inline_encode() do
    quote location: :keep do
      n when is_number(n) ->
        <<4::int32, n::float32>>
      :NaN ->
        <<4::int32, 0::1, 255, 1::1, 0::22>>
      :inf ->
        <<4::int32, 0::1, 255, 0::23>>
      :"-inf" ->
        <<4::int32, 1::1, 255, 0::23>>
      other ->
        raise ArgumentError, Postgrex.Utils.encode_msg(other, "a float")
    end
  end

  defp inline_decode() do
    quote location: :keep do
      <<4::int32, 0::1, 255, 0::23>> -> :inf
      <<4::int32, 1::1, 255, 0::23>> -> :"-inf"
      <<4::int32, _::1, 255, _::23>> -> :NaN
      <<4::int32, float::float32>>   -> float
    end
  end
end
