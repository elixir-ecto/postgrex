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

  def decode(_, <<0::1, 255, 0::23>>, _, _),
    do: :inf
  def decode(_, <<1::1, 255, 0::23>>, _, _),
    do: :"-inf"
  def decode(_, <<_::1, 255, _::23>>, _, _),
    do: :NaN
  def decode(_, <<n :: float32>>, _, _),
    do: n
end
