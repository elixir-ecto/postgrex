defmodule Postgrex.Extensions.Float4 do
  @moduledoc false
  import Postgrex.BinaryUtils
  use Postgrex.BinaryExtension, send: "float4send"

  def encode(_, :NaN, _, _),
    do: <<127, 192, 0, 0>>
  def encode(_, :inf, _, _),
    do: <<127, 128, 0, 0>>
  def encode(_, :"-inf", _, _),
    do: <<255, 128, 0, 0>>
  def encode(_, n, _, _) when is_number(n),
    do: <<n :: float32>>
  def encode(type_info, value, _, _) do
    raise ArgumentError, Postgrex.Utils.encode_msg(type_info, value, "a float")
  end

  def decode(_, <<127, 192, 0, 0>>, _, _),
    do: :NaN
  def decode(_, <<127, 128, 0, 0>>, _, _),
    do: :inf
  def decode(_, <<255, 128, 0, 0>>, _, _),
    do: :"-inf"
  def decode(_, <<n :: float32>>, _, _),
    do: n
end
