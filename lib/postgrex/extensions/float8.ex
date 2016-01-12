defmodule Postgrex.Extensions.Float8 do
  @moduledoc false
  import Postgrex.BinaryUtils
  use Postgrex.BinaryExtension, send: "float8send"

  def encode(_, :NaN, _, _),
    do: <<127, 248, 0, 0, 0, 0, 0, 0>>
  def encode(_, :inf, _, _),
    do: <<127, 240, 0, 0, 0, 0, 0, 0>>
  def encode(_, :"-inf", _, _),
    do: <<255, 240, 0, 0, 0, 0, 0, 0>>
  def encode(_, n, _, _) when is_number(n),
    do: <<n :: float64>>
  def encode(type_info, value, _, _) do
    raise ArgumentError, Postgrex.Utils.encode_msg(type_info, value, "a float")
  end

  def decode(_, <<127, 248, 0, 0, 0, 0, 0, 0>>, _, _),
    do: :NaN
  def decode(_, <<127, 240, 0, 0, 0, 0, 0, 0>>, _, _),
    do: :inf
  def decode(_, <<255, 240, 0, 0, 0, 0, 0, 0>>, _, _),
    do: :"-inf"
  def decode(_, <<n :: float64>>, _, _),
    do: n
end
