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

  def decode(_, <<0::1, 2047::11, 0::52>>, _, _),
    do: :inf
  def decode(_, <<1::1, 2047::11, 0::52>>, _, _),
    do: :"-inf"
  def decode(_, <<_::1, 2047::11, _::52>>, _, _),
    do: :NaN
  def decode(_, <<n :: float64>>, _, _),
    do: n
end
