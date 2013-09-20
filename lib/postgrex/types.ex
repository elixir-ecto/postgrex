defmodule Postgrex.Types do
  import Postgrex.BinaryUtils

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
