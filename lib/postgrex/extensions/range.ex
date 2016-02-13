defmodule Postgrex.Extensions.Range do
  @moduledoc false
  alias Postgrex.TypeInfo
  alias Postgrex.Types
  import Postgrex.BinaryUtils
  use Postgrex.BinaryExtension, [] # send: "range_send" hard coded in types
  use Bitwise, only_operators: true

  @range_empty   0x01
  @range_lb_inc  0x02
  @range_ub_inc  0x04
  @range_lb_inf  0x08
  @range_ub_inf  0x10

  def encode(%TypeInfo{base_type: oid}, %Postgrex.Range{} = range, types, _),
    do: encode_range(range, oid, types)
  def encode(type_info, value, _, _) do
    raise ArgumentError,
      Postgrex.Utils.encode_msg(type_info, value, Postgrex.Range)
  end

  def decode(%TypeInfo{base_type: oid}, bin, types, _),
    do: decode_range(bin, oid, types)

  ## Helpers

  defp encode_range(%Postgrex.Range{lower: nil, upper: nil}, _oid, _types) do
    <<@range_empty>>
  end

  defp encode_range(range, oid, types) do
    flags = 0

    {flags, bin} =
      if range.lower == nil do
        {flags ||| @range_lb_inf, ""}
      else
        data = Types.encode(oid, range.lower, types)
        {flags, [<<IO.iodata_length(data)::int32>>, data]}
      end

    {flags, bin} =
    if range.upper == nil do
      {flags ||| @range_ub_inf, bin}
    else
      data = Types.encode(oid, range.upper, types)
      {flags, [bin, <<IO.iodata_length(data)::int32>>, data]}
    end

    flags =
      if range.lower_inclusive do
        flags ||| @range_lb_inc
      else
        flags
      end

    flags =
      if range.upper_inclusive do
        flags ||| @range_ub_inc
      else
        flags
      end

    [flags|bin]
  end

  defp decode_range(<<flags>>, _oid, _types) when (flags &&& @range_empty) != 0 do
    %Postgrex.Range{}
  end

  defp decode_range(<<flags, rest::binary>>, oid, types) do
    {lower, rest} =
      if (flags &&& @range_lb_inf) != 0 do
        {nil, rest}
      else
        <<size::int32, lower::binary(size), rest::binary>> = rest
        {Types.decode(oid, lower, types), rest}
      end

    {upper, rest} =
      if (flags &&& @range_ub_inf) != 0 do
        {nil, rest}
      else
        <<size::int32, upper::binary(size), rest::binary>> = rest
        {Types.decode(oid, upper, types), rest}
      end

    "" = rest
    lower_inclusive = (flags &&& @range_lb_inc) != 0
    upper_inclusive = (flags &&& @range_ub_inc) != 0
    %Postgrex.Range{lower: lower, upper: upper, lower_inclusive: lower_inclusive,
                    upper_inclusive: upper_inclusive}
  end
end
