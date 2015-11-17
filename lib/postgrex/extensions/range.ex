defmodule Postgrex.Extensions.Range do
  @moduledoc false
  alias Postgrex.TypeInfo
  alias Postgrex.Types
  import Postgrex.BinaryUtils
  use Postgrex.BinaryExtension, send: "range_send"
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

    if range.lower == nil do
      flags = flags ||| @range_lb_inf
      bin = ""
    else
      data = Types.encode(oid, range.lower, types)
      bin = [<<IO.iodata_length(data)::int32>>, data]
    end

    if range.upper == nil do
      flags = flags ||| @range_ub_inf
    else
      data = Types.encode(oid, range.upper, types)
      bin = [bin, <<IO.iodata_length(data)::int32>>, data]
    end

    if range.lower_inclusive do
      flags = flags ||| @range_lb_inc
    end

    if range.upper_inclusive do
      flags = flags ||| @range_ub_inc
    end

    [flags|bin]
  end

  defp decode_range(<<flags>>, _oid, _types) when (flags &&& @range_empty) != 0 do
    %Postgrex.Range{}
  end

  defp decode_range(<<flags, rest::binary>>, oid, types) do
    if (flags &&& @range_lb_inf) != 0 do
      lower = nil
    else
      <<size::int32, lower::binary(size), rest::binary>> = rest
      lower = Types.decode(oid, lower, types)
    end

    if (flags &&& @range_ub_inf) != 0 do
      upper = nil
    else
      <<size::int32, upper::binary(size), rest::binary>> = rest
      upper = Types.decode(oid, upper, types)
    end

    "" = rest
    lower_inclusive = (flags &&& @range_lb_inc) != 0
    upper_inclusive = (flags &&& @range_ub_inc) != 0
    %Postgrex.Range{lower: lower, upper: upper, lower_inclusive: lower_inclusive,
                    upper_inclusive: upper_inclusive}
  end
end
