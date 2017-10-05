defmodule Postgrex.Extensions.Range do
  @moduledoc false
  import Postgrex.BinaryUtils, warn: false
  use Bitwise, only_operators: true
  @behaviour Postgrex.SuperExtension

  @range_empty   0x01
  @range_lb_inc  0x02
  @range_ub_inc  0x04
  @range_lb_inf  0x08
  @range_ub_inf  0x10

  def init(_), do: nil

  def matching(_),
    do: [send: "range_send"]

  def format(_),
    do: :super_binary

  def oids(%Postgrex.TypeInfo{base_type: base_oid}, _) do
    [base_oid]
  end

  def encode(_) do
    quote location: :keep do
      %Postgrex.Range{lower: lower, upper: upper} = range, [oid], [type] ->
        # encode_value/2 defined by TypeModule
        lower = encode_value(lower, type)
        upper = encode_value(upper, type)
        unquote(__MODULE__).encode(range, oid, lower, upper)
      other, _, _ ->
        raise ArgumentError,
          Postgrex.Utils.encode_msg(other, Postgrex.Range)
    end
  end

  def decode(_) do
    quote location: :keep do
      <<len :: int32, binary :: binary-size(len)>>, [oid], [type] ->
        <<flags, data :: binary>> = binary
        # decode_list/2 and @null defined by TypeModule
        case decode_list(data, type) do
          [upper, lower] ->
            unquote(__MODULE__).decode(flags, oid, [lower, upper], @null)
          empty_or_one ->
            unquote(__MODULE__).decode(flags, oid, empty_or_one, @null)
        end
    end
  end

  ## Helpers

  def encode(%Postgrex.Range{lower_inclusive: lower_inc,
                             upper_inclusive: upper_inc}, _oid, lower, upper) do
    flags = 0

    {flags, bin} =
      if lower == <<-1::int32>> do
        {flags ||| @range_lb_inf, ""}
      else
        {flags, lower}
      end

    {flags, bin} =
      if upper == <<-1::int32>> do
        {flags ||| @range_ub_inf, bin}
      else
        {flags, [bin | upper]}
      end

    flags =
      if lower_inc do
        flags ||| @range_lb_inc
      else
        flags
      end

    flags =
      if upper_inc do
        flags ||| @range_ub_inc
      else
        flags
      end

    [<<IO.iodata_length(bin)+1::int32>>, flags | bin]
  end

  def decode(flags, _oid, [], null) when (flags &&& @range_empty) != 0 do
    %Postgrex.Range{lower: null, upper: null}
  end

  def decode(flags, _oid, elems, null) do
    {lower, elems} =
      if (flags &&& @range_lb_inf) != 0 do
        {null, elems}
      else
        [lower | rest] = elems
        {lower, rest}
      end

    {upper, []} =
      if (flags &&& @range_ub_inf) != 0 do
        {null, elems}
      else
        [upper | rest] = elems
        {upper, rest}
      end

    lower_inclusive = (flags &&& @range_lb_inc) != 0
    upper_inclusive = (flags &&& @range_ub_inc) != 0
    %Postgrex.Range{lower: lower, upper: upper, lower_inclusive: lower_inclusive,
                    upper_inclusive: upper_inclusive}
  end
end
