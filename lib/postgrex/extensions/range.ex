defmodule Postgrex.Extensions.Range do
  @moduledoc false
  import Postgrex.BinaryUtils, warn: false
  import Bitwise
  @behaviour Postgrex.SuperExtension

  @range_empty 0x01
  @range_lb_inc 0x02
  @range_ub_inc 0x04
  @range_lb_inf 0x08
  @range_ub_inf 0x10

  def init(_), do: nil

  def matching(_), do: [send: "range_send"]

  def format(_), do: :super_binary

  def oids(%Postgrex.TypeInfo{base_type: base_oid}, _) do
    [base_oid]
  end

  def encode(_) do
    quote location: :keep do
      %Postgrex.Range{lower: lower, upper: upper} = range, [_oid], [type] ->
        # encode_value/2 defined by TypeModule
        lower = if is_atom(lower), do: lower, else: encode_value(lower, type)
        upper = if is_atom(upper), do: upper, else: encode_value(upper, type)
        unquote(__MODULE__).encode(range, lower, upper)

      other, _, _ ->
        raise DBConnection.EncodeError, Postgrex.Utils.encode_msg(other, Postgrex.Range)
    end
  end

  def decode(_) do
    quote location: :keep do
      <<len::int32(), binary::binary-size(len)>>, [_oid], [type] ->
        <<flags, data::binary>> = binary

        # decode_list/2 defined by TypeModule
        sub_type_with_mod =
          case type do
            {extension, sub_oids, sub_types} -> {extension, sub_oids, sub_types, nil}
            extension -> {extension, nil}
          end

        case decode_list(data, sub_type_with_mod) do
          [upper, lower] ->
            unquote(__MODULE__).decode(flags, [lower, upper])

          empty_or_one ->
            unquote(__MODULE__).decode(flags, empty_or_one)
        end
    end
  end

  ## Helpers

  def encode(_range, :empty, :empty) do
    [<<1::int32(), @range_empty>>]
  end

  def encode(%{lower_inclusive: lower_inc, upper_inclusive: upper_inc}, lower, upper) do
    flags = 0

    {flags, data} =
      if is_atom(lower) do
        {flags ||| @range_lb_inf, []}
      else
        {flags, lower}
      end

    {flags, data} =
      if is_atom(upper) do
        {flags ||| @range_ub_inf, data}
      else
        {flags, [data | upper]}
      end

    flags =
      case lower_inc do
        true -> flags ||| @range_lb_inc
        false -> flags
      end

    flags =
      case upper_inc do
        true -> flags ||| @range_ub_inc
        false -> flags
      end

    [<<IO.iodata_length(data) + 1::int32()>>, flags | data]
  end

  def decode(flags, []) when (flags &&& @range_empty) != 0 do
    %Postgrex.Range{
      lower: :empty,
      upper: :empty,
      lower_inclusive: false,
      upper_inclusive: false
    }
  end

  def decode(flags, elems) do
    {lower, elems} =
      if (flags &&& @range_lb_inf) != 0 do
        {:unbound, elems}
      else
        [lower | rest] = elems
        {lower, rest}
      end

    {upper, []} =
      if (flags &&& @range_ub_inf) != 0 do
        {:unbound, elems}
      else
        [upper | rest] = elems
        {upper, rest}
      end

    lower_inclusive? = (flags &&& @range_lb_inc) != 0
    upper_inclusive? = (flags &&& @range_ub_inc) != 0

    %Postgrex.Range{
      lower: lower,
      upper: upper,
      lower_inclusive: lower_inclusive?,
      upper_inclusive: upper_inclusive?
    }
  end
end
