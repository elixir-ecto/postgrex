defmodule Postgrex.Extensions.Multirange do
  @moduledoc false

  import Postgrex.BinaryUtils, warn: false

  @behaviour Postgrex.SuperExtension

  def init(_), do: nil

  def matching(_state), do: [send: "multirange_send"]

  def format(_), do: :super_binary

  def oids(%Postgrex.TypeInfo{base_type: base_oid}, _) do
    [base_oid]
  end

  def encode(_) do
    quote location: :keep do
      %Postgrex.Multirange{ranges: ranges}, [_oid], [type] when is_list(ranges) ->
        # encode_value/2 defined by TypeModule
        bound_encoder = &encode_value(&1, type)
        unquote(__MODULE__).encode(ranges, bound_encoder)

      other, _, _ ->
        raise DBConnection.EncodeError, Postgrex.Utils.encode_msg(other, Postgrex.Multirange)
    end
  end

  def decode(_) do
    quote location: :keep do
      <<len::int32(), data::binary-size(len)>>, [_oid], [type] ->
        <<_num_ranges::int32(), ranges::binary>> = data

        # decode_list/2 defined by TypeModule
        sub_type_with_mod =
          case type do
            {extension, sub_oids, sub_types} -> {extension, sub_oids, sub_types, nil}
            extension -> {extension, nil}
          end

        bound_decoder = &decode_list(&1, sub_type_with_mod)
        unquote(__MODULE__).decode(ranges, bound_decoder, [])
    end
  end

  ## Helpers

  def encode(ranges, bound_encoder) do
    encoded_ranges =
      Enum.map(ranges, fn range ->
        %{lower: lower, upper: upper} = range
        lower = if is_atom(lower), do: lower, else: bound_encoder.(lower)
        upper = if is_atom(upper), do: upper, else: bound_encoder.(upper)
        Postgrex.Extensions.Range.encode(range, lower, upper)
      end)

    num_ranges = length(ranges)
    iodata = [<<num_ranges::int32()>> | encoded_ranges]
    [<<IO.iodata_length(iodata)::int32()>> | iodata]
  end

  def decode(<<>>, _bound_decoder, acc), do: %Postgrex.Multirange{ranges: Enum.reverse(acc)}

  def decode(<<len::int32(), encoded_range::binary-size(len), rest::binary>>, bound_decoder, acc) do
    <<flags, data::binary>> = encoded_range

    decoded_range =
      case bound_decoder.(data) do
        [upper, lower] ->
          Postgrex.Extensions.Range.decode(flags, [lower, upper])

        empty_or_one ->
          Postgrex.Extensions.Range.decode(flags, empty_or_one)
      end

    decode(rest, bound_decoder, [decoded_range | acc])
  end
end
