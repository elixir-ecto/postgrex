defmodule Postgrex.Extensions.Multirange do
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
        encoder = &encode_value(&1, type)
        unquote(__MODULE__).encode(ranges, encoder)

      other, _, _ ->
        raise DBConnection.EncodeError, Postgrex.Utils.encode_msg(other, Postgrex.Multirange)
    end
  end

  def decode(_) do
    quote location: :keep do
      <<len::int32(), data::binary-size(len)>>, [_oid], [type] ->
        <<_num_ranges::int32(), ranges::binary>> = data
        # decode_list/2 defined by TypeModule
        decoder = &decode_list(&1, type)
        unquote(__MODULE__).decode(ranges, decoder, [])
    end
  end

  ## Helpers

  def encode(ranges, encoder) do
    encoded_ranges =
      Enum.map(ranges, fn range ->
        %{lower: lower, upper: upper} = range
        lower = if is_atom(lower), do: lower, else: encoder.(lower)
        upper = if is_atom(upper), do: upper, else: encoder.(upper)
        Postgrex.Extensions.Range.encode(range, lower, upper)
      end)

    num_ranges = length(ranges)
    iodata = [<<num_ranges::int32()>> | encoded_ranges]
    [<<IO.iodata_length(iodata)::int32()>> | iodata]
  end

  def decode(<<>>, _decoder, acc), do: %Postgrex.Multirange{ranges: Enum.reverse(acc)}

  def decode(<<len::int32(), encoded_range::binary-size(len), rest::binary>>, decoder, acc) do
    <<flags, data::binary>> = encoded_range

    decoded_range =
      case decoder.(data) do
        [upper, lower] ->
          Postgrex.Extensions.Range.decode(flags, [lower, upper])

        empty_or_one ->
          Postgrex.Extensions.Range.decode(flags, empty_or_one)
      end

    decode(rest, decoder, [decoded_range | acc])
  end
end
