defmodule Postgrex.Extensions.Binary do
  @moduledoc false

  alias Postgrex.TypeInfo
  alias Postgrex.Types
  import Postgrex.BinaryUtils
  require Decimal
  use Bitwise, only_operators: true

  @behaviour Postgrex.Extension

  @gd_epoch :calendar.date_to_gregorian_days({2000, 1, 1})
  @gs_epoch :calendar.datetime_to_gregorian_seconds({{2000, 1, 1}, {0, 0, 0}})
  @days_in_month 30
  @secs_in_day 24 * 60 * 60

  @date_max_year 5874897
  @timestamp_max_year 294276

  @numeric_base 10_000

  @range_empty   0x01
  @range_lb_inc  0x02
  @range_ub_inc  0x04
  @range_lb_inf  0x08
  @range_ub_inf  0x10

  @int2_range -32768..32767
  @int4_range -2147483648..2147483647
  @int8_range -9223372036854775808..9223372036854775807

  @senders ~w(boolsend bpcharsend textsend citextsend varcharsend byteasend
              int2send int4send int8send float4send float8send numeric_send
              uuid_send date_send time_send timetz_send timestamp_send
              timestamptz_send interval_send enum_send unknownsend hstore_send)

  def init(parameters, _opts),
    do: parameters["server_version"] |> Postgrex.Utils.version_to_int

  def matching(version) when version >= 90_100,
    do: [send: "void_send"] ++ matching(0)

  def matching(_),
    do: unquote(Enum.map(@senders, &{:send, &1}))

  def format(_),
    do: :binary

  ### ENCODING ###

  def encode(%TypeInfo{send: "void_send"}, :void, _, _),
    do: ""
  def encode(%TypeInfo{send: "boolsend"}, true, _, _),
    do: <<1>>
  def encode(%TypeInfo{send: "boolsend"}, false, _, _),
    do: <<0>>
  def encode(%TypeInfo{send: "bpcharsend"}, bin, _, _) when is_binary(bin),
    do: bin
  def encode(%TypeInfo{send: "textsend"}, bin, _, _) when is_binary(bin),
    do: bin
  def encode(%TypeInfo{send: "citextsend"}, bin, _, _) when is_binary(bin),
    do: bin
  def encode(%TypeInfo{send: "varcharsend"}, bin, _, _) when is_binary(bin),
    do: bin
  def encode(%TypeInfo{send: "byteasend"}, bin, _, _) when is_binary(bin),
    do: bin
  def encode(%TypeInfo{send: "enum_send"}, bin, _, _) when is_binary(bin),
    do: bin
  def encode(%TypeInfo{send: "unknownsend"}, bin, _, _) when is_binary(bin),
    do: bin
  def encode(%TypeInfo{send: "int2send"}, n, _, _)
    when is_integer(n) and n in @int2_range,
    do: <<n :: int16>>
  def encode(%TypeInfo{send: "int4send"}, n, _, _)
    when is_integer(n) and n in @int4_range,
    do: <<n :: int32>>
  def encode(%TypeInfo{send: "int8send"}, n, _, _)
    when is_integer(n) and n in @int8_range,
    do: <<n :: int64>>
  def encode(%TypeInfo{send: "float4send"}, :NaN, _, _),
    do: <<127, 192, 0, 0>>
  def encode(%TypeInfo{send: "float4send"}, :inf, _, _),
    do: <<127, 128, 0, 0>>
  def encode(%TypeInfo{send: "float4send"}, :"-inf", _, _),
    do: <<255, 128, 0, 0>>
  def encode(%TypeInfo{send: "float4send"}, n, _, _) when is_number(n),
    do: <<n :: float32>>
  def encode(%TypeInfo{send: "float8send"}, :NaN, _, _),
    do: <<127, 248, 0, 0, 0, 0, 0, 0>>
  def encode(%TypeInfo{send: "float8send"}, :inf, _, _),
    do: <<127, 240, 0, 0, 0, 0, 0, 0>>
  def encode(%TypeInfo{send: "float8send"}, :"-inf", _, _),
    do: <<255, 240, 0, 0, 0, 0, 0, 0>>
  def encode(%TypeInfo{send: "float8send"}, n, _, _) when is_number(n),
    do: <<n :: float64>>
  def encode(%TypeInfo{send: "numeric_send"}, %Decimal{} = n, _, _),
    do: encode_numeric(n)
  def encode(%TypeInfo{send: "uuid_send"}, <<_ :: binary(16)>> = bin, _, _),
    do: bin
  def encode(%TypeInfo{send: "date_send"}, date, _, _),
    do: encode_date(date)
  def encode(%TypeInfo{send: "time_send"}, time, _, _),
    do: encode_time(time)
  def encode(%TypeInfo{send: "timestamp_send"}, timestamp, _, _),
    do: encode_timestamp(timestamp)
  def encode(%TypeInfo{send: "timestamptz_send"}, timestamp, _, _),
    do: encode_timestamp(timestamp)
  def encode(%TypeInfo{send: "interval_send"}, interval, _, _),
    do: encode_interval(interval)
  def encode(%TypeInfo{send: "array_send", array_elem: elem_oid}, list, types, _) when is_list(list),
    do: encode_array(list, elem_oid, types)
  def encode(%TypeInfo{send: "record_send", comp_elems: elem_oids}, tuple, types, _) when is_tuple(tuple),
    do: encode_record(tuple, elem_oids, types)
  def encode(%TypeInfo{send: "range_send", base_type: oid}, %Postgrex.Range{} = range, types, _),
    do: encode_range(range, oid, types)
  def encode(%TypeInfo{send: "hstore_send"}, map, _, _),
    do: encode_hstore(map)

  defp encode_numeric(dec) do
    if Decimal.nan?(dec) do
      <<0 :: int16, 0 :: int16, 0xC000 :: uint16, 0 :: int16>>
    else
      string = Decimal.to_string(dec, :normal) |> :binary.bin_to_list

      if List.first(string) == ?- do
        [_|string] = string
        sign = 0x4000
      else
        sign = 0x0000
      end

      {int, float} = Enum.split_while(string, &(&1 != ?.))
      {weight, int_digits} = Enum.reverse(int) |> encode_numeric_int(0, [])

      if float != [] do
        [_|float] = float
        scale = length(float)
        float_digits = encode_numeric_float(float, [])
      else
        scale = 0
        float_digits = []
      end

      digits = int_digits ++ float_digits
      bin = for digit <- digits, into: "", do: <<digit :: uint16>>
      ndigits = div(byte_size(bin), 2)

      [<<ndigits :: int16, weight :: int16, sign :: uint16, scale :: int16>>, bin]
    end
  end

  defp encode_numeric_float([], [digit|acc]) do
    [pad_float(digit)|acc]
    |> trim_zeros
    |> Enum.reverse
  end

  defp encode_numeric_float(list, acc) do
    {list, rest} = Enum.split(list, 4)
    digit = List.to_integer(list)

    encode_numeric_float(rest, [digit|acc])
  end

  defp encode_numeric_int([], weight, acc) do
    {weight, acc}
  end

  defp encode_numeric_int(list, weight, acc) do
    {list, rest} = Enum.split(list, 4)
    digit = Enum.reverse(list) |> List.to_integer

    if rest != [], do: weight = weight + 1

    encode_numeric_int(rest, weight, [digit|acc])
  end

  defp trim_zeros([0|tail]), do: trim_zeros(tail)
  defp trim_zeros(list), do: list

  defp pad_float(0) do
    0
  end

  defp pad_float(num) do
    num10 = num*10
    if num10 >= @numeric_base do
      num
    else
      pad_float(num10)
    end
  end

  defp encode_date(%Postgrex.Date{year: year, month: month, day: day}) when year <= @date_max_year do
    date = {year, month, day}
    <<:calendar.date_to_gregorian_days(date) - @gd_epoch :: int32>>
  end

  defp encode_time(%Postgrex.Time{hour: hour, min: min, sec: sec, usec: usec})
    when hour in 0..23 and min in 0..59 and sec in 0..59 and usec in 0..999_999  do
    time = {hour, min, sec}
    <<:calendar.time_to_seconds(time) * 1_000_000 + usec :: int64>>
  end

  defp encode_timestamp(%Postgrex.Timestamp{year: year, month: month, day: day, hour: hour, min: min, sec: sec, usec: usec})
    when year <= @timestamp_max_year and hour in 0..23 and min in 0..59 and sec in 0..59 and usec in 0..999_999 do
    datetime = {{year, month, day}, {hour, min, sec}}
    secs = :calendar.datetime_to_gregorian_seconds(datetime) - @gs_epoch
    <<secs * 1_000_000 + usec :: int64>>
  end

  defp encode_interval(%Postgrex.Interval{months: months, days: days, secs: secs}) do
    microsecs = secs * 1_000_000
    <<microsecs :: int64, days :: int32, months :: int32>>
  end

  defp encode_array(list, elem_oid, types) do
    encoder = &Types.encode(elem_oid, &1, types)

    {data, ndims, lengths} = encode_array(list, 0, [], encoder)
    lengths = for len <- Enum.reverse(lengths), do: <<len :: int32, 1 :: int32>>
    [<<ndims :: int32, 0 :: int32, elem_oid :: int32>>, lengths, data]
  end

  defp encode_array([], ndims, lengths, _encoder) do
    {"", ndims, lengths}
  end

  defp encode_array([head|tail]=list, ndims, lengths, encoder)
      when is_list(head) do
    lengths = [length(list)|lengths]
    {data, ndims, lengths} = encode_array(head, ndims, lengths, encoder)
    [dimlength|_] = lengths

    rest = Enum.reduce(tail, [], fn sublist, acc ->
      {data, _, [len|_]} = encode_array(sublist, ndims, lengths, encoder)
      if len != dimlength do
        raise ArgumentError, message: "nested lists must have lists with matching lengths"
      end
      [acc|data]
    end)

    {[data|rest], ndims+1, lengths}
  end

  defp encode_array(list, ndims, lengths, encoder) do
    {data, length} =
      Enum.map_reduce(list, 0, fn
        nil, length ->
          {<<-1::int32>>, length + 1}
        elem, length ->
          data = encoder.(elem)
          data = [<<IO.iodata_length(data)::int32>>, data]
          {data, length + 1}
      end)
    {data, ndims+1, [length|lengths]}
  end

  defp encode_record(tuple, elem_oids, types) do
    list = Tuple.to_list(tuple)
    zipped = :lists.zip(list, elem_oids)

    {data, count} =
      Enum.map_reduce(zipped, 0, fn
        {nil, oid}, count ->
          {<<oid::int32, -1::int32>>, count + 1}
        {value, oid}, count ->
          data = Types.encode(oid, value, types)
          data = [<<oid::int32>>, <<IO.iodata_length(data)::int32>>, data]
          {data, count + 1}
      end)

    [<<count :: int32>>, data]
  end

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

  defp encode_hstore(hstore_map) when is_map(hstore_map) do
    keys_and_values = Enum.reduce hstore_map, "", fn ({key, value}, acc) ->
        [acc, encode_hstore_key(key), encode_hstore_value(value)]
    end
    :erlang.iolist_to_binary([<<Map.size(hstore_map)::int32>> | keys_and_values])
  end

  defp encode_hstore_key(key) when is_binary(key) do
    encode_hstore_value key
  end

  defp encode_hstore_key(key) when is_nil(key) do
    raise ArgumentError, message: "hstore keys cannot be nil!"
  end

  defp encode_hstore_value(nil) do
    <<-1::int32>>
  end

  defp encode_hstore_value(value) when is_binary(value) do
    value_byte_size = byte_size(value)
    <<value_byte_size::int32>> <> value
  end

  ### DECODING ###


  def decode(%TypeInfo{send: "void_send"}, "", _, _),
    do: :void
  def decode(%TypeInfo{send: "boolsend"}, <<1 :: int8>>, _, _),
    do: true
  def decode(%TypeInfo{send: "boolsend"}, <<0 :: int8>>, _, _),
    do: false
  def decode(%TypeInfo{send: "bpcharsend"}, bin, _, _),
    do: bin
  def decode(%TypeInfo{send: "textsend"}, bin, _, _),
    do: bin
  def decode(%TypeInfo{send: "citextsend"}, bin, _, _),
    do: bin
  def decode(%TypeInfo{send: "varcharsend"}, bin, _, _),
    do: bin
  def decode(%TypeInfo{send: "byteasend"}, bin, _, _),
    do: bin
  def decode(%TypeInfo{send: "enum_send"}, bin, _, _),
    do: bin
  def decode(%TypeInfo{send: "unknownsend"}, bin, _, _),
    do: bin
  def decode(%TypeInfo{send: "int2send"}, <<n :: int16>>, _, _),
    do: n
  def decode(%TypeInfo{send: "int4send"}, <<n :: int32>>, _, _),
    do: n
  def decode(%TypeInfo{send: "int8send"}, <<n :: int64>>, _, _),
    do: n
  def decode(%TypeInfo{send: "float4send"}, <<127, 192, 0, 0>>, _, _),
    do: :NaN
  def decode(%TypeInfo{send: "float4send"}, <<127, 128, 0, 0>>, _, _),
    do: :inf
  def decode(%TypeInfo{send: "float4send"}, <<255, 128, 0, 0>>, _, _),
    do: :"-inf"
  def decode(%TypeInfo{send: "float4send"}, <<n :: float32>>, _, _),
    do: n
  def decode(%TypeInfo{send: "float8send"}, <<127, 248, 0, 0, 0, 0, 0, 0>>, _, _),
    do: :NaN
  def decode(%TypeInfo{send: "float8send"}, <<127, 240, 0, 0, 0, 0, 0, 0>>, _, _),
    do: :inf
  def decode(%TypeInfo{send: "float8send"}, <<255, 240, 0, 0, 0, 0, 0, 0>>, _, _),
    do: :"-inf"
  def decode(%TypeInfo{send: "float8send"}, <<n :: float64>>, _, _),
    do: n
  def decode(%TypeInfo{send: "numeric_send"}, bin, _, _),
    do: decode_numeric(bin)
  def decode(%TypeInfo{send: "uuid_send"}, bin, _, _),
    do: bin
  def decode(%TypeInfo{send: "date_send"}, <<n :: int32>>, _, _),
    do: decode_date(n)
  def decode(%TypeInfo{send: "time_send"}, <<n :: int64>>, _, _),
    do: decode_time(n)
  def decode(%TypeInfo{send: "timetz_send"}, <<n :: int64, _tz :: int32>>, _, _),
    do: decode_time(n)
  def decode(%TypeInfo{send: "timestamp_send"}, <<n :: int64>>, _, _),
    do: decode_timestamp(n)
  def decode(%TypeInfo{send: "timestamptz_send"}, <<n :: int64>>, _, _),
    do: decode_timestamp(n)
  def decode(%TypeInfo{send: "interval_send"}, <<s :: int64, d :: int32, m :: int32>>, _, _),
    do: decode_interval(s, d, m)
  def decode(%TypeInfo{send: "array_send"}, bin, types, _),
    do: decode_array(bin, types)
  def decode(%TypeInfo{send: "record_send"}, bin, types, _),
    do: decode_record(bin, types)
  def decode(%TypeInfo{send: "range_send", base_type: oid}, bin, types, _),
    do: decode_range(bin, oid, types)
  def decode(%TypeInfo{send: "hstore_send"}, bin, _, _),
    do: decode_hstore(bin)

  defp decode_numeric(<<ndigits :: int16, weight :: int16, sign :: uint16, scale :: int16, tail :: binary>>) do
    decode_numeric(ndigits, weight, sign, scale, tail)
  end

  defp decode_numeric(0, _weight, 0xC000, _scale, "") do
    Decimal.new(1, :qNaN, 0)
  end

  defp decode_numeric(_num_digits, weight, sign, scale, bin) do
    {value, weight} = decode_numeric_int(bin, weight, 0)

    case sign do
      0x0000 -> sign = 1
      0x4000 -> sign = -1
    end

    {coef, exp} = scale(value, (weight+1)*4, -scale)
    Decimal.new(sign, coef, exp)
  end

  defp scale(coef, exp, scale) when scale == exp,
    do: {coef, exp}

  defp scale(coef, exp, scale) when scale > exp,
    do: scale(div(coef, 10), exp+1, scale)

  defp scale(coef, exp, scale) when scale < exp,
    do: scale(coef * 10, exp-1, scale)

  defp decode_numeric_int("", weight, acc), do: {acc, weight}

  defp decode_numeric_int(<<digit :: int16, tail :: binary>>, weight, acc) do
    acc = (acc * @numeric_base) + digit
    decode_numeric_int(tail, weight - 1, acc)
  end

  defp decode_date(days) do
    {year, month, day} = :calendar.gregorian_days_to_date(days + @gd_epoch)
    %Postgrex.Date{year: year, month: month, day: day}
  end

  defp decode_time(microsecs) do
    secs = div(microsecs, 1_000_000)
    usec = rem(microsecs, 1_000_000)
    {hour, min, sec} = :calendar.seconds_to_time(secs)
    %Postgrex.Time{hour: hour, min: min, sec: sec, usec: usec}
  end

  defp decode_timestamp(microsecs) do
    secs = div(microsecs, 1_000_000)
    usec = rem(microsecs, 1_000_000)
    {{year, month, day}, {hour, min, sec}} = :calendar.gregorian_seconds_to_datetime(secs + @gs_epoch)

    if year < 2000 and usec != 0 do
      sec = sec - 1
      usec = 1_000_000 + usec
    end

    %Postgrex.Timestamp{year: year, month: month, day: day, hour: hour, min: min, sec: sec, usec: usec}
  end

  defp decode_interval(microsecs, days, months) do
    secs = div(microsecs, 1_000_000)
    %Postgrex.Interval{months: months, days: days, secs: secs}
  end

  defp decode_array(<<ndims :: int32, _has_null :: int32, oid :: int32, rest :: binary>>,
                    types) do
    {dims, rest} = :erlang.split_binary(rest, ndims * 2 * 4)
    lengths = for <<len :: int32, _lbound :: int32 <- dims>>, do: len
    decoder = &Types.decode(oid, &1, types)

    {array, ""} = decode_array(rest, lengths, decoder)
    array
  end

  defp decode_array("", [], _decoder) do
    {[], ""}
  end

  defp decode_array(rest, [len], decoder) do
    array_elements(rest, len, [], decoder)
  end

  defp decode_array(rest, [len|lens], decoder) do
    Enum.map_reduce(1..len, rest, fn _, rest ->
      decode_array(rest, lens, decoder)
    end)
  end

  defp array_elements(rest, 0, acc, _decoder) do
    {Enum.reverse(acc), rest}
  end

  defp array_elements(<<-1 :: int32, rest :: binary>>, count, acc, decoder) do
    array_elements(rest, count-1, [nil|acc], decoder)
  end

  defp array_elements(<<size :: int32, elem :: binary(size), rest :: binary>>,
                       count, acc, decoder) do
    value = decoder.(elem)
    array_elements(rest, count-1, [value|acc], decoder)
  end

  defp decode_record(<<num :: int32, rest :: binary>>, types) do
    decoder = &Types.decode(&1, &2, types)
    record_elements(num, rest, decoder) |> List.to_tuple
  end

  defp record_elements(0, <<>>, _decoder) do
    []
  end

  defp record_elements(num, <<_oid :: int32, -1 :: int32, rest :: binary>>, decoder) do
    [nil | record_elements(num-1, rest, decoder)]
  end

  defp record_elements(num, <<oid :: int32, size :: int32, elem :: binary(size), rest :: binary>>,
                       decoder) do
    value = decoder.(oid, elem)
    [value | record_elements(num-1, rest, decoder)]
  end

  defp decode_range(<<flags>>, _oid, _types) when (flags &&& @range_empty) != 0 do
    %Postgrex.Range{}
  end

  defp decode_range(<<flags, rest::binary>>, oid, types) do
    if (flags &&& @range_lb_inf) != 0 do
      lower = nil
    else
      <<size::int32, lower::binary(size), rest::binary>> = rest
      lower = Postgrex.Types.decode(oid, lower, types)
    end

    if (flags &&& @range_ub_inf) != 0 do
      upper = nil
    else
      <<size::int32, upper::binary(size), rest::binary>> = rest
      upper = Postgrex.Types.decode(oid, upper, types)
    end

    "" = rest
    lower_inclusive = (flags &&& @range_lb_inc) != 0
    upper_inclusive = (flags &&& @range_ub_inc) != 0
    %Postgrex.Range{lower: lower, upper: upper, lower_inclusive: lower_inclusive,
                    upper_inclusive: upper_inclusive}
  end

  def decode_hstore(<<_length::int32, pairs::binary>>) do
    decode_hstore_payload(pairs, %{})
  end

  defp decode_hstore_payload(<<>>, acc) do
    acc
  end

  # in the case of a NULL value, there won't be a length
  defp decode_hstore_payload(<<key_length::int32, key::binary(key_length),
                             -1::int32, rest::binary>>, acc) do
    decode_hstore_payload(rest, Map.put(acc, key, nil))
  end

  defp decode_hstore_payload(<<key_length::int32, key::binary(key_length),
                        value_length::int32, value::binary(value_length), rest::binary>>, acc) do
    decode_hstore_payload(rest, Map.put(acc, key, value))
  end
end
