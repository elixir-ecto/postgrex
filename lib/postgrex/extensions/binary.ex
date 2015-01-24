defmodule Postgrex.Extensions.Binary do
  @moduledoc false

  alias Postgrex.TypeInfo
  import Postgrex.BinaryUtils
  require Decimal
  use Bitwise, only_operators: true

  @behaviour Postgrex.Extension

  @gd_epoch :calendar.date_to_gregorian_days({2000, 1, 1})
  @gs_epoch :calendar.datetime_to_gregorian_seconds({{2000, 1, 1}, {0, 0, 0}})
  @days_in_month 30
  @secs_in_day 24 * 60 * 60
  @numeric_base 10_000
  @default_flag 0x02 ||| 0x04

  @senders ~w(boolsend bpcharsend textsend varcharsend byteasend int2send
              int4send int8send float4send float8send numeric_send uuid_send
              date_send time_send timetz_send timestamp_send timestamptz_send
              interval_send array_send record_send range_send unknownsend)

  def matching,
    do: unquote(Enum.map(@senders, &{:send, &1}))

  def format,
    do: :binary

  ### ENCODING ###

  def encode(%TypeInfo{send: "boolsend"}, true, _),
    do: <<1>>
  def encode(%TypeInfo{send: "boolsend"}, false, _),
    do: <<0>>
  def encode(%TypeInfo{send: "bpcharsend"}, bin, _) when is_binary(bin),
    do: bin
  def encode(%TypeInfo{send: "textsend"}, bin, _) when is_binary(bin),
    do: bin
  def encode(%TypeInfo{send: "citextsend"}, bin, _) when is_binary(bin),
    do: bin
  def encode(%TypeInfo{send: "varcharsend"}, bin, _) when is_binary(bin),
    do: bin
  def encode(%TypeInfo{send: "byteasend"}, bin, _) when is_binary(bin),
    do: bin
  def encode(%TypeInfo{send: "unknownsend"}, bin, _) when is_binary(bin),
    do: bin
  def encode(%TypeInfo{send: "int2send"}, n, _) when is_integer(n),
    do: <<n :: int16>>
  def encode(%TypeInfo{send: "int4send"}, n, _) when is_integer(n),
    do: <<n :: int32>>
  def encode(%TypeInfo{send: "int8send"}, n, _) when is_integer(n),
    do: <<n :: int64>>
  def encode(%TypeInfo{send: "float4send"}, :NaN, _),
    do: <<127, 192, 0, 0>>
  def encode(%TypeInfo{send: "float4send"}, :inf, _),
    do: <<127, 128, 0, 0>>
  def encode(%TypeInfo{send: "float4send"}, :"-inf", _),
    do: <<255, 128, 0, 0>>
  def encode(%TypeInfo{send: "float4send"}, n, _) when is_number(n),
    do: <<n :: float32>>
  def encode(%TypeInfo{send: "float8send"}, :NaN, _),
    do: <<127, 248, 0, 0, 0, 0, 0, 0>>
  def encode(%TypeInfo{send: "float8send"}, :inf, _),
    do: <<127, 240, 0, 0, 0, 0, 0, 0>>
  def encode(%TypeInfo{send: "float8send"}, :"-inf", _),
    do: <<255, 240, 0, 0, 0, 0, 0, 0>>
  def encode(%TypeInfo{send: "float8send"}, n, _) when is_number(n),
    do: <<n :: float64>>
  def encode(%TypeInfo{send: "numeric_send"}, n, _),
    do: encode_numeric(n)
  def encode(%TypeInfo{send: "uuid_send"}, <<_ :: binary(16)>> = bin, _),
    do: bin
  def encode(%TypeInfo{send: "date_send"}, date, _),
    do: encode_date(date)
  def encode(%TypeInfo{send: "time_send"}, time, _),
    do: encode_time(time)
  def encode(%TypeInfo{send: "timestamp_send"}, timestamp, _),
    do: encode_timestamp(timestamp)
  def encode(%TypeInfo{send: "timestamptz_send"}, timestamp, _),
    do: encode_timestamp(timestamp)
  def encode(%TypeInfo{send: "interval_send"}, interval, _),
    do: encode_interval(interval)
  def encode(%TypeInfo{send: "array_send", array_elem: elem_oid}, list, encoder) when is_list(list),
    do: encode_array(list, elem_oid, encoder)
  def encode(%TypeInfo{send: "record_send", comp_elems: elem_oids}, tuple, encoder) when is_tuple(tuple),
    do: encode_record(tuple, elem_oids, encoder)
  def encode(%TypeInfo{send: "range_send", type: type}, tuple, _),
    do: encode_range(type, tuple)
  def encode(%TypeInfo{type: type}, value, _),
    do: throw {:postgrex_encode, "unable to encode value `#{inspect value}` for type `#{type}`"}

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

  defp encode_date(date) do
    <<:calendar.date_to_gregorian_days(date) - @gd_epoch :: int32>>
  end

  defp encode_time(time) do
    <<:calendar.time_to_seconds(time) * 1_000_000 :: int64>>
  end

  defp encode_timestamp(timestamp) do
    secs = :calendar.datetime_to_gregorian_seconds(timestamp) - @gs_epoch
    <<secs * 1_000_000 :: int64>>
  end

  defp encode_interval({months, days, secs}) do
    microsecs = secs * 1_000_000
    <<microsecs :: int64, days :: int32, months :: int32>>
  end

  defp encode_array(list, elem_oid, encoder) do
    encoder = &encoder.(elem_oid, &1)

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
        throw {:postgrex_encode, "nested lists must have lists with matching lengths"}
      end
      [acc|data]
    end)

    {[data|rest], ndims+1, lengths}
  end

  defp encode_array(list, ndims, lengths, encoder) do
    {data, length} = Enum.map_reduce(list, 0, fn elem, length ->
      data = encoder.(elem)
      {data, length + 1}
    end)
    {data, ndims+1, [length|lengths]}
  end

  defp encode_record(tuple, elem_oids, encoder) do
    list = Tuple.to_list(tuple)
    zipped = :lists.zip(list, elem_oids)

    {data, count} = Enum.map_reduce(zipped, 0, fn {value, oid}, count ->
      data = encoder.(oid, value)
      {[<<oid :: int32>>, data], count + 1}
    end)

    [<<count :: int32>>, data]
  end

  # TODO: Encode ranges generically with typbasetype
  defp encode_range("int4range", tuple) do
    encode_range(tuple, &(<<&1 :: int32>>))
  end

  defp encode_range("int8range", tuple) do
    encode_range(tuple, &(<<&1 :: int64>>))
  end

  defp encode_range(type, tuple) when type in ["tsrange", "tstzrange"] do
    encode_range(tuple, &encode_timestamp/1)
  end

  defp encode_range("daterange", tuple) do
    encode_range(tuple, &encode_date/1)
  end

  defp encode_range("numrange", tuple) do
    encode_range(tuple, fn(bound) ->
      [meta, bin] = encode_numeric(bound)
      meta <> bin
    end)
  end

  defp encode_range(tuple, fun) when is_function(fun) do
    flag = range_flag(tuple)

    case tuple do
      {:"-inf", upper} ->
        flag <> encode_bound(upper, fun)
      {lower, :inf} ->
        flag <> encode_bound(lower, fun)
      {lower, upper} ->
        flag <> encode_bound(lower, fun) <> encode_bound(upper, fun)
    end
  end

  defp encode_bound(value, fun) do
    bin = apply(fun, [value])
    <<byte_size(bin) :: int32>> <> bin
  end

  defp range_flag({:"-inf", _upper}) do
    <<@default_flag ||| 0x08>> # Set lower bound infinity flag
  end

  defp range_flag({_lower, :inf}) do
    <<@default_flag ||| 0x10>> # Set upper bound infinity flag
  end

  defp range_flag({_lower, _upper}) do
    <<@default_flag>> # Inclusive lower and upper bounds
  end

  ### DECODING ###

  def decode(%TypeInfo{send: "boolsend"}, <<1 :: int8>>, _),
    do: true
  def decode(%TypeInfo{send: "boolsend"}, <<0 :: int8>>, _),
    do: false
  def decode(%TypeInfo{send: "bpcharsend"}, bin, _),
    do: bin
  def decode(%TypeInfo{send: "textsend"}, bin, _),
    do: bin
  def decode(%TypeInfo{send: "citextsend"}, bin, _),
    do: bin
  def decode(%TypeInfo{send: "varcharsend"}, bin, _),
    do: bin
  def decode(%TypeInfo{send: "byteasend"}, bin, _),
    do: bin
  def decode(%TypeInfo{send: "unknownsend"}, bin, _),
    do: bin
  def decode(%TypeInfo{send: "int2send"}, <<n :: int16>>, _),
    do: n
  def decode(%TypeInfo{send: "int4send"}, <<n :: int32>>, _),
    do: n
  def decode(%TypeInfo{send: "int8send"}, <<n :: int64>>, _),
    do: n
  def decode(%TypeInfo{send: "float4send"}, <<127, 192, 0, 0>>, _),
    do: :NaN
  def decode(%TypeInfo{send: "float4send"}, <<127, 128, 0, 0>>, _),
    do: :inf
  def decode(%TypeInfo{send: "float4send"}, <<255, 128, 0, 0>>, _),
    do: :"-inf"
  def decode(%TypeInfo{send: "float4send"}, <<n :: float32>>, _),
    do: n
  def decode(%TypeInfo{send: "float8send"}, <<127, 248, 0, 0, 0, 0, 0, 0>>, _),
    do: :NaN
  def decode(%TypeInfo{send: "float8send"}, <<127, 240, 0, 0, 0, 0, 0, 0>>, _),
    do: :inf
  def decode(%TypeInfo{send: "float8send"}, <<255, 240, 0, 0, 0, 0, 0, 0>>, _),
    do: :"-inf"
  def decode(%TypeInfo{send: "float8send"}, <<n :: float64>>, _),
    do: n
  def decode(%TypeInfo{send: "numeric_send"}, bin, _),
    do: decode_numeric(bin)
  def decode(%TypeInfo{send: "uuid_send"}, bin, _),
    do: bin
  def decode(%TypeInfo{send: "date_send"}, <<n :: int32>>, _),
    do: decode_date(n)
  def decode(%TypeInfo{send: "time_send"}, <<n :: int64>>, _),
    do: decode_time(n)
  def decode(%TypeInfo{send: "timetz_send"}, <<n :: int64, _tz :: int32>>, _),
    do: decode_time(n)
  def decode(%TypeInfo{send: "timestamp_send"}, <<n :: int64>>, _),
    do: decode_timestamp(n)
  def decode(%TypeInfo{send: "timestamptz_send"}, <<n :: int64>>, _),
    do: decode_timestamp(n)
  def decode(%TypeInfo{send: "interval_send"}, <<s :: int64, d :: int32, m :: int32>>, _),
    do: decode_interval(s, d, m)
  def decode(%TypeInfo{send: "array_send"}, bin, decoder),
    do: decode_array(bin, decoder)
  def decode(%TypeInfo{send: "record_send"}, bin, decoder),
    do: decode_record(bin, decoder)
  def decode(%TypeInfo{send: "range_send", type: type}, <<flags, payload :: binary>>, _decoder),
    do: decode_range(type, flags, payload)

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
    :calendar.gregorian_days_to_date(days + @gd_epoch)
  end

  defp decode_time(microsecs) do
    secs = div(microsecs, 1_000_000)
    :calendar.seconds_to_time(secs)
  end

  defp decode_timestamp(microsecs) do
    secs = div(microsecs, 1_000_000)
    :calendar.gregorian_seconds_to_datetime(secs + @gs_epoch)
  end

  defp decode_interval(microsecs, days, months) do
    {months, days, div(microsecs, 1_000_000)}
  end

  defp decode_array(<<ndims :: int32, _has_null :: int32, oid :: int32, rest :: binary>>,
                    decoder) do
    {dims, rest} = :erlang.split_binary(rest, ndims * 2 * 4)
    lengths = for <<len :: int32, _lbound :: int32 <- dims>>, do: len
    decoder = &decoder.(oid, &1)

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

  defp array_elements(<<length :: int32, elem :: binary(length), rest :: binary>>,
                       count, acc, decoder) do
    value = decoder.(elem)
    array_elements(rest, count-1, [value|acc], decoder)
  end

  defp decode_record(<<num :: int32, rest :: binary>>, decoder) do
    record_elements(num, rest, decoder) |> List.to_tuple
  end

  defp record_elements(0, <<>>, _decoder) do
    []
  end

  defp record_elements(num, <<_oid :: int32, -1 :: int32, rest :: binary>>, decoder) do
    [nil | record_elements(num-1, rest, decoder)]
  end

  defp record_elements(num, <<oid :: int32, length :: int32, elem :: binary(length), rest :: binary>>,
                       decoder) do
    value = decoder.(oid, elem)
    [value | record_elements(num-1, rest, decoder)]
  end

  # TODO: Decode ranges generically with typbasetype
  defp decode_range("numrange", _flags, <<len :: int32, lower_bound :: binary(len), len2 :: int32, upper_bound :: binary(len2)>>) do
    {decode_numeric(lower_bound), decode_numeric(upper_bound)}
  end

  defp decode_range("numrange", flags, <<len :: int32, single_value :: binary(len)>>) do
    case check_infinite(flags) do
      :lower ->
        {:"-inf", decode_numeric(single_value)}
      :upper ->
        {decode_numeric(single_value), :inf}
    end
  end

  defp decode_range(type, _flags, <<_ :: int32, lower_bound :: int32, _ :: int32, upper_bound :: int32>>) do
    case type do
      "int4range" ->
        {lower_bound, upper_bound - 1}
      "daterange" ->
        {decode_date(lower_bound), decode_date(upper_bound - 1)}
    end
  end

  defp decode_range(type, flags, <<_ :: int32, single_value :: int32>>) do
    case {type, check_infinite(flags)} do
      {"int4range", :lower} ->
        {:"-inf", single_value - 1}
      {"daterange", :lower} ->
        {:"-inf", decode_date(single_value - 1)}
      {"int4range", :upper} ->
        {single_value, :inf}
      {"daterange", :upper} ->
        {decode_date(single_value), :inf}
    end
  end

  defp decode_range("int8range", _flags, <<_ :: int32, lower_bound :: int64, _ :: int32, upper_bound :: int64>>) do
     {lower_bound, upper_bound - 1}
  end

  defp decode_range(type, _flags, <<_ :: int32, lower_bound :: int64, _ :: int32, upper_bound :: int64>>) when type in ["tsrange", "tstzrange"] do
    {decode_timestamp(lower_bound), decode_timestamp(upper_bound)}
  end

  defp decode_range("int8range", flags, <<_ :: int32, single_value :: int64>>) do
    case check_infinite(flags) do
      :lower ->
        {:"-inf", single_value - 1}
      :upper ->
        {single_value, :inf}
    end
  end

  defp decode_range(type, flags, <<_ :: int32, single_value :: int64>>) when type in ["tsrange", "tstzrange"] do
    case check_infinite(flags) do
      :lower ->
        {:"-inf", decode_timestamp(single_value)}
      :upper ->
        {decode_timestamp(single_value), :inf}
    end
  end

  defp check_infinite(flags) do
    cond do
      (flags &&& 0x8)  != 0 ->
        :lower
      (flags &&& 0x10) != 0 ->
        :upper
    end
  end
end
