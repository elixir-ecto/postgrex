defmodule Postgrex.Types do
  @moduledoc false

  import Postgrex.BinaryUtils

  @types [ :bool, :bpchar, :text, :varchar, :bytea, :int2, :int4, :int8,
           :float4, :float8, :numeric, :date, :time, :timetz, :timestamp,
           :timestamptz, :interval, :unknown ]

  @gd_epoch :calendar.date_to_gregorian_days({ 2000, 1, 1 })
  @gs_epoch :calendar.datetime_to_gregorian_seconds({ { 2000, 1, 1 }, { 0, 0, 0 } })
  @days_in_month 30
  @secs_in_day 24 * 60 * 60
  @numeric_base 10_000

  def build_types(rows) do
    Enum.reduce(rows, HashDict.new, fn row, acc ->
      [oid, type, send, elem_oid, comp_oids] = row
      type = binary_to_atom(type)
      oid = binary_to_integer(oid)
      send_size = byte_size(send)
      elem = binary_to_integer(elem_oid)
      comp_oids = parse_oids(comp_oids)

      send =
        cond do
          String.ends_with?(send, "_send") ->
            :binary.part(send, 0, send_size - 5) |> binary_to_atom
          String.ends_with?(send, "send") ->
            :binary.part(send, 0, send_size - 4) |> binary_to_atom
          true ->
            nil
        end

      Dict.put(acc, oid, { send, type, elem, comp_oids })
    end)
  end

  defp parse_oids("{}") do
    []
  end

  defp parse_oids("{" <> rest) do
    parse_oids(rest, [])
  end

  defp parse_oids(bin, acc) do
    case Integer.parse(bin) do
      { int, "," <> rest } -> parse_oids(rest, [int|acc])
      { int, "}" } -> Enum.reverse([int|acc])
    end
  end

  def bootstrap_query do
    """
    SELECT t.oid, t.typname, t.typsend, t.typelem, ARRAY (
      SELECT a.atttypid
      FROM pg_attribute AS a
      WHERE a.attrelid = t.typrelid AND a.attnum > 0 AND NOT a.attisdropped
      ORDER BY a.attnum
    )
    FROM pg_type AS t
    """
  end

  def format(types, oid, formatter) do
    case Dict.fetch(types, oid) do
      { :ok, { sender, type, elem_oid, comp_oids } } ->
        cond do
          formatter && (format = formatter.(sender, type, oid)) ->
            format
          sender == :array and format(types, elem_oid, formatter) == :binary ->
            :binary
          sender == :record and type != :record and
          Enum.all?(comp_oids, &(format(types, &1, formatter) == :binary)) ->
            :binary
          binary_type?(sender) ->
            :binary
          true ->
            :text
        end

      :error ->
        :text
    end
  end

  def oid_to_type(types, oid) do
    case Dict.fetch(types, oid) do
      { :ok, { sender, type, _, _ } } -> { sender, type }
      :error -> nil
    end
  end

  def oid_to_elem(types, oid) do
    case Dict.fetch(types, oid) do
      { :ok, { _, _, elem, _ } } -> elem
      :error -> nil
    end
  end

  def oid_to_comp_oids(types, oid) do
    case Dict.fetch(types, oid) do
      { :ok, { _, _, _, oids } } -> oids
      :error -> nil
    end
  end

  def encode_value(sender, type, oid, { types, encoder, formatter }, default, value) do
    bin = if encoder, do: encoder.(sender, type, oid, default, value)

    result = case format(types, oid, formatter) do
      :binary ->
        if bin = bin || default.(value), do: { :binary, bin }
      :text when not nil?(bin) ->
        { :text, bin }
      :text when is_binary(value) ->
        { :text, value }
      _ ->
        nil
    end

    if nil?(result) do
      throw { :postgrex_encode, "unable to encode value `#{inspect value}` as type #{type}" }
    end

    result
  end

  def decode_value(sender, type, oid, format, decoder, default, bin) do
    decoded = if decoder, do: decoder.(sender, type, oid, format, default, bin)
    cond do
      decoded -> decoded
      format == :binary -> default.(bin)
      true -> bin
    end
  end

  def decode(:bool, _, << 1 :: int8 >>), do: true
  def decode(:bool, _, << 0 :: int8 >>), do: false
  def decode(:bpchar, _, bin), do: bin
  def decode(:text, _, bin), do: bin
  def decode(:varchar, _, bin), do: bin
  def decode(:bytea, _, bin), do: bin
  def decode(:int2, _, << n :: int16 >>), do: n
  def decode(:int4, _, << n :: int32 >>), do: n
  def decode(:int8, _, << n :: int64 >>), do: n
  def decode(:float4, _, << 127, 192, 0, 0 >>), do: :NaN
  def decode(:float4, _, << 127, 128, 0, 0 >>), do: :inf
  def decode(:float4, _, << 255, 128, 0, 0 >>), do: :"-inf"
  def decode(:float4, _, << n :: float32 >>), do: n
  def decode(:float8, _, << 127, 248, 0, 0, 0, 0, 0, 0 >>), do: :NaN
  def decode(:float8, _, << 127, 240, 0, 0, 0, 0, 0, 0 >>), do: :inf
  def decode(:float8, _, << 255, 240, 0, 0, 0, 0, 0, 0 >>), do: :"-inf"
  def decode(:float8, _, << n :: float64 >>), do: n
  def decode(:numeric, _, << ndigits :: int16, weight :: int16, sign :: uint16, scale :: int16, tail :: binary >>),
    do: decode_numeric(ndigits, weight, sign, scale, tail)
  def decode(:date, _, << n :: int32 >>), do: decode_date(n)
  def decode(:time, _, << n :: int64 >>), do: decode_time(n)
  def decode(:timetz, _, << n :: int64, _tz :: int32 >>), do: decode_time(n)
  def decode(:timestamp, _, << n :: int64 >>), do: decode_timestamp(n)
  def decode(:timestamptz, _, << n :: int64 >>), do: decode_timestamp(n)
  def decode(:interval, _, << s :: int64, d :: int32, m :: int32 >>), do: decode_interval(s, d, m)
  def decode(:array, extra, bin), do: decode_array(bin, extra)
  def decode(:record, extra, bin), do: decode_record(bin, extra)
  def decode(:unknown, _, bin), do: bin

  def encode(:bool, _, _, true), do: << 1 >>
  def encode(:bool, _, _, false), do: << 0 >>
  def encode(:bpchar, _, _, bin) when is_binary(bin), do: bin
  def encode(:text, _, _, bin) when is_binary(bin), do: bin
  def encode(:varchar, _, _, bin) when is_binary(bin), do: bin
  def encode(:bytea, _, _, bin) when is_binary(bin), do: bin
  def encode(:int2, _, _, n) when is_integer(n), do: << n :: int16 >>
  def encode(:int4, _, _, n) when is_integer(n), do: << n :: int32 >>
  def encode(:int8, _, _, n) when is_integer(n), do: << n :: int64 >>
  def encode(:float4, _, _, :NaN), do: << 127, 192, 0, 0 >>
  def encode(:float4, _, _, :inf), do: << 127, 128, 0, 0 >>
  def encode(:float4, _, _, :"-inf"), do: << 255, 128, 0, 0 >>
  def encode(:float4, _, _, n) when is_number(n), do: << n :: float32 >>
  def encode(:float8, _, _, :NaN), do: << 127, 248, 0, 0, 0, 0, 0, 0 >>
  def encode(:float8, _, _, :inf), do: << 127, 240, 0, 0, 0, 0, 0, 0 >>
  def encode(:float8, _, _, :"-inf"), do: << 255, 240, 0, 0, 0, 0, 0, 0 >>
  def encode(:float8, _, _, n) when is_number(n), do: << n :: float64 >>
  def encode(:numeric, _, _, n), do: encode_numeric(n)
  def encode(:date, _, _, date), do: encode_date(date)
  def encode(:time, _, _, time), do: encode_time(time)
  def encode(:timestamp, _, _, timestamp), do: encode_timestamp(timestamp)
  def encode(:timestamptz, _, _, timestamp), do: encode_timestamp(timestamp)
  def encode(:interval, _, _, interval), do: encode_interval(interval)
  def encode(:array, oid, extra, list) when is_list(list), do: encode_array(list, oid, extra)
  def encode(:record, oid, extra, tuple) when is_tuple(tuple), do: encode_record(tuple, oid, extra)
  def encode(_, _, _, _), do: nil

  Enum.each(@types, fn type ->
    defp binary_type?(unquote(type)), do: true
  end)
  defp binary_type?(_), do: false

  ### decode helpers ###

  defp decode_numeric(0, 0, 0xC000, 0, ""), do: :NaN

  defp decode_numeric(num_digits, weight, sign, _scale, tail) do
    ^num_digits = div(byte_size(tail), 2)
    { value, weight } = decode_numeric_int(tail, weight, 0)
    value = decode_numeric_float(value, weight)

    case sign do
      0x0000 -> value
      0x4000 -> -value
    end
  end

  defp decode_numeric_int("", weight, acc), do: { acc, weight }

  defp decode_numeric_int(<< digit :: int16, tail :: binary >>, weight, acc) do
    acc = (acc * @numeric_base) + digit
    decode_numeric_int(tail, weight - 1, acc)
  end

  defp decode_numeric_float(value, -1), do: value

  defp decode_numeric_float(value, weight) when weight < 0 do
    value = value / @numeric_base
    decode_numeric_float(value, weight + 1)
  end

  defp decode_numeric_float(value, weight) when weight >= 0 do
    value = value * @numeric_base
    decode_numeric_float(value, weight - 1)
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
    { months, days, div(microsecs, 1_000_000) }
  end

  defp decode_array(<< ndims :: int32, _has_null :: int32, oid :: int32, rest :: binary >>,
                    { types, _ } = extra) do
    { dims, rest } = :erlang.split_binary(rest, ndims * 2 * 4)
    lengths = lc << len :: int32, _lbound :: int32 >> inbits dims, do: len
    { sender, type } = oid_to_type(types, oid)
    default = &decode(sender, extra, &1)

    { array, "" } = decode_array(rest, { sender, type, oid }, extra, default, lengths)
    array
  end

  defp decode_array("", _type, _extra, _default, []) do
    { [], "" }
  end

  defp decode_array(rest, type, extra, default, [len]) do
    array_elements(rest, type, extra, default, [], len)
  end

  defp decode_array(rest, type, extra, default, [len|lens]) do
    Enum.map_reduce(1..len, rest, fn _, rest ->
      decode_array(rest, type, extra, default, lens)
    end)
  end

  defp array_elements(rest, _type, _extra, _default, acc, 0) do
    { Enum.reverse(acc), rest }
  end

  defp array_elements(<< -1 :: int32, rest :: binary >>, type, extra, default, acc, count) do
    array_elements(rest, type, extra, default, [nil|acc], count-1)
  end

  defp array_elements(<< length :: int32, elem :: binary(length), rest :: binary >>,
                       type_info, extra, default, acc, count) do
    { sender, type, oid } = type_info
    { _, decoder } = extra
    value = decode_value(sender, type, oid, :binary, decoder, default, elem)
    array_elements(rest, type_info, extra, default, [value|acc], count-1)
  end

  defp decode_record(<< num :: int32, rest :: binary >>, extra) do
    record_elements(num, rest, extra) |> list_to_tuple
  end

  defp record_elements(0, <<>>, _types) do
    []
  end

  defp record_elements(num, << _oid :: int32, -1 :: int32, rest :: binary >>, extra) do
    [ nil | record_elements(num-1, rest, extra) ]
  end

  defp record_elements(num, << oid :: int32, length :: int32, elem :: binary(length), rest :: binary >>,
                       { types, decoder } = extra) do
    { sender, type } = oid_to_type(types, oid)
    default = &decode(sender, extra, &1)
    value = decode_value(sender, type, oid, :binary, decoder, default, elem)
    [ value | record_elements(num-1, rest, extra) ]
  end

  ### encode helpers ###

  defp encode_numeric(:NaN), do: << 0 :: int16, 0 :: int16, 0xC000 :: uint16, 0 :: int16 >>

  defp encode_numeric(number) do
    sign = cond do
      number < 0  -> 0x4000
      number >= 0 -> 0x0000
    end

    number = abs(number)
    int_part = trunc(number)
    float_part = number - int_part

    { weight, digits } = cond do
      int_part == 0 and float_part == 0 ->
        { 0, [] }
      int_part == 0 ->
        { -1, encode_numeric_float(float_part, []) }
      true ->
        { weight, digits } = encode_numeric_int(int_part, 0, [])
        { weight, digits ++ encode_numeric_float(float_part, []) }
    end

    bin = bc digit inlist digits, do: << digit :: uint16 >>
    ndigits = div(byte_size(bin), 2)

    << ndigits :: int16, weight :: int16, sign :: uint16, 0 :: int16, bin :: binary >>
  end

  defp encode_numeric_float(number, acc) do
    cond do
      number == 0 ->
        Enum.reverse(acc)
      true ->
        number = number * @numeric_base
        digit = trunc(number)
        rest = number - digit
        encode_numeric_float(rest, [digit|acc])
    end
  end

  defp encode_numeric_int(number, weight, acc) do
    cond do
      number == 0 ->
        { weight, acc }
      number < @numeric_base ->
        { weight, [number|acc] }
      true ->
        rest = div(number, @numeric_base)
        digit = number - rest * @numeric_base
        encode_numeric_int(rest, weight+1, [digit|acc])
    end
  end

  defp encode_date(date) do
    << :calendar.date_to_gregorian_days(date) - @gd_epoch :: int32 >>
  end

  defp encode_time(time) do
    << :calendar.time_to_seconds(time) * 1_000_000 :: int64 >>
  end

  defp encode_timestamp(timestamp) do
    secs = :calendar.datetime_to_gregorian_seconds(timestamp) - @gs_epoch
    << secs * 1_000_000 :: int64 >>
  end

  defp encode_interval({ months, days, secs }) do
    microsecs = secs * 1_000_000
    << microsecs :: int64, days :: int32, months :: int32 >>
  end

  defp encode_array(list, oid, { types, _, _ } = extra) do
    elem_oid = oid_to_elem(types, oid)
    { elem_sender, elem_type } = oid_to_type(types, elem_oid)
    type = { elem_sender, elem_type, elem_oid }
    default = &encode(elem_sender, elem_oid, extra, &1)

    { data, ndims, lengths } = encode_array(list, type, extra, default, 0, [])
    bin = iolist_to_binary(data)
    lengths = bc len inlist Enum.reverse(lengths), do: << len :: int32, 1 :: int32 >>
    << ndims :: int32, 0 :: int32, elem_oid :: int32, lengths :: binary, bin :: binary >>
  end

  defp encode_array([], _type, _extra, _default, ndims, lengths) do
    { "", ndims, lengths }
  end

  defp encode_array([head|tail]=list, type, extra, default, ndims, lengths)
      when is_list(head) do
    lengths = [length(list)|lengths]
    { data, ndims, lengths } = encode_array(head, type, extra, default, ndims, lengths)
    [dimlength|_] = lengths

    rest = Enum.map(tail, fn sublist ->
      { data, _, [len|_] } = encode_array(sublist, type, extra, default, ndims, lengths)
      if len != dimlength do
        throw { :postgrex_encode, "nested lists must have lists with matching lengths" }
      end
      data
    end)

    { [data|rest], ndims+1, lengths }
  end

  defp encode_array(list, { sender, type, oid }, extra, default, ndims, lengths) do
    { data, length } = Enum.map_reduce(list, 0, fn elem, length ->
      { :binary, bin } = encode_value(sender, type, oid, extra, default, elem)
      { << byte_size(bin) :: int32, bin :: binary >>, length + 1 }
    end)
    { data, ndims+1, [length|lengths] }
  end

  defp encode_record(tuple, oid, { types, _, _ } = extra) do
    list = tuple_to_list(tuple)
    comp_oids = oid_to_comp_oids(types, oid)
    zipped = :lists.zip(list, comp_oids)

    { data, count } = Enum.map_reduce(zipped, 0, fn { value, oid }, count ->
      { sender, type } = oid_to_type(types, oid)
      default = &encode(sender, oid, extra, &1)
      { :binary, bin } = encode_value(sender, type, oid, extra, default, value)
      { << oid :: int32, byte_size(bin) :: int32, bin :: binary >>, count + 1 }
    end)

    << count :: int32, iolist_to_binary(data) :: binary >>
  end
end
