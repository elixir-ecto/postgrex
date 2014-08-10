defmodule Postgrex.Types do
  @moduledoc false

  alias Postgrex.TypeInfo
  import Postgrex.BinaryUtils
  require Decimal

  @types [ "bool", "bpchar", "text", "varchar", "bytea", "int2", "int4", "int8",
           "float4", "float8", "numeric", "date", "time", "timetz", "timestamp",
           "timestamptz", "interval" ]

  @gd_epoch :calendar.date_to_gregorian_days({2000, 1, 1})
  @gs_epoch :calendar.datetime_to_gregorian_seconds({{2000, 1, 1}, {0, 0, 0}})
  @days_in_month 30
  @secs_in_day 24 * 60 * 60
  @numeric_base 10_000

  def build_types(rows) do
    Enum.reduce(rows, HashDict.new, fn row, acc ->
      [oid, type, send, array_oid, comp_oids] = row
      oid = String.to_integer(oid)
      send_size = byte_size(send)
      array_oid = String.to_integer(array_oid)
      comp_oids = parse_oids(comp_oids)

      send =
        cond do
          String.ends_with?(send, "_send") ->
            :binary.part(send, 0, send_size - 5)
          String.ends_with?(send, "send") ->
            :binary.part(send, 0, send_size - 4)
          true ->
            nil
        end

      info = %TypeInfo{oid: oid, sender: send, type: type, array_elem: array_oid,
                       comp_elems: comp_oids}
      Dict.put(acc, oid, info)
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
      {int, "," <> rest} -> parse_oids(rest, [int|acc])
      {int, "}"} -> Enum.reverse([int|acc])
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
      {:ok, %TypeInfo{sender: sender, type: type, array_elem: array_oid, comp_elems: comp_oids} = info} ->
        cond do
          formatter && (format = formatter.(info)) ->
            format
          sender == "array" and format(types, array_oid, formatter) == :binary ->
            :binary
          sender == "record" and type != "record" and
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

  def encode_value(%TypeInfo{} = info, {types, encoder, formatter}, default, value) do
    bin = if encoder, do: encoder.(info, default, value)

    result = case format(types, info.oid, formatter) do
      :binary ->
        if bin = bin || default.(value), do: {:binary, bin}
      :text when not nil?(bin) ->
        {:text, bin}
      :text when is_binary(value) ->
        {:text, value}
      _ ->
        nil
    end

    if nil?(result) do
      throw {:postgrex_encode, "unable to encode value `#{inspect value}` as type #{info.type}"}
    end

    result
  end

  def decode_value(info, format, decoder, default, bin) do
    decoded = if decoder, do: decoder.(info, format, default, bin)
    cond do
      decoded -> decoded
      true -> default.(bin)
    end
  end

  def decode_binary(%TypeInfo{sender: "bool"}, _, <<1 :: int8>>),
    do: true
  def decode_binary(%TypeInfo{sender: "bool"}, _, <<0 :: int8>>),
    do: false
  def decode_binary(%TypeInfo{sender: "bpchar"}, _, bin),
    do: bin
  def decode_binary(%TypeInfo{sender: "text"}, _, bin),
    do: bin
  def decode_binary(%TypeInfo{sender: "varchar"}, _, bin),
    do: bin
  def decode_binary(%TypeInfo{sender: "bytea"}, _, bin),
    do: bin
  def decode_binary(%TypeInfo{sender: "int2"}, _, <<n :: int16>>),
    do: n
  def decode_binary(%TypeInfo{sender: "int4"}, _, <<n :: int32>>),
    do: n
  def decode_binary(%TypeInfo{sender: "int8"}, _, <<n :: int64>>),
    do: n
  def decode_binary(%TypeInfo{sender: "float4"}, _, <<127, 192, 0, 0>>),
    do: :NaN
  def decode_binary(%TypeInfo{sender: "float4"}, _, <<127, 128, 0, 0>>),
    do: :inf
  def decode_binary(%TypeInfo{sender: "float4"}, _, <<255, 128, 0, 0>>),
    do: :"-inf"
  def decode_binary(%TypeInfo{sender: "float4"}, _, <<n :: float32>>),
    do: n
  def decode_binary(%TypeInfo{sender: "float8"}, _, <<127, 248, 0, 0, 0, 0, 0, 0>>),
    do: :NaN
  def decode_binary(%TypeInfo{sender: "float8"}, _, <<127, 240, 0, 0, 0, 0, 0, 0>>),
    do: :inf
  def decode_binary(%TypeInfo{sender: "float8"}, _, <<255, 240, 0, 0, 0, 0, 0, 0>>),
    do: :"-inf"
  def decode_binary(%TypeInfo{sender: "float8"}, _, <<n :: float64>>),
    do: n
  def decode_binary(%TypeInfo{sender: "numeric"}, _, <<ndigits :: int16, weight :: int16, sign :: uint16, scale :: int16, tail :: binary>>),
    do: decode_numeric(ndigits, weight, sign, scale, tail)
  def decode_binary(%TypeInfo{sender: "date"}, _, <<n :: int32>>),
    do: decode_date(n)
  def decode_binary(%TypeInfo{sender: "time"}, _, <<n :: int64>>),
    do: decode_time(n)
  def decode_binary(%TypeInfo{sender: "timetz"}, _, <<n :: int64, _tz :: int32>>),
    do: decode_time(n)
  def decode_binary(%TypeInfo{sender: "timestamp"}, _, <<n :: int64>>),
    do: decode_timestamp(n)
  def decode_binary(%TypeInfo{sender: "timestamptz"}, _, <<n :: int64>>),
    do: decode_timestamp(n)
  def decode_binary(%TypeInfo{sender: "interval"}, _, <<s :: int64, d :: int32, m :: int32>>),
    do: decode_interval(s, d, m)
  def decode_binary(%TypeInfo{sender: "array"}, extra, bin),
    do: decode_array(bin, extra)
  def decode_binary(%TypeInfo{sender: "record"}, extra, bin),
    do: decode_record(bin, extra)
  def decode_binary(%TypeInfo{}, _, _),
    do: nil

  def decode_text(%TypeInfo{type: "void"}, _, ""),
    do: :void
  def decode_text(%TypeInfo{}, _, text),
    do: text

  def encode(%TypeInfo{sender: "bool"}, _, true),
    do: <<1>>
  def encode(%TypeInfo{sender: "bool"}, _, false),
    do: <<0>>
  def encode(%TypeInfo{sender: "bpchar"}, _, bin) when is_binary(bin),
    do: bin
  def encode(%TypeInfo{sender: "text"}, _, bin) when is_binary(bin),
    do: bin
  def encode(%TypeInfo{sender: "varchar"}, _, bin) when is_binary(bin),
    do: bin
  def encode(%TypeInfo{sender: "bytea"}, _, bin) when is_binary(bin),
    do: bin
  def encode(%TypeInfo{sender: "int2"}, _, n) when is_integer(n),
    do: <<n :: int16>>
  def encode(%TypeInfo{sender: "int4"}, _, n) when is_integer(n),
    do: <<n :: int32>>
  def encode(%TypeInfo{sender: "int8"}, _, n) when is_integer(n),
    do: <<n :: int64>>
  def encode(%TypeInfo{sender: "float4"}, _, :NaN),
    do: <<127, 192, 0, 0>>
  def encode(%TypeInfo{sender: "float4"}, _, :inf),
    do: <<127, 128, 0, 0>>
  def encode(%TypeInfo{sender: "float4"}, _, :"-inf"),
    do: <<255, 128, 0, 0>>
  def encode(%TypeInfo{sender: "float4"}, _, n) when is_number(n),
    do: <<n :: float32>>
  def encode(%TypeInfo{sender: "float8"}, _, :NaN),
    do: <<127, 248, 0, 0, 0, 0, 0, 0>>
  def encode(%TypeInfo{sender: "float8"}, _, :inf),
    do: <<127, 240, 0, 0, 0, 0, 0, 0>>
  def encode(%TypeInfo{sender: "float8"}, _, :"-inf"),
    do: <<255, 240, 0, 0, 0, 0, 0, 0>>
  def encode(%TypeInfo{sender: "float8"}, _, n) when is_number(n),
    do: <<n :: float64>>
  def encode(%TypeInfo{sender: "numeric"}, _, n),
    do: encode_numeric(n)
  def encode(%TypeInfo{sender: "date"}, _, date),
    do: encode_date(date)
  def encode(%TypeInfo{sender: "time"}, _, time),
    do: encode_time(time)
  def encode(%TypeInfo{sender: "timestamp"}, _, timestamp),
    do: encode_timestamp(timestamp)
  def encode(%TypeInfo{sender: "timestamptz"}, _, timestamp),
    do: encode_timestamp(timestamp)
  def encode(%TypeInfo{sender: "interval"}, _, interval),
    do: encode_interval(interval)
  def encode(%TypeInfo{sender: "array", oid: oid}, extra, list) when is_list(list),
    do: encode_array(list, oid, extra)
  def encode(%TypeInfo{sender: "record", oid: oid}, extra, tuple) when is_tuple(tuple),
    do: encode_record(tuple, oid, extra)
  def encode(%TypeInfo{}, _, _),
    do: nil

  Enum.each(@types, fn type ->
    defp binary_type?(unquote(type)), do: true
  end)
  defp binary_type?(_), do: false

  ### decode helpers ###

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
                    {types, _} = extra) do
    {dims, rest} = :erlang.split_binary(rest, ndims * 2 * 4)
    lengths = for <<len :: int32, _lbound :: int32 <- dims>>, do: len
    info = Dict.fetch!(types, oid)
    default = &decode_binary(info, extra, &1)

    {array, ""} = decode_array(rest, info, extra, default, lengths)
    array
  end

  defp decode_array("", _info, _extra, _default, []) do
    {[], ""}
  end

  defp decode_array(rest, info, extra, default, [len]) do
    array_elements(rest, info, extra, default, [], len)
  end

  defp decode_array(rest, info, extra, default, [len|lens]) do
    Enum.map_reduce(1..len, rest, fn _, rest ->
      decode_array(rest, info, extra, default, lens)
    end)
  end

  defp array_elements(rest, _info, _extra, _default, acc, 0) do
    {Enum.reverse(acc), rest}
  end

  defp array_elements(<<-1 :: int32, rest :: binary>>, info, extra, default, acc, count) do
    array_elements(rest, info, extra, default, [nil|acc], count-1)
  end

  defp array_elements(<<length :: int32, elem :: binary(length), rest :: binary>>,
                       info, extra, default, acc, count) do
    {_, decoder} = extra
    value = decode_value(info, :binary, decoder, default, elem)
    array_elements(rest, info, extra, default, [value|acc], count-1)
  end

  defp decode_record(<<num :: int32, rest :: binary>>, extra) do
    record_elements(num, rest, extra) |> List.to_tuple
  end

  defp record_elements(0, <<>>, _extra) do
    []
  end

  defp record_elements(num, <<_oid :: int32, -1 :: int32, rest :: binary>>, extra) do
    [ nil | record_elements(num-1, rest, extra) ]
  end

  defp record_elements(num, <<oid :: int32, length :: int32, elem :: binary(length), rest :: binary>>,
                       {types, decoder} = extra) do
    info = Dict.fetch!(types, oid)
    default = &decode_binary(info, extra, &1)
    value = decode_value(info, :binary, decoder, default, elem)
    [ value | record_elements(num-1, rest, extra) ]
  end

  ### encode helpers ###

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

  defp encode_array(list, oid, {types, _, _} = extra) do
    %TypeInfo{array_elem: elem_oid} = Dict.fetch!(types, oid)
    info = Dict.fetch!(types, elem_oid)
    default = &encode(info, extra, &1)

    {data, ndims, lengths} = encode_array(list, info, extra, default, 0, [])
    lengths = for len <- Enum.reverse(lengths), do: <<len :: int32, 1 :: int32>>
    [<<ndims :: int32, 0 :: int32, elem_oid :: int32>>, lengths, data]
  end

  defp encode_array([], _info, _extra, _default, ndims, lengths) do
    {"", ndims, lengths}
  end

  defp encode_array([head|tail]=list, info, extra, default, ndims, lengths)
      when is_list(head) do
    lengths = [length(list)|lengths]
    {data, ndims, lengths} = encode_array(head, info, extra, default, ndims, lengths)
    [dimlength|_] = lengths

    rest = Enum.reduce(tail, [], fn sublist, acc ->
      {data, _, [len|_]} = encode_array(sublist, info, extra, default, ndims, lengths)
      if len != dimlength do
        throw {:postgrex_encode, "nested lists must have lists with matching lengths"}
      end
      [acc|data]
    end)

    {[data|rest], ndims+1, lengths}
  end

  defp encode_array(list, info, extra, default, ndims, lengths) do
    {data, length} = Enum.map_reduce(list, 0, fn elem, length ->
      {:binary, data} = encode_value(info, extra, default, elem)
      {[<<IO.iodata_length(data) :: int32>>, data], length + 1}
    end)
    {data, ndims+1, [length|lengths]}
  end

  defp encode_record(tuple, oid, {types, _, _} = extra) do
    list = Tuple.to_list(tuple)
    %TypeInfo{comp_elems: comp_oids} = Dict.fetch!(types, oid)
    zipped = :lists.zip(list, comp_oids)

    {data, count} = Enum.map_reduce(zipped, 0, fn {value, oid}, count ->
      info = Dict.fetch!(types, oid)
      default = &encode(info, extra, &1)
      {:binary, data} = encode_value(info, extra, default, value)
      {[<<oid :: int32, IO.iodata_length(data) :: int32>>, data], count + 1}
    end)

    [<<count :: int32>>, data]
  end
end
