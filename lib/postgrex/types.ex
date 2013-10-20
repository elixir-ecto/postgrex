defmodule Postgrex.Types do
  @moduledoc false

  import Postgrex.BinaryUtils

  @types [ :bool, :bpchar, :text, :varchar, :bytea, :int2, :int4, :int8,
           :float4, :float8, :date, :time, :timetz, :timestamp, :timestamptz,
           :interval, :array, :unknown ]

  @gd_epoch :calendar.date_to_gregorian_days({ 2000, 1, 1 })
  @gs_epoch :calendar.datetime_to_gregorian_seconds({ { 2000, 1, 1 }, { 0, 0, 0 } })
  @days_in_month 30
  @secs_in_day 24 * 60 * 60

  def build_types(rows) do
    Enum.reduce(rows, HashDict.new, fn row, acc ->
      [oid, type, send, elem_oid] = row
      type = binary_to_atom(type)
      oid = binary_to_integer(oid)
      send_size = byte_size(send)
      elem = binary_to_integer(elem_oid)

      send =
        cond do
          String.ends_with?(send, "_send") ->
            :binary.part(send, 0, send_size - 5) |> binary_to_atom
          String.ends_with?(send, "send") ->
            :binary.part(send, 0, send_size - 4) |> binary_to_atom
          true ->
            nil
        end

      Dict.put(acc, oid, { send, type, binary_type?(send), elem })
    end)
  end

  def bootstrap_query do
    "SELECT oid, typname, typsend, typelem FROM pg_type"
  end

  # TODO: Consider doing automatic refecth of pg_type if we get an
  # unrecognized oid

  def can_decode?(types, oid) do
    case Dict.fetch(types, oid) do
      { :ok, { :array, _, true, elem } } -> can_decode?(types, elem)
      { :ok, { _, _, true, _, } } -> true
      _ -> false
    end
  end

  def oid_to_type(types, oid) do
    case Dict.fetch(types, oid) do
      { :ok, { sender, type , _, _ } } -> { type, sender }
      :error -> nil
    end
  end

  def oid_to_elem(types, oid) do
    case Dict.fetch(types, oid) do
      { :ok, { _, _, _, elem } } -> elem
      :error -> nil
    end
  end

  def decode(:bool, << 1 :: int8 >>, _), do: true
  def decode(:bool, << 0 :: int8 >>, _), do: false
  def decode(:bpchar, bin, _), do: bin
  def decode(:text, bin, _), do: bin
  def decode(:varchar, bin, _), do: bin
  def decode(:bytea, bin, _), do: bin
  def decode(:int2, << n :: int16 >>, _), do: n
  def decode(:int4, << n :: int32 >>, _), do: n
  def decode(:int8, << n :: int64 >>, _), do: n
  def decode(:float4, << 127, 192, 0, 0 >>, _), do: :NaN
  def decode(:float4, << 127, 128, 0, 0 >>, _), do: :inf
  def decode(:float4, << 255, 128, 0, 0 >>, _), do: :"-inf"
  def decode(:float4, << n :: float32 >>, _), do: n
  def decode(:float8, << 127, 248, 0, 0, 0, 0, 0, 0 >>, _), do: :NaN
  def decode(:float8, << 127, 240, 0, 0, 0, 0, 0, 0 >>, _), do: :inf
  def decode(:float8, << 255, 240, 0, 0, 0, 0, 0, 0 >>, _), do: :"-inf"
  def decode(:float8, << n :: float64 >>, _), do: n
  def decode(:date, << n :: int32 >>, _), do: decode_date(n)
  def decode(:time, << n :: int64 >>, _), do: decode_time(n)
  def decode(:timetz, << n :: int64, _tz :: int32 >>, _), do: decode_time(n)
  def decode(:timestamp, << n :: int64 >>, _), do: decode_timestamp(n)
  def decode(:timestamptz, << n :: int64 >>, _), do: decode_timestamp(n)
  def decode(:interval, << s :: int64, d :: int32, m :: int32 >>, _), do: decode_interval(s, d, m)
  def decode(:array, bin, types), do: decode_array(bin, types)
  def decode(:unknown, bin, _), do: bin

  def encode(:bool, true, _, _), do: << 1 >>
  def encode(:bool, false, _, _), do: << 0 >>
  def encode(:bpchar, bin, _, _) when is_binary(bin), do: bin
  def encode(:text, bin, _, _) when is_binary(bin), do: bin
  def encode(:varchar, bin, _, _) when is_binary(bin), do: bin
  def encode(:bytea, bin, _, _) when is_binary(bin), do: bin
  def encode(:int2, n, _, _) when is_integer(n), do: << n :: int16 >>
  def encode(:int4, n, _, _) when is_integer(n), do: << n :: int32 >>
  def encode(:int8, n, _, _) when is_integer(n), do: << n :: int64 >>
  def encode(:float4, :NaN, _, _), do: << 127, 192, 0, 0 >>
  def encode(:float4, :inf, _, _), do: << 127, 128, 0, 0 >>
  def encode(:float4, :"-inf", _, _), do: << 255, 128, 0, 0 >>
  def encode(:float4, n, _, _) when is_number(n), do: << n :: float32 >>
  def encode(:float8, :NaN, _, _), do: << 127, 248, 0, 0, 0, 0, 0, 0 >>
  def encode(:float8, :inf, _, _), do: << 127, 240, 0, 0, 0, 0, 0, 0 >>
  def encode(:float8, :"-inf", _, _), do: << 255, 240, 0, 0, 0, 0, 0, 0 >>
  def encode(:float8, n, _, _) when is_number(n), do: << n :: float64 >>
  def encode(:date, date, _, _), do: encode_date(date)
  def encode(:time, time, _, _), do: encode_time(time)
  def encode(:timestamp, timestamp, _, _), do: encode_timestamp(timestamp)
  def encode(:timestamptz, timestamp, _, _), do: encode_timestamp(timestamp)
  def encode(:interval, interval, _, _), do: encode_interval(interval)
  def encode(:array, list, oid, types) when is_list(list), do: encode_array(list, oid, types)
  def encode(_, _, _, _), do: nil

  Enum.each(@types, fn type ->
    defp binary_type?(unquote(type)), do: true
  end)
  defp binary_type?(_), do: false

  ### decode helpers ###

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

  defp decode_array(<< ndims :: int32, _has_null :: int32, oid :: int32, rest :: binary >>, types) do
    { dims, rest } = :erlang.split_binary(rest, ndims * 2 * 4)
    lengths = lc << len :: int32, _lbound :: int32 >> inbits dims, do: len
    { _, sender } = oid_to_type(types, oid)
    { array, "" } = decode_array(rest, sender, types, lengths)
    array
  end

  defp decode_array("", _type, _types, []) do
    { [], "" }
  end

  defp decode_array(rest, sender, types, [len]) do
    decode_elements(rest, sender, types, [], len)
  end

  defp decode_array(rest, sender, types, [len|lens]) do
    Enum.map_reduce(1..len, rest, fn _, rest ->
      decode_array(rest, sender, types, lens)
    end)
  end

  defp decode_elements(rest, _type, _types, acc, 0) do
    { Enum.reverse(acc), rest }
  end

  defp decode_elements(<< -1 :: int32, rest :: binary >>, sender, types, acc, count) do
    decode_elements(rest, sender, types, [nil|acc], count-1)
  end

  defp decode_elements(<< length :: int32, value :: binary(length), rest :: binary >>,
                       sender, types, acc, count) do
    value = decode(sender, value, types)
    decode_elements(rest, sender, types, [value|acc], count-1)
  end

  ### encode helpers ###

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

  defp encode_array(list, oid, types) do
    elem_oid = oid_to_elem(types, oid)
    { _, elem_type } = oid_to_type(types, elem_oid)
    { data, ndims, lengths } = encode_array(list, elem_type, elem_oid, types, 0, [])
    bin = iolist_to_binary(data)
    lengths = bc len inlist Enum.reverse(lengths), do: << len :: int32, 1 :: int32 >>
    << ndims :: int32, 0 :: int32, elem_oid :: int32, lengths :: binary, bin :: binary >>
  end

  defp encode_array([], _type, _oid, _types, ndims, lengths) do
    { "", ndims, lengths }
  end

  defp encode_array([head|tail]=list, type, oid, types, ndims, lengths) when is_list(head) do
    lengths = [length(list)|lengths]
    { data, ndims, lengths } = encode_array(head, type, oid, types, ndims, lengths)
    [dimlength|_] = lengths

    rest = Enum.map(tail, fn sublist ->
      { data, _, [len|_] } = encode_array(sublist, type, oid, types, ndims, lengths)
      if len != dimlength do
        throw { :postgrex_encode, "nested lists must have lists with matching lengths" }
      end
      data
    end)

    { [data|rest], ndims+1, lengths }
  end

  defp encode_array(list, type, oid, types, ndims, lengths) do
    { data, length } = Enum.map_reduce(list, 0, fn elem, length ->
      bin = encode(type, elem, oid, types)
      { << byte_size(bin) :: int32, bin :: binary >>, length + 1 }
    end)
    { data, ndims+1, [length|lengths] }
  end
end
