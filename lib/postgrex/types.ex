defmodule Postgrex.Types do
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
      [oid, send, elem_oid] = row
      { oid, "" } = String.to_integer(oid)
      send_size = String.length(send)
      { elem, "" } = if elem_oid == "-1", do: nil, else: String.to_integer(elem_oid)

      send =
        try do
          cond do
            String.ends_with?(send, "_send") ->
              String.slice(send, 0, send_size - 5) |> binary_to_existing_atom
            String.ends_with?(send, "send") ->
              String.slice(send, 0, send_size - 4) |> binary_to_existing_atom
            true ->
              nil
          end
        catch
          :error, :badarg -> nil
        end

      if binary_type?(send), do: Dict.put(acc, oid, { send, elem }), else: acc
    end)
  end

  def bootstrap_query do
    "SELECT oid, typsend, typelem FROM pg_type"
  end

  def can_decode?(types, oid) do
    case Dict.fetch(types, oid) do
      { :ok, { :array, elem } } -> can_decode?(types, elem)
      { :ok, _ } -> true
      :error -> false
    end
  end

  def oid_to_sender(types, oid) do
    case Dict.fetch(types, oid) do
      { :ok, { sender, _ } } -> sender
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
  def decode(:float4, << n :: float32 >>, _), do: n
  def decode(:float8, << n :: float64 >>, _), do: n
  def decode(:date, << n :: int32 >>, _), do: decode_date(n)
  def decode(:time, << n :: int64 >>, _), do: decode_time(n)
  def decode(:timetz, << n :: int64, _tz :: int32 >>, _), do: decode_time(n)
  def decode(:timestamp, << n :: int64 >>, _), do: decode_timestamp(n)
  def decode(:timestamptz, << n :: int64 >>, _), do: decode_timestamp(n)
  def decode(:interval, << s :: int64, d :: int32, m :: int32 >>, _), do: decode_interval(s, d, m)
  def decode(:array, bin, types), do: decode_array(bin, types)
  def decode(:unknown, bin, _), do: bin
  def decode(_, nil, _), do: nil

  def encode(_, nil), do: nil
  def encode(:bool, true), do: << 1 >>
  def encode(:bool, false), do: << 0 >>
  def encode(:bpchar, bin), do: bin
  def encode(:text, bin), do: bin
  def encode(:varchar, bin), do: bin
  def encode(:bytea, bin), do: bin
  def encode(:int2, n), do: << n :: int16 >>
  def encode(:int4, n), do: << n :: int32 >>
  def encode(:int8, n), do: << n :: int64 >>
  def encode(:float4, n), do: << n :: float32 >>
  def encode(:float8, n), do: << n :: float64 >>
  def encode(:date, date), do: encode_date(date)
  def encode(:time, time), do: encode_time(time)
  def encode(:timestamp, timestamp), do: encode_timestamp(timestamp)
  def encode(:timestamptz, timestamp), do: encode_timestamp(timestamp)
  def encode(:interval, interval), do: encode_interval(interval)

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
    { div(microsecs, 1_000_000), days, months }
  end

  defp decode_array(<< ndims :: int32, _has_null :: int32, oid :: int32, rest :: binary >>, types) do
    { dims, rest } = :erlang.split_binary(rest, ndims * 2 * 4)
    lengths = lc << len :: int32, _lbound :: int32 >> inbits dims, do: len
    sender = oid_to_sender(types, oid)
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

  defp encode_interval({ secs, days, months }) do
    microsecs = secs * 1_000_000
    << microsecs :: int64, days :: int32, months :: int32 >>
  end
end
