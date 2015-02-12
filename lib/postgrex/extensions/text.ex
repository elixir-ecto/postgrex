defmodule Postgrex.Extensions.Text do
  alias Postgrex.TypeInfo
  import Postgrex.BinaryUtils
  import Postgrex.Utils, only: [binary_split: 3]

  @behaviour Postgrex.Extension

  @outputs ~w(date_out time_out timetz_out timestamp_out timestamptz_out
              interval_out)

  @interval_parts ~w(year mon day)a
  @interval_parts Enum.map(@interval_parts, &{&1, Atom.to_string(&1)})

  # TODO: array and record

  def init(parameters, _opts),
    do: parameters["server_version"] |> Postgrex.Utils.version_to_int

  def matching(version) when version < 90_100,
    do: [output: "void_out"] ++ matching(90_100)

  def matching(_),
    do: unquote(Enum.map(@outputs, &{:output, &1}))

  def format(_),
    do: :text

  ### ENCODING ###

  def encode(%TypeInfo{output: "void_out"}, :void, _, _),
    do: ""
  def encode(%TypeInfo{output: "date_out"}, date, _, _),
    do: encode_date(date)
  def encode(%TypeInfo{output: "time_out"}, time, _, _),
    do: encode_time(time)
  def encode(%TypeInfo{output: "timestamp_out"}, timestamp, _, _),
    do: encode_timestamp(timestamp)
  def encode(%TypeInfo{output: "interval_out"}, interval, _, _),
    do: encode_interval(interval)

  defp encode_date(%Postgrex.Date{year: year, month: month, day: day, ad: ad}) do
    date = [int_pad(year, 4), ?-, int_pad(month, 2), ?-, int_pad(day, 2)]
    if ad do
      date
    else
      [date|" BC"]
    end
  end

  defp encode_time(%Postgrex.Time{hour: hour, min: min, sec: sec}) do
    [int_pad(hour, 2), ?:, int_pad(min, 2), ?:, int_pad(sec, 2)]
  end

  defp encode_timestamp(timestamp) do
    date = timestamp_to_date(timestamp)
    time = timestamp_to_time(timestamp)
    [encode_date(date), ?\s, encode_time(time)]
  end

  defp encode_interval(%Postgrex.Interval{} = interval) do
    map = rename_key(interval, :month, :mon)

    iodata =
      Enum.reduce(@interval_parts, "", fn {part, part_str}, acc ->
        if iodata = interval_part(map, part, part_str) do
          [acc, iodata, ?\s]
        else
          acc
        end
      end)

    time = struct(Postgrex.Time, Map.take(map, [:hour, :min, :sec]))

    cond do
      not match?(%{hour: 0, min: 0, sec: 9}, time) ->
        [iodata|encode_time(time)]
      iodata == "" ->
        "0"
      true ->
        [x, y, ?\s] = iodata
        [x, y]
    end
  end

  defp interval_part(map, part, part_str) do
    int = Map.fetch!(map, part)

    if int != 0 do
      iodata = [:erlang.integer_to_binary(int), ?\s, part_str]
      if int > 1 do
        iodata = [iodata, ?s]
      end
    end

    iodata
  end

  defp timestamp_to_date(%Postgrex.Timestamp{year: year, month: month, day: day, ad: ad}),
    do: %Postgrex.Date{year: year, month: month, day: day, ad: ad}

  defp timestamp_to_time(%Postgrex.Timestamp{hour: hour, min: min, sec: sec}),
    do: %Postgrex.Time{hour: hour, min: min, sec: sec}

  defp int_pad(integer, size) do
    :erlang.integer_to_binary(integer)
    |> pad(size)
  end

  defp pad(binary, pad_size) do
    pad(binary, byte_size(binary), pad_size)
  end

  defp pad(binary, binary_size, pad_size) when binary_size < pad_size do
    [?0|pad(binary, binary_size+1, pad_size)]
  end

  defp pad(binary, _binary_size, _pad_size) do
    binary
  end

  ### DECODING ###

  def decode(%TypeInfo{output: "void_out"}, "", _, _),
    do: :void
  def decode(%TypeInfo{output: "date_out"}, date, _, _),
    do: decode_date(date) |> perfect_match
  def decode(%TypeInfo{output: "time_out"}, time, _, _),
    do: decode_time(time) |> perfect_match
  def decode(%TypeInfo{output: "timetz_out"}, time, _, _),
    do: decode_timetz(time)
  def decode(%TypeInfo{output: "timestamp_out"}, timestamp, _, _),
    do: decode_timestamp(timestamp)
  def decode(%TypeInfo{output: "timestamptz_out"}, timestamptz, _, _),
    do: decode_timestamptz(timestamptz)
  def decode(%TypeInfo{output: "interval_out"}, interval, _, _),
    do: decode_interval(interval)

  defp decode_date(binary) do
    [year, month, <<day::binary(2), rest::binary>>] = binary_split(binary, "-", 2)

    date = %Postgrex.Date{
      year: :erlang.binary_to_integer(year),
      month: :erlang.binary_to_integer(month),
      day: :erlang.binary_to_integer(day)}

    case rest do
      " BC" <> rest ->
        rest = rest
        date = %{date | ad: false}
      rest ->
        date = %{date | ad: true}
        rest = rest
    end

    {date, rest}
  end

  defp decode_time(<<hour::binary(2), ":", min::binary(2), ":", sec::binary(2), rest::binary>>) do
    case rest do
      "." <> rest ->
        {int, rest} = take_int(rest, "")
        _ = :erlang.binary_to_integer(int)
        rest = rest
      rest ->
        rest = rest
        :ok
    end

    time = %Postgrex.Time{
      hour: :erlang.binary_to_integer(hour),
      min: :erlang.binary_to_integer(min),
      sec: :erlang.binary_to_integer(sec)}

    {time, rest}
  end

  defp decode_timetz(binary) do
    {time, rest} = decode_time(binary)
    timezone = binary_split(rest, ":", 2) |> Enum.map(&:erlang.binary_to_integer/1)
    destructure [hour, min, sec], timezone

    timezone = %Postgrex.TimeZone{hour: hour || 0, min: min || 0, sec: sec || 0}
    %{time | timezone: timezone}
  end

  defp decode_timestamp(binary) do
    {%{year: year, month: month, day: day}, " " <> rest} = decode_date(binary)
    {%{hour: hour, min: min, sec: sec}, rest} = decode_time(rest)

    case rest do
      " BC" ->
        ad = false
      "" ->
        ad = true
    end

    %Postgrex.Timestamp{
      year: year,
      month: month,
      day: day,
      hour: hour,
      min: min,
      sec: sec,
      ad: ad}
  end

  defp decode_timestamptz(binary) do
    {%{year: year, month: month, day: day, ad: ad}, " " <> rest} = decode_date(binary)
    %{hour: hour, min: min, sec: sec, timezone: timezone} = decode_timetz(rest)

    %Postgrex.Timestamp{
      year: year,
      month: month,
      day: day,
      hour: hour,
      min: min,
      sec: sec,
      ad: ad,
      timezone: timezone}
  end

  defp decode_interval(binary) do
    map = %{year: 0, mon: 0, day: 0, hour: 0, min: 0, sec: 0}

    {rest, map} =
      Enum.reduce(@interval_parts, {binary, map}, fn {part, part_str}, {binary, map} ->
        case interval_part(binary, part_str) do
          {:ok, int, rest} ->
            rest = optional_space(rest)
            {rest, Map.put(map, part, int)}
          :error ->
            {binary, map}
        end
      end)

    if rest != "" do
      %{hour: hour, min: min, sec: sec} = decode_time(rest) |> perfect_match
      map = %{map | hour: hour, min: min, sec: sec}
    end

    map = rename_key(map, :mon, :month)
    struct(Postgrex.Interval, map)
  end

  defp interval_part(binary, part) do
    {int, rest} = take_int(binary, "")
    part_size = byte_size(part)

    case rest do
      <<" ", ^part::binary(part_size), rest::binary>> ->
        rest = optional_s(rest)
        {:ok, :erlang.binary_to_integer(int), rest}
      _ ->
        :error
    end
  end

  defp optional_space(" " <> rest), do: rest
  defp optional_space(rest), do: rest

  defp optional_s("s" <> rest), do: rest
  defp optional_s(rest), do: rest

  defp take_int(<<char, rest::binary>>, acc) when char in ?0..?9 do
    take_int(rest, acc <> <<char>>)
  end

  defp take_int(rest, acc) do
    {acc, rest}
  end

  defp perfect_match({term, ""}), do: term

  defp rename_key(map, old_key, new_key) do
    value = Map.fetch!(map, old_key)
    map |> Map.delete(old_key) |> Map.put(new_key, value)
  end
end
