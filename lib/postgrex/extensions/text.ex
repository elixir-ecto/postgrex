defmodule Postgrex.Extensions.Text do
  alias Postgrex.TypeInfo
  import Postgrex.BinaryUtils

  @behaviour Postgrex.Extension

  @senders ~w(date_out time_out timetz_out timestamp_out timestamptz_out
              interval_out)

  @types ~w(void)

  @interval_parts ~w(year mon day)a
  @interval_parts Enum.map(@interval_parts, &{&1, Atom.to_string(&1)})


  def init(opts),
    do: opts

  def matching(_) do
    unquote(Enum.map(@senders, &{:output, &1}) ++
            Enum.map(@types, &{:type, &1}))
  end

  def format(_),
    do: :text

  ### ENCODING ###

  def encode(%TypeInfo{type: "void"}, :void, _, _),
    do: ""

  def encode(%TypeInfo{output: "date_out"}, date, _, _),
    do: encode_date(date)

  def encode(%TypeInfo{output: "time_out"}, time, _, _),
    do: encode_time(time)

  def encode(%TypeInfo{output: "timestamp_out"}, timestamp, _, _),
    do: encode_timestamp(timestamp)

  def encode(%TypeInfo{output: "interval_out"}, interval, _, _),
    do: encode_interval(interval)

  defp encode_date({year, month, day}) do
    [int_pad(year, 4), ?-, int_pad(month, 2), ?-, int_pad(day, 2)]
  end

  defp encode_time({hour, min, sec}) do
    [int_pad(hour, 2), ?:, int_pad(min, 2), ?:, int_pad(sec, 2)]
  end

  defp encode_timestamp({date, time}) do
    [encode_date(date), ?\s, encode_time(time)]
  end

  defp encode_interval(map) do
    iodata =
      Enum.reduce(@interval_parts, "", fn {part, part_str}, acc ->
        if iodata = interval_part(map, part, part_str) do
          [acc, iodata, ?\s]
        else
          acc
        end
      end)

    time = {Map.fetch!(map, :hour), Map.fetch!(map, :min), Map.fetch!(map, :sec)}

    cond do
      time != {0, 0, 0} ->
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

  def decode(%TypeInfo{type: "void"}, "", _, _),
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

  defp decode_date("infinity") do
    raise "TODO"
  end

  defp decode_date("-infinity") do
    raise "TODO"
  end

  defp decode_date(binary) do
    [year, month, <<day::binary(2), rest::binary>>] = :binary.split(binary, "-", [:global])

    case rest do
      " BC" <> rest ->
        rest = rest
        raise "TODO"
      rest ->
        rest = rest
    end

    date =
      {:erlang.binary_to_integer(year),
       :erlang.binary_to_integer(month),
       :erlang.binary_to_integer(day)}

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

    time =
      {:erlang.binary_to_integer(hour),
       :erlang.binary_to_integer(min),
       :erlang.binary_to_integer(sec)}

    {time, rest}
  end

  defp decode_timetz(binary) do
    {time, rest} = decode_time(binary)
    timezone = :binary.split(rest, ":", [:global]) |> Enum.map(&:erlang.binary_to_integer/1)
    destructure [_hour, _min, _sec], timezone

    time
  end

  defp decode_timestamp("infinity") do
    raise "TODO"
  end

  defp decode_timestamp("-infinity") do
    raise "TODO"
  end

  defp decode_timestamp(binary) do
    {date, " " <> rest} = decode_date(binary)
    {time, ""} = decode_time(rest)
    {date, time}
  end

  defp decode_timestamptz("infinity") do
    raise "TODO"
  end

  defp decode_timestamptz("-infinity") do
    raise "TODO"
  end

  defp decode_timestamptz(binary) do
    {date, " " <> rest} = decode_date(binary)
    time = decode_timetz(rest)
    {date, time}
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
      {hour, min, sec} = decode_time(rest) |> perfect_match
      %{map | hour: hour, min: min, sec: sec}
    else
      map
    end
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
end
