defmodule Postgrex.Extensions.Timestamp do
  @moduledoc false
  import Postgrex.BinaryUtils
  use Postgrex.BinaryExtension,
    [send: "timestamp_send", send: "timestamptz_send"]

  @gs_epoch :calendar.datetime_to_gregorian_seconds({{2000, 1, 1}, {0, 0, 0}})
  @timestamp_max_year 294276

  def encode(_, %Postgrex.Timestamp{year: year, month: month, day: day, hour: hour, min: min, sec: sec, usec: usec}, _, _)
  when year <= @timestamp_max_year and hour in 0..23 and min in 0..59 and sec in 0..59 and usec in 0..999_999 do
    datetime = {{year, month, day}, {hour, min, sec}}
    secs = :calendar.datetime_to_gregorian_seconds(datetime) - @gs_epoch
    <<secs * 1_000_000 + usec :: int64>>
  end
  def encode(type_info, value, _, _) do
    raise ArgumentError,
      Postgrex.Utils.encode_msg(type_info, value, Postgrex.Timestamp)
  end

  def decode(_, <<microsecs :: int64>>, _, _) do
    secs = div(microsecs, 1_000_000)
    usec = rem(microsecs, 1_000_000)
    {{year, month, day}, {hour, min, sec}} = :calendar.gregorian_seconds_to_datetime(secs + @gs_epoch)

    {sec, usec} =
      if year < 2000 and usec != 0 do
        {sec - 1, 1_000_000 + usec}
      else
        {sec, usec}
      end

    %Postgrex.Timestamp{year: year, month: month, day: day, hour: hour, min: min, sec: sec, usec: usec}
  end
end
