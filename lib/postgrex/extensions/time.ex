defmodule Postgrex.Extensions.Time do
  @moduledoc false
  import Postgrex.BinaryUtils
  use Postgrex.BinaryExtension, [send: "time_send", send: "timetz_send"]

  def encode(_, %Postgrex.Time{hour: hour, min: min, sec: sec, usec: usec}, _, _)
  when hour in 0..23 and min in 0..59 and sec in 0..59 and usec in 0..999_999  do
    time = {hour, min, sec}
    <<:calendar.time_to_seconds(time) * 1_000_000 + usec :: int64>>
  end
  def encode(type_info, value, _, _) do
    raise ArgumentError,
      Postgrex.Utils.encode_msg(type_info, value, Postgrex.Time)
  end

  def decode(_, <<n :: int64>>, _, _),
    do: decode_time(n)

  ## Helpers

  defp decode_time(microsecs) do
    secs = div(microsecs, 1_000_000)
    usec = rem(microsecs, 1_000_000)
    {hour, min, sec} = :calendar.seconds_to_time(secs)
    %Postgrex.Time{hour: hour, min: min, sec: sec, usec: usec}
  end
end
