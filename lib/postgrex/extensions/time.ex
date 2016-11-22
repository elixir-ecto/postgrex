defmodule Postgrex.Extensions.Time do
  @moduledoc false
  alias Postgrex.TypeInfo
  import Postgrex.BinaryUtils
  use Postgrex.BinaryExtension, [send: "time_send", send: "timetz_send"]

  @us_day (:calendar.time_to_seconds({23, 59, 59}) + 1) * 1_000_000

  def encode(%TypeInfo{send: "time_send"}, %Postgrex.Time{} = time, _, _) do
    encode_time(time)
  end
  def encode(%TypeInfo{send: "timetz_send"}, %Postgrex.Time{} = time, _, _) do
    encode_timetz(time)
  end

  def encode(type_info, value, _, _) do
    raise ArgumentError,
      Postgrex.Utils.encode_msg(type_info, value, Postgrex.Time)
  end

  def decode(_, <<n :: int64>>, _, _),
    do: decode_time(n)

  def decode(_, <<n :: int64, tz :: int32>>, _, _),
    do: decode_time(n + tz * 1_000_000)

  def inline(%TypeInfo{send: send}, _types, _opts) do
    case send do
      "time_send"   -> {Time, inline_time_encode(), inline_time_decode()}
      "timetz_send" -> {TimeTZ, inline_timetz_encode(), inline_timetz_decode()}
    end
  end

  ## Helpers

  def encode_time(%Postgrex.Time{hour: hour, min: min, sec: sec, usec: usec})
      when hour in 0..23 and min in 0..59 and sec in 0..59 and usec in 0..999_999 do
    time = {hour, min, sec}
    <<:calendar.time_to_seconds(time) * 1_000_000 + usec :: int64>>
  end

  def encode_timetz(%Postgrex.Time{hour: hour, min: min, sec: sec, usec: usec})
      when hour in 0..23 and min in 0..59 and sec in 0..59 and usec in 0..999_999 do
    time = {hour, min, sec}
    <<:calendar.time_to_seconds(time) * 1_000_000 + usec :: int64, 0 :: int32>>
  end

  def decode_time(microsecs) when microsecs < 0 do
    decode_time(@us_day + microsecs)
  end
  def decode_time(microsecs) when microsecs < @us_day do
    secs = div(microsecs, 1_000_000)
    usec = rem(microsecs, 1_000_000)
    {hour, min, sec} = :calendar.seconds_to_time(secs)
    %Postgrex.Time{hour: hour, min: min, sec: sec, usec: usec}
  end
  def decode_time(microsecs) do
    decode_time(microsecs - @us_day)
  end

  defp inline_time_encode() do
    quote location: :keep do
      %Postgrex.Time{} = time ->
        [<<8 :: int32>> | unquote(__MODULE__).encode_time(time)]
      other ->
        raise ArgumentError, Postgrex.Utils.encode_msg(other, Postgrex.Time)
    end
  end

  defp inline_time_decode() do
    quote location: :keep do
      <<8 :: int32, microsecs :: int64>> ->
        unquote(__MODULE__).decode_time(microsecs)
    end
  end

  defp inline_timetz_encode() do
    quote location: :keep do
      %Postgrex.Time{} = time ->
        [<<12 :: int32>> | unquote(__MODULE__).encode_timetz(time)]
      other ->
        raise ArgumentError, Postgrex.Utils.encode_msg(other, Postgrex.Time)
    end
  end

  defp inline_timetz_decode() do
    quote location: :keep do
      <<12 :: int32, microsecs :: int64, tz :: int32>> ->
        unquote(__MODULE__).decode_time(microsecs + tz * 1_000_000)
    end
  end
end
