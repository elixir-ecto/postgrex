defmodule Postgrex.Extensions.TimeTZ do
  @moduledoc false
  import Postgrex.BinaryUtils, warn: false
  use Postgrex.BinaryExtension, [send: "timetz_send"]

  @day (:calendar.time_to_seconds({23, 59, 59}) + 1) * 1_000_000

  def init(opts), do: Keyword.fetch!(opts, :date)

  def encode(:postgrex) do
    quote location: :keep do
      %Postgrex.Time{} = time ->
        unquote(__MODULE__).encode_postgrex(time)
      other ->
        raise ArgumentError, Postgrex.Utils.encode_msg(other, Postgrex.Time)
    end
  end
  def encode(:elixir) do
    quote location: :keep do
      %Time{} = time ->
        unquote(__MODULE__).encode_elixir(time)
      other ->
        raise ArgumentError, Postgrex.Utils.encode_msg(other, Time)
    end
  end

  def decode(:postgrex) do
    quote location: :keep do
      <<12 :: int32, microsecs :: int64, tz :: int32>> ->
        unquote(__MODULE__).microsecond_to_postgrex(microsecs, tz)
    end
  end
  def decode(:elixir) do
    quote location: :keep do
      <<12 :: int32, microsecs :: int64, tz :: int32>> ->
        unquote(__MODULE__).microsecond_to_elixir(microsecs, tz)
    end
  end

  ## Helpers

  def encode_postgrex(%Postgrex.Time{hour: hour, min: min, sec: sec, usec: usec})
      when hour in 0..23 and min in 0..59 and sec in 0..59 and usec in 0..999_999 do
    time = {hour, min, sec}
    <<12 :: int32, :calendar.time_to_seconds(time) * 1_000_000 + usec :: int64,
      0 :: int32>>
  end

  def microsecond_to_postgrex(microsec, tz) do
    microsec
    |> adjust_microsecond(tz)
    |> microsecond_to_postgrex()
  end

  defp adjust_microsecond(microsec, tz) do
    case microsec + tz * 1_000_000 do
      adjusted_microsec when adjusted_microsec < 0 ->
        @day + adjusted_microsec
      adjusted_microsec when adjusted_microsec < @day ->
        adjusted_microsec
      adjusted_microsec ->
        adjusted_microsec - @day
    end
  end

  defp microsecond_to_postgrex(microsecs) do
    secs = div(microsecs, 1_000_000)
    usec = rem(microsecs, 1_000_000)
    {hour, min, sec} = :calendar.seconds_to_time(secs)
    %Postgrex.Time{hour: hour, min: min, sec: sec, usec: usec}
  end

  def encode_elixir(%Time{hour: hour, minute: min, second: sec, microsecond: {usec, _}})
      when hour in 0..23 and min in 0..59 and sec in 0..59 and usec in 0..999_999 do
    time = {hour, min, sec}
    <<12 :: int32, :calendar.time_to_seconds(time) * 1_000_000 + usec :: int64, 0 :: int32>>
  end

  def microsecond_to_elixir(microsec, tz) do
    microsec
    |> adjust_microsecond(tz)
    |> microsecond_to_elixir()
  end

  defp microsecond_to_elixir(microsec) do
    sec = div(microsec, 1_000_000)
    microsec = rem(microsec, 1_000_000)
    sec
    |> :calendar.seconds_to_time()
    |> Time.from_erl!({microsec, 6})
  end
end
