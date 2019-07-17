defmodule Postgrex.Extensions.TimeTZ do
  @moduledoc false
  import Postgrex.BinaryUtils, warn: false
  use Postgrex.BinaryExtension, send: "timetz_send"

  @day (:calendar.time_to_seconds({23, 59, 59}) + 1) * 1_000_000

  def encode(_) do
    quote location: :keep do
      %Time{calendar: Calendar.ISO} = time ->
        unquote(__MODULE__).encode_elixir(time)

      other ->
        raise DBConnection.EncodeError, Postgrex.Utils.encode_msg(other, Time)
    end
  end

  def decode(_) do
    quote location: :keep do
      <<12::int32, microsecs::int64, tz::int32>> ->
        unquote(__MODULE__).microsecond_to_elixir(microsecs, tz)
    end
  end

  ## Helpers

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

  def encode_elixir(%Time{hour: hour, minute: min, second: sec, microsecond: {usec, _}})
      when hour in 0..23 and min in 0..59 and sec in 0..59 and usec in 0..999_999 do
    time = {hour, min, sec}
    <<12::int32, :calendar.time_to_seconds(time) * 1_000_000 + usec::int64, 0::int32>>
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
