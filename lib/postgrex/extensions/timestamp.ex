defmodule Postgrex.Extensions.Timestamp do
  @moduledoc false
  import Postgrex.BinaryUtils, warn: false
  use Postgrex.BinaryExtension, [send: "timestamp_send"]

  @gs_epoch :calendar.datetime_to_gregorian_seconds({{2000, 1, 1}, {0, 0, 0}})
  @max_year 294276

  def init(opts), do: Keyword.fetch!(opts, :date)

  def encode(:postgrex) do
    quote location: :keep do
      %Postgrex.Timestamp{} = timestamp ->
        unquote(__MODULE__).encode_postgrex(timestamp)
      other ->
        raise ArgumentError,
          Postgrex.Utils.encode_msg(other, Postgrex.Timestamp)
    end
  end
  def encode(:elixir) do
    quote location: :keep do
      %NaiveDateTime{} = naive ->
        unquote(__MODULE__).encode_elixir(naive)
      other ->
        raise ArgumentError, Postgrex.Utils.encode_msg(other, NaiveDateTime)
    end
  end

  def decode(:postgrex) do
    quote location: :keep do
      <<8 :: int32, microsecs :: int64>> ->
        unquote(__MODULE__).microsecond_to_postgrex(microsecs)
    end
  end
  def decode(:elixir) do
    quote location: :keep do
      <<8 :: int32, microsecs :: int64>> ->
        unquote(__MODULE__).microsecond_to_elixir(microsecs)
    end
  end

  ## Helpers

  def encode_postgrex(%Postgrex.Timestamp{year: year, month: month, day: day, hour: hour, min: min, sec: sec, usec: usec})
      when year <= @max_year and hour in 0..23 and min in 0..59 and sec in 0..59 and usec in 0..999_999 do
    datetime = {{year, month, day}, {hour, min, sec}}
    secs = :calendar.datetime_to_gregorian_seconds(datetime) - @gs_epoch
    <<8 :: int32, secs * 1_000_000 + usec :: int64>>
  end

  def microsecond_to_postgrex(microsecs) do
    {{{year, month, day}, {hour, min, sec}}, usec} = split(microsecs)

    %Postgrex.Timestamp{year: year, month: month, day: day, hour: hour, min: min, sec: sec, usec: usec}
  end

  defp split(microsecs) when microsecs < 0 and rem(microsecs, 1_000_000) != 0 do
    secs = div(microsecs, 1_000_000) - 1
    microsecs = 1_000_000 + rem(microsecs, 1_000_000)
    split(secs, microsecs)
  end
  defp split(microsecs) do
    secs = div(microsecs, 1_000_000)
    microsecs = rem(microsecs, 1_000_000)
    split(secs, microsecs)
  end

  defp split(secs, microsecs) do
    {:calendar.gregorian_seconds_to_datetime(secs + @gs_epoch), microsecs}
  end

  def encode_elixir(%NaiveDateTime{year: year, month: month, day: day,
                                   hour: hour, minute: min, second: sec, microsecond: {usec, _}})
      when year <= @max_year and hour in 0..23 and min in 0..59 and sec in 0..59 and usec in 0..999_999 do
    datetime = {{year, month, day}, {hour, min, sec}}
    secs = :calendar.datetime_to_gregorian_seconds(datetime) - @gs_epoch
    <<8 :: int32, secs * 1_000_000 + usec :: int64>>
  end

  def microsecond_to_elixir(microsecs) do
    {erl_datetime, microsecs} = split(microsecs)
    NaiveDateTime.from_erl!(erl_datetime, {microsecs, 6})
  end
end
