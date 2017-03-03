defmodule Postgrex.Extensions.TimestampTZ do
  @moduledoc false
  import Postgrex.BinaryUtils, warn: false
  use Postgrex.BinaryExtension, [send: "timestamptz_send"]

  @gs_epoch :calendar.datetime_to_gregorian_seconds({{2000, 1, 1}, {0, 0, 0}})
  @max_year 294276

  @gs_unix_epoch :calendar.datetime_to_gregorian_seconds({{1970, 1, 1}, {0, 0, 0}})
  @us_epoch (@gs_epoch - @gs_unix_epoch) * 1_000_000
  @gs_max :calendar.datetime_to_gregorian_seconds({{@max_year+1, 1, 1}, {0, 0, 0}}) - @gs_unix_epoch
  @us_max @gs_max * 1_000_000

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
      %DateTime{} = datetime ->
        unquote(__MODULE__).encode_elixir(datetime)
      other ->
        raise ArgumentError, Postgrex.Utils.encode_msg(other, DateTime)
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

  def encode_elixir(%DateTime{utc_offset: 0, std_offset: 0} = datetime) do
    case DateTime.to_unix(datetime, :microseconds) do
      microsecs when microsecs < @us_max ->
        <<8 :: int32, microsecs - @us_epoch :: int64>>
      _ ->
        raise ArgumentError, "#{inspect datetime} is beyond the maximum year 294276"
    end
  end
  def encode_elixir(%DateTime{} = datetime) do
    raise ArgumentError, "#{inspect datetime} is not in UTC"
  end

  def microsecond_to_elixir(microsecs) do
    DateTime.from_unix!(microsecs + @us_epoch, :microseconds)
  end
end
