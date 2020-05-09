defmodule Postgrex.Extensions.Timestamp do
  @moduledoc false
  import Postgrex.BinaryUtils, warn: false
  use Postgrex.BinaryExtension, send: "timestamp_send"

  @gs_epoch :calendar.datetime_to_gregorian_seconds({{2000, 1, 1}, {0, 0, 0}})
  @max_year 294_276
  @plus_infinity 9_223_372_036_854_775_807
  @minus_infinity -9_223_372_036_854_775_808

  def init(opts), do: Keyword.get(opts, :allow_infinite_timestamps, false)

  def encode(_) do
    quote location: :keep do
      %NaiveDateTime{calendar: Calendar.ISO} = naive ->
        unquote(__MODULE__).encode_elixir(naive)

      %DateTime{calendar: Calendar.ISO} = dt ->
        unquote(__MODULE__).encode_elixir(dt)

      other ->
        raise DBConnection.EncodeError,
              Postgrex.Utils.encode_msg(other, {DateTime, NaiveDateTime})
    end
  end

  def decode(infinity?) do
    quote location: :keep do
      <<8::int32, microsecs::int64>> ->
        unquote(__MODULE__).microsecond_to_elixir(microsecs, unquote(infinity?))
    end
  end

  ## Helpers

  def encode_elixir(%_{
        year: year,
        month: month,
        day: day,
        hour: hour,
        minute: min,
        second: sec,
        microsecond: {usec, _}
      })
      when year <= @max_year and hour in 0..23 and min in 0..59 and sec in 0..59 and
             usec in 0..999_999 do
    datetime = {{year, month, day}, {hour, min, sec}}
    secs = :calendar.datetime_to_gregorian_seconds(datetime) - @gs_epoch
    <<8::int32, secs * 1_000_000 + usec::int64>>
  end

  def microsecond_to_elixir(@plus_infinity, infinity?) do
    if infinity?, do: :inf, else: raise_infinity("infinity")
  end

  def microsecond_to_elixir(@minus_infinity, infinity?) do
    if infinity?, do: :"-inf", else: raise_infinity("-infinity")
  end

  def microsecond_to_elixir(microsecs, _infinity) do
    {erl_datetime, microsecs} = split(microsecs)
    NaiveDateTime.from_erl!(erl_datetime, {microsecs, 6})
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

  defp raise_infinity(type) do
    raise ArgumentError, """
    got \"#{type}\" from PostgreSQL. If you want to support infinity timestamps \
    in your application, you can enable them by defining your own types:

        Postgrex.Types.define(MyApp.PostgrexTypes, [], allow_infinite_timestamps: true)

    And then configuring your database to use it:

        types: MyApp.PostgrexTypes
    """
  end
end
