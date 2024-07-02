defmodule Postgrex.Extensions.Timestamp do
  @moduledoc false
  import Postgrex.BinaryUtils, warn: false
  use Postgrex.BinaryExtension, send: "timestamp_send"

  @gs_epoch NaiveDateTime.to_gregorian_seconds(~N[2000-01-01 00:00:00]) |> elem(0)
  @max_year 294_276
  @min_year -4_713
  @plus_infinity 9_223_372_036_854_775_807
  @minus_infinity -9_223_372_036_854_775_808
  @default_precision 6

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
      <<8::int32(), microsecs::int64()>> ->
        unquote(__MODULE__).microsecond_to_elixir(microsecs, var!(mod), unquote(infinity?))
    end
  end

  ## Helpers

  def encode_elixir(
        %_{
          year: year,
          hour: hour,
          minute: min,
          second: sec,
          microsecond: {usec, _}
        } = date_time
      )
      when year <= @max_year and year >= @min_year and hour in 0..23 and min in 0..59 and
             sec in 0..59 and
             usec in 0..999_999 do
    {gregorian_seconds, usec} = NaiveDateTime.to_gregorian_seconds(date_time)
    secs = gregorian_seconds - @gs_epoch
    <<8::int32(), secs * 1_000_000 + usec::int64()>>
  end

  def microsecond_to_elixir(@plus_infinity, _precision, infinity?) do
    if infinity?, do: :inf, else: raise_infinity("infinity")
  end

  def microsecond_to_elixir(@minus_infinity, _precision, infinity?) do
    if infinity?, do: :"-inf", else: raise_infinity("-infinity")
  end

  def microsecond_to_elixir(microsecs, precision, _infinity) do
    split(microsecs, precision)
  end

  defp split(microsecs, precision) when microsecs < 0 and rem(microsecs, 1_000_000) != 0 do
    secs = div(microsecs, 1_000_000) - 1
    microsecs = 1_000_000 + rem(microsecs, 1_000_000)
    split(secs, microsecs, precision)
  end

  defp split(microsecs, precision) do
    secs = div(microsecs, 1_000_000)
    microsecs = rem(microsecs, 1_000_000)
    split(secs, microsecs, precision)
  end

  defp split(secs, microsecs, precision) do
    # use the default precision if the precision modifier from postgres is -1 (this means no precision specified)
    # or if the precision is missing because we are in a super type which does not give us the sub-type's modifier
    precision = if precision in [-1, nil], do: @default_precision, else: precision
    NaiveDateTime.from_gregorian_seconds(secs + @gs_epoch, {microsecs, precision})
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
