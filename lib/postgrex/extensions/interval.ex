defmodule Postgrex.Extensions.Interval do
  @moduledoc false
  import Postgrex.BinaryUtils, warn: false
  use Postgrex.BinaryExtension, send: "interval_send"

  @int64_max 9_223_372_036_854_775_807
  @int64_min -9_223_372_036_854_775_808
  @int32_max 2_147_483_647
  @int32_min -2_147_483_648

  def init(opts) do
    infinity? = Keyword.get(opts, :allow_infinite_intervals, false)
    type = Keyword.get(opts, :interval_decode_type, Postgrex.Interval)

    if type not in [Postgrex.Interval, Duration] do
      raise ArgumentError,
            "#{inspect(type)} is not valid for `:interval_decode_type`. Please use either `Postgrex.Interval` or `Duration`"
    end

    {type, infinity?}
  end

  if Code.ensure_loaded?(Duration) do
    import Bitwise, warn: false
    @default_precision 6
    @precision_mask 0xFFFF
    # 0xFFFF: user did not specify precision (2's complement version of -1)
    # nil: coming from a super type that does not pass modifier for sub-type
    @unspecified_precision [0xFFFF, nil]

    def encode({_type, infinity?}) do
      quote location: :keep do
        :inf ->
          if unquote(infinity?),
            do: unquote(__MODULE__).infinity_binary(:inf),
            else: unquote(__MODULE__).raise_encode_infinity(:inf)

        :"-inf" ->
          if unquote(infinity?),
            do: unquote(__MODULE__).infinity_binary(:"-inf"),
            else: unquote(__MODULE__).raise_encode_infinity(:"-inf")

        %Postgrex.Interval{months: months, days: days, secs: seconds, microsecs: microseconds} ->
          microseconds = 1_000_000 * seconds + microseconds
          <<16::int32(), microseconds::int64(), days::int32(), months::int32()>>

        %Duration{
          year: years,
          month: months,
          week: weeks,
          day: days,
          hour: hours,
          minute: minutes,
          second: seconds,
          microsecond: {microseconds, _precision}
        } ->
          months = 12 * years + months
          days = 7 * weeks + days
          microseconds = 1_000_000 * (3600 * hours + 60 * minutes + seconds) + microseconds
          <<16::int32(), microseconds::int64(), days::int32(), months::int32()>>

        other ->
          raise DBConnection.EncodeError,
                Postgrex.Utils.encode_msg(other, {Postgrex.Interval, Duration})
      end
    end

    def decode({type, infinity?}) do
      quote location: :keep, generated: true do
        <<16::int32(), microseconds::int64(), days::int32(), months::int32()>> ->
          unquote(__MODULE__).decode_interval(
            microseconds,
            days,
            months,
            var!(mod),
            unquote(type),
            unquote(infinity?)
          )
      end
    end

    ## Helpers

    def decode_interval(@int64_max, @int32_max, @int32_max, _type_mod, _struct, infinity?) do
      if infinity?, do: :inf, else: raise_decode_infinity("infinity")
    end

    def decode_interval(@int64_min, @int32_min, @int32_min, _type_mod, _struct, infinity?) do
      if infinity?, do: :"-inf", else: raise_decode_infinity("-infinity")
    end

    def decode_interval(microseconds, days, months, _type_mod, Postgrex.Interval, _infinity?) do
      seconds = div(microseconds, 1_000_000)
      microseconds = rem(microseconds, 1_000_000)

      %Postgrex.Interval{
        months: months,
        days: days,
        secs: seconds,
        microsecs: microseconds
      }
    end

    def decode_interval(microseconds, days, months, type_mod, Duration, _infinity?) do
      seconds = div(microseconds, 1_000_000)
      microseconds = rem(microseconds, 1_000_000)
      precision = if type_mod, do: type_mod &&& unquote(@precision_mask)

      precision =
        if precision in unquote(@unspecified_precision),
          do: unquote(@default_precision),
          else: precision

      Duration.new!(
        month: months,
        day: days,
        second: seconds,
        microsecond: {microseconds, precision}
      )
    end
  else
    def encode({_type, infinity?}) do
      quote location: :keep do
        :inf ->
          if unquote(infinity?),
            do: unquote(__MODULE__).infinity_binary(:inf),
            else: unquote(__MODULE__).raise_encode_infinity(:inf)

        :"-inf" ->
          if unquote(infinity?),
            do: unquote(__MODULE__).infinity_binary(:"-inf"),
            else: unquote(__MODULE__).raise_encode_infinity(:"-inf")

        %Postgrex.Interval{months: months, days: days, secs: seconds, microsecs: microseconds} ->
          microseconds = 1_000_000 * seconds + microseconds
          <<16::int32(), microseconds::int64(), days::int32(), months::int32()>>

        other ->
          raise DBConnection.EncodeError, Postgrex.Utils.encode_msg(other, Postgrex.Interval)
      end
    end

    def decode({_type, infinity?}) do
      quote location: :keep do
        <<16::int32(), microseconds::int64(), days::int32(), months::int32()>> ->
          unquote(__MODULE__).decode_interval(
            microseconds,
            days,
            months,
            Postgrex.Interval,
            unquote(infinity?)
          )
      end
    end

    ## Helpers

    def decode_interval(@int64_max, @int32_max, @int32_max, _struct, infinity?) do
      if infinity?, do: :inf, else: raise_decode_infinity("infinity")
    end

    def decode_interval(@int64_min, @int32_min, @int32_min, _struct, infinity?) do
      if infinity?, do: :"-inf", else: raise_decode_infinity("-infinity")
    end

    def decode_interval(microseconds, days, months, Postgrex.Interval, _infinity?) do
      seconds = div(microseconds, 1_000_000)
      microseconds = rem(microseconds, 1_000_000)

      %Postgrex.Interval{
        months: months,
        days: days,
        secs: seconds,
        microsecs: microseconds
      }
    end
  end

  def infinity_binary(:inf) do
    <<16::int32(), @int64_max::int64(), @int32_max::int32(), @int32_max::int32()>>
  end

  def infinity_binary(:"-inf") do
    <<16::int32(), @int64_min::int64(), @int32_min::int32(), @int32_min::int32()>>
  end

  def raise_encode_infinity(type) do
    raise ArgumentError, """
    got query parameter value of `#{inspect(type)}`. If you want to support infinite intervals \
    in your application, you can enable them by defining your own types:

        Postgrex.Types.define(MyApp.PostgrexTypes, [], allow_infinite_intervals: true)

    And then configuring your database to use it:

        types: MyApp.PostgrexTypes
    """
  end

  defp raise_decode_infinity(type) do
    raise ArgumentError, """
    got \"#{type}\" from PostgreSQL. If you want to support infinite intervals \
    in your application, you can enable them by defining your own types:

        Postgrex.Types.define(MyApp.PostgrexTypes, [], allow_infinite_intervals: true)

    And then configuring your database to use it:

        types: MyApp.PostgrexTypes
    """
  end
end
