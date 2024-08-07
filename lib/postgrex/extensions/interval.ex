defmodule Postgrex.Extensions.Interval do
  @moduledoc false
  import Postgrex.BinaryUtils, warn: false
  use Postgrex.BinaryExtension, send: "interval_send"

  def init(opts) do
    case Keyword.get(opts, :interval_decode_type, Postgrex.Interval) do
      type when type in [Postgrex.Interval, Duration] ->
        type

      other ->
        raise ArgumentError,
              "#{inspect(other)} is not valid for `:interval_decode_type`. Please use either `Postgrex.Interval` or `Duration`"
    end
  end

  if Code.ensure_loaded?(Duration) do
    import Bitwise, warn: false
    @default_precision 6
    @precision_mask 0xFFFF
    # 0xFFFF: user did not specify precision (2's complement version of -1)
    # nil: coming from a super type that does not pass modifier for sub-type
    @unspecified_precision [0xFFFF, nil]

    def encode(_) do
      quote location: :keep do
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

    def decode(type) do
      quote location: :keep, generated: true do
        <<16::int32(), microseconds::int64(), days::int32(), months::int32()>> ->
          unquote(__MODULE__).decode_interval(
            microseconds,
            days,
            months,
            var!(mod),
            unquote(type)
          )
      end
    end

    ## Helpers

    def decode_interval(microseconds, days, months, _type_mod, Postgrex.Interval) do
      seconds = div(microseconds, 1_000_000)
      microseconds = rem(microseconds, 1_000_000)

      %Postgrex.Interval{
        months: months,
        days: days,
        secs: seconds,
        microsecs: microseconds
      }
    end

    def decode_interval(microseconds, days, months, type_mod, Duration) do
      years = div(months, 12)
      months = rem(months, 12)
      weeks = div(days, 7)
      days = rem(days, 7)
      seconds = div(microseconds, 1_000_000)
      microseconds = rem(microseconds, 1_000_000)
      minutes = div(seconds, 60)
      seconds = rem(seconds, 60)
      hours = div(minutes, 60)
      minutes = rem(minutes, 60)
      precision = if type_mod, do: type_mod &&& unquote(@precision_mask)

      precision =
        if precision in unquote(@unspecified_precision),
          do: unquote(@default_precision),
          else: precision

      Duration.new!(
        year: years,
        month: months,
        week: weeks,
        day: days,
        hour: hours,
        minute: minutes,
        second: seconds,
        microsecond: {microseconds, precision}
      )
    end
  else
    def encode(_) do
      quote location: :keep do
        %Postgrex.Interval{months: months, days: days, secs: seconds, microsecs: microseconds} ->
          microseconds = 1_000_000 * seconds + microseconds
          <<16::int32(), microseconds::int64(), days::int32(), months::int32()>>

        other ->
          raise DBConnection.EncodeError, Postgrex.Utils.encode_msg(other, Postgrex.Interval)
      end
    end

    def decode(_) do
      quote location: :keep do
        <<16::int32(), microseconds::int64(), days::int32(), months::int32()>> ->
          unquote(__MODULE__).decode_interval(microseconds, days, months, Postgrex.Interval)
      end
    end

    ## Helpers

    def decode_interval(microseconds, days, months, Postgrex.Interval) do
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
end
