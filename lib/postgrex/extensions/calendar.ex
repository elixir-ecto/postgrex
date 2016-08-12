if Code.ensure_loaded?(Calendar) do
  defmodule Postgrex.Extensions.Calendar do
    @moduledoc """
    An extension that supports the calendar structs introduced in Elixir 1.3.0:
    `Date`, `Time`, `DateTime` and `NaiveDateTime`. This module won't be
    compiled if the Elixir version is less than 1.3.0 because the structs do
    not exist.

    This extension is not used by default, it needs to be included in the
    `:extensions` option to `Postgrex.start_link/1`. The option term is ignored
    by this extension.

    ## Examples

        Postgrex.start_link([extensions: [{Postgrex.Extensions.Calendar, []}]])
    """

    import Postgrex.BinaryUtils
    alias Postgrex.TypeInfo
    use Postgrex.BinaryExtension,
      [send: "date_send", send: "timestamp_send", send: "timestamptz_send",
       send: "time_send", send: "timetz_send"]

    def encode(%TypeInfo{send: send} = type_info, value, _, _) do
      case send do
        "date_send" -> encode_date(type_info, value)
        "timestamp_send" -> encode_naive(type_info, value)
        "timestamptz_send" -> encode_datetime(type_info, value)
        "time_send" -> encode_time(type_info, value)
        "timetz_send" -> encode_timetz(type_info, value)
      end
    end

    def decode(%TypeInfo{send: send}, value, _, _) do
      case send do
        "date_send" -> decode_date(value)
        "timestamp_send" -> decode_naive(value)
        "timestamptz_send" -> decode_datetime(value)
        "time_send" -> decode_time(value)
        "timetz_send" -> decode_timetz(value)
      end
    end

    # Date

    @gd_epoch :calendar.date_to_gregorian_days({2000, 1, 1})
    @gd_max :calendar.date_to_gregorian_days({5874898, 1, 1})

    defp encode_date(_, %Date{} = date) do
      days =
        date
        |> Date.to_erl()
        |> :calendar.date_to_gregorian_days()
      if days < @gd_max do
        <<days - @gd_epoch :: int32>>
      else
        raise ArgumentError, "#{inspect date} is beyond the maximum year 5874987"
      end
    end
    defp encode_date(type_info, value) do
      raise ArgumentError,
        Postgrex.Utils.encode_msg(type_info, value, Date)
    end

    defp decode_date(<<days :: int32>>) do
      days + @gd_epoch
      |> :calendar.gregorian_days_to_date()
      |> Date.from_erl!()
    end

    # NaiveDateTime

    @gs_epoch :calendar.datetime_to_gregorian_seconds({{2000, 1, 1}, {0, 0, 0}})
    @gs_max :calendar.datetime_to_gregorian_seconds({{294277, 1, 1}, {0, 0, 0}})

    defp encode_naive(_, %NaiveDateTime{microsecond: {microsecs, _}} = naive) do
      erl = NaiveDateTime.to_erl(naive)
      case :calendar.datetime_to_gregorian_seconds(erl) - @gs_epoch do
        sec when sec < @gs_max ->
          <<sec * 1_000_000 + microsecs :: int64>>
        _ ->
          raise ArgumentError, "#{inspect naive} is beyond the maximum year 294276"
      end
    end
    defp encode_naive(type_info, value) do
      raise ArgumentError,
        Postgrex.Utils.encode_msg(type_info, value, NaiveDateTime)
    end

    defp decode_naive(<<microsecs :: int64>>) when microsecs < 0 do
      secs = div(microsecs, 1_000_000) - 1
      microsecs = 1_000_000 + rem(microsecs, 1_000_000)
      to_naive(secs, microsecs)
    end
    defp decode_naive(<<microsecs :: int64>>) do
      secs = div(microsecs, 1_000_000)
      microsecs = rem(microsecs, 1_000_000)
      to_naive(secs, microsecs)
    end

    defp to_naive(secs, microsecs) do
      secs + @gs_epoch
      |> :calendar.gregorian_seconds_to_datetime()
      |> NaiveDateTime.from_erl!({microsecs, 6})
    end

    # DateTime

    @gs_unix_epoch :calendar.datetime_to_gregorian_seconds({{1970, 1, 1}, {0, 0, 0}})
    @us_epoch :calendar.datetime_to_gregorian_seconds({{2000, 1, 1}, {0, 0, 0}}) - @gs_unix_epoch
    @uus_epoch @us_epoch |> DateTime.from_unix!() |> DateTime.to_unix(:microseconds)
    @us_max :calendar.datetime_to_gregorian_seconds({{294277, 1, 1}, {0, 0, 0}}) - @gs_unix_epoch
    @uus_max @us_max |> DateTime.from_unix!() |> DateTime.to_unix(:microseconds)

    defp encode_datetime(_, %DateTime{utc_offset: 0, std_offset: 0} = date_time) do
      case DateTime.to_unix(date_time, :microseconds) do
        microsecs when microsecs < @uus_max ->
          <<microsecs - @uus_epoch :: int64>>
        _ ->
          raise ArgumentError, "#{inspect date_time} is beyond the maximum year 294276"
      end
    end
    defp encode_datetime(_, %DateTime{} = date_time) do
      raise ArgumentError, "#{inspect date_time} is not in UTC"
    end
    defp encode_datetime(type_info, value) do
      raise ArgumentError,
        Postgrex.Utils.encode_msg(type_info, value, DateTime)
    end

    defp decode_datetime(<<microsecs :: int64>>) do
      DateTime.from_unix!(microsecs + @uus_epoch, :microseconds)
    end

    # Time

    defp encode_time(type_info, value) do
      case time_to_microseconds(value) do
        microsec when is_integer(microsec) ->
          <<microsec :: int64>>
        :error ->
          raise ArgumentError,
          Postgrex.Utils.encode_msg(type_info, value, Time)
      end
    end

    defp time_to_microseconds(%Time{microsecond: {microsec, _}} = time) do
      sec =
        time
        |> Time.to_erl()
        |> :calendar.time_to_seconds()

      sec * 1_000_000 + microsec
    end

    defp decode_time(<<microsec :: int64>>) do
      microseconds_to_time(microsec)
    end

    defp microseconds_to_time(microsec) do
      sec = div(microsec, 1_000_000)
      microsec = rem(microsec, 1_000_000)
      sec
      |> :calendar.seconds_to_time()
      |> Time.from_erl!(microsec)
    end

    # Time with time zone

    @dus_max (:calendar.time_to_seconds({23, 59, 59}) + 1) * 1_000_000

    defp encode_timetz(type_info, value) do
      case time_to_microseconds(value) do
        microsec when is_integer(microsec) ->
          <<microsec :: int64, 0 :: int32>>
        :error ->
          raise ArgumentError,
          Postgrex.Utils.encode_msg(type_info, value, Time)
      end
    end

    defp decode_timetz(<<microsec :: int64, tz :: int32>>) do
      microsec
      |> adjust_microseconds(tz)
      |> microseconds_to_time()
    end

    defp adjust_microseconds(microsec, tz) do
      case microsec + tz * 1_000_000 do
        adjusted_microsec when adjusted_microsec < 0 ->
          @dus_max + adjusted_microsec
        adjusted_microsec when adjusted_microsec < @dus_max ->
          adjusted_microsec
        adjusted_microsec ->
          adjusted_microsec - @dus_max
      end
    end
  end
end
