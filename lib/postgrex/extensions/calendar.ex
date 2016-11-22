if Code.ensure_loaded?(NaiveDateTime) do
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
        "date_send" ->
          <<days :: int32>> = value
          days_to_date(days)
        "timestamp_send" ->
          <<microsecs :: int64>> = value
          decode_naive(microsecs)
        "timestamptz_send" ->
          <<microsecs :: int64>> = value
          decode_datetime(microsecs)
        "time_send" ->
          <<microsecs :: int64>> = value
          microseconds_to_time(microsecs)
        "timetz_send" ->
          <<microsecs :: int64, tz :: int32>> = value
          microseconds_to_time(microsecs, tz)
      end
    end

    def inline(%TypeInfo{send: send}, _, _) do
      case send do
        "date_send" -> inline_date()
        "timestamp_send" -> inline_naive()
        "timestamptz_send" -> inline_datetime()
        "time_send" -> inline_time()
        "timetz_send" -> inline_timetz()
      end
    end

    # Date

    @gd_epoch :calendar.date_to_gregorian_days({2000, 1, 1})
    @gd_max :calendar.date_to_gregorian_days({5874898, 1, 1})

    defp encode_date(_, %Date{} = date) do
      encode_date(date)
    end
    defp encode_date(type_info, value) do
      raise ArgumentError,
        Postgrex.Utils.encode_msg(type_info, value, Date)
    end

    @doc false
    def encode_date(date) do
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

    @doc false
    def days_to_date(days) do
      days + @gd_epoch
      |> :calendar.gregorian_days_to_date()
      |> Date.from_erl!()
    end

    defp inline_date() do
      {Date, inline_date_encode(), inline_date_decode()}
    end

    defp inline_date_encode() do
      quote location: :keep do
        %Date{} = date ->
          [<<4 :: int32>> | unquote(__MODULE__).encode_date(data)]
        other ->
          raise ArgumentError, Postgrex.Utils.encode_msg(other, Date)
      end
    end

    defp inline_date_decode() do
      quote location: :keep do
        <<4 :: int32, days :: int32>> ->
          unquote(__MODULE__).days_to_date(days)
      end
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

    @doc false
    def decode_naive(microsecs) when microsecs < 0 do
      secs = div(microsecs, 1_000_000) - 1
      microsecs = 1_000_000 + rem(microsecs, 1_000_000)
      to_naive(secs, microsecs)
    end
    def decode_naive(microsecs) do
      secs = div(microsecs, 1_000_000)
      microsecs = rem(microsecs, 1_000_000)
      to_naive(secs, microsecs)
    end

    defp to_naive(secs, microsecs) do
      secs + @gs_epoch
      |> :calendar.gregorian_seconds_to_datetime()
      |> NaiveDateTime.from_erl!({microsecs, 6})
    end

    defp inline_naive() do
      {NaiveDateTime, inline_naive_encode(), inline_naive_decode()}
    end

    defp inline_naive_encode() do
      quote location: :keep do
        %NaiveDateTime{} = naive ->
          [<<8 :: int32>> | unquote(__MODULE__).encode_naive(naive)]
        other ->
          raise ArgumentError, Postgrex.Utils.encode_msg(other, NaiveDateTime)
      end
    end

    defp inline_naive_decode() do
      quote location: :keep do
        <<8 :: int32, microsecs :: int64>> ->
          unquote(__MODULE__).decode_naive(microsecs)
      end
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

    defp decode_datetime(microsecs) do
      DateTime.from_unix!(microsecs + @uus_epoch, :microseconds)
    end

    defp inline_datetime() do
      {DateTime, inline_datetime_encode(), inline_datetime_decode()}
    end

    defp inline_datetime_encode() do
      quote location: :keep do
        %DateTime{} = datetime ->
          [<<8 :: int32>> | unquote(__MODULE__).encode_datetime(datetime)]
        other ->
          raise ArgumentError, Postgrex.Utils.encode_msg(other, DateTime)
      end
    end

    defp inline_datetime_decode() do
      quote location: :keep do
        <<8 :: int32, microsecs :: int64>> ->
          unquote(__MODULE__).decode_datetime(microsecs)
      end
    end

    # Time

    defp encode_time(_, %Time{} = value) do
      encode_time(value)
    end
    defp encode_time(type_info, value) do
      raise ArgumentError, Postgrex.Utils.encode_msg(type_info, value, Time)
    end

    @doc false
    def encode_time(time) do
      <<time_to_microseconds(time) :: int64>>
    end

    defp time_to_microseconds(%Time{microsecond: {microsec, _}} = time) do
      sec =
        time
        |> Time.to_erl()
        |> :calendar.time_to_seconds()

      sec * 1_000_000 + microsec
    end

    @doc false
    def microseconds_to_time(microsec) do
      sec = div(microsec, 1_000_000)
      microsec = rem(microsec, 1_000_000)
      sec
      |> :calendar.seconds_to_time()
      |> Time.from_erl!(microsec)
    end

    defp inline_time() do
      {Time, inline_time_encode(), inline_time_decode()}
    end

    defp inline_time_encode() do
      quote location: :keep do
        %Time{} = time ->
          [<<8 :: int32>> | unquote(__MODULE__).encode_time(time)]
        other ->
          raise ArgumentError, Postgrex.Utils.encode_msg(other, Time)
      end
    end

    defp inline_time_decode() do
      quote location: :keep do
        <<8 :: int32, microsecs :: int64>> ->
          unquote(__MODULE__).microseconds_to_time(microsecs)
      end
    end

    # Time with time zone

    @dus_max (:calendar.time_to_seconds({23, 59, 59}) + 1) * 1_000_000

    defp encode_timetz(_, %Time{} = value) do
      encode_timetz(value)
    end
    defp encode_timetz(type_info, value) do
      raise ArgumentError, Postgrex.Utils.encode_msg(type_info, value, Time)
    end

    @doc false
    def encode_timetz(time) do
      <<time_to_microseconds(time) :: int64, 0 :: int32>>
    end

    @doc false
    def microseconds_to_time(microsec, tz) do
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

    defp inline_timetz() do
      {TimeTZ, inline_timetz_encode(), inline_timetz_decode()}
    end

    defp inline_timetz_encode() do
      quote location: :keep do
        %Time{} = time ->
          [<<12 :: int32>> | unquote(__MODULE__).encode_timetz(time)]
        other ->
          raise ArgumentError, Postgrex.Utils.encode_msg(other, Time)
      end
    end

    defp inline_timetz_decode() do
      quote location: :keep do
        <<12 :: int32, microsecs :: int64, tz :: int32>> ->
          unquote(__MODULE__).microseconds_to_time(microsecs, tz)
      end
    end
  end
end
