if Code.ensure_loaded?(Calendar) do
  defmodule Postgrex.Extensions.Calendar.Date do
    @moduledoc false
    import Postgrex.BinaryUtils
    use Postgrex.BinaryExtension, [send: "date_send"]

    @gd_epoch :calendar.date_to_gregorian_days({2000, 1, 1})
    @max_days :calendar.date_to_gregorian_days({5874898, 1, 1})

    def encode(_, %Date{} = date, _, _) do
      days =
        date
        |> Date.to_erl()
        |> :calendar.date_to_gregorian_days()
      if days < @max_days do
        <<days - @gd_epoch :: int32>>
      else
        raise ArgumentError, "#{inspect date} is beyond the maximum year 5874987"
      end
    end
    def encode(type_info, value, _, _) do
      raise ArgumentError,
        Postgrex.Utils.encode_msg(type_info, value, Date)
    end

    def decode(_, <<days :: int32>>, _, _) do
      days + @gd_epoch
      |> :calendar.gregorian_days_to_date()
      |> Date.from_erl!()
    end
  end

  defmodule Postgrex.Extensions.Calendar.NaiveDateTime do
    @moduledoc false
    import Postgrex.BinaryUtils
    use Postgrex.BinaryExtension, [send: "timestamp_send"]

    @gs_epoch :calendar.datetime_to_gregorian_seconds({{2000, 1, 1}, {0, 0, 0}})
    @max_sec :calendar.datetime_to_gregorian_seconds({{294277, 1, 1}, {0, 0, 0}})

    def encode(_, %NaiveDateTime{microsecond: {microsecs, _}} = naive, _, _) do
      erl = NaiveDateTime.to_erl(naive)
      case :calendar.datetime_to_gregorian_seconds(erl) - @gs_epoch do
        sec when sec < @max_sec ->
          <<sec * 1_000_000 + microsecs :: int64>>
        _ ->
          raise ArgumentError, "#{inspect naive} is beyond the maximum year 294276"
      end
    end
    def encode(type_info, value, _, _) do
      raise ArgumentError,
        Postgrex.Utils.encode_msg(type_info, value, NaiveDateTime)
    end

    def decode(_, <<microsecs :: int64>>, _, _) when microsecs < 0 do
      secs = div(microsecs, 1_000_000) - 1
      microsecs = 1_000_000 + rem(microsecs, 1_000_000)
      to_naive(secs, microsecs)
    end
    def decode(_, <<microsecs :: int64>>, _, _) do
      secs = div(microsecs, 1_000_000)
      microsecs = rem(microsecs, 1_000_000)
      to_naive(secs, microsecs)
    end

    defp to_naive(secs, microsecs) do
      secs + @gs_epoch
      |> :calendar.gregorian_seconds_to_datetime()
      |> NaiveDateTime.from_erl!({microsecs, 6})
    end
  end

  defmodule Postgrex.Extensions.Calendar.DateTime do
    @moduledoc false
    import Postgrex.BinaryUtils
    use Postgrex.BinaryExtension, [send: "timestamptz_send"]

    @unix_epoch_sec :calendar.datetime_to_gregorian_seconds({{1970, 1, 1}, {0, 0, 0}})
    @gs_epoch_sec :calendar.datetime_to_gregorian_seconds({{2000, 1, 1}, {0, 0, 0}}) - @unix_epoch_sec
    @gs_epoch @gs_epoch_sec |> DateTime.from_unix!() |> DateTime.to_unix(:microseconds)
    @max_unix_sec :calendar.datetime_to_gregorian_seconds({{294277, 1, 1}, {0, 0, 0}}) - @unix_epoch_sec
    @max_unix @max_unix_sec |> DateTime.from_unix!() |> DateTime.to_unix(:microseconds)

    def encode(_, %DateTime{utc_offset: 0, std_offset: 0} = date_time, _, _) do
      case DateTime.to_unix(date_time, :microseconds) do
        microsecs when microsecs < @max_unix ->
          <<microsecs - @gs_epoch :: int64>>
        _ ->
          raise ArgumentError, "#{inspect date_time} is beyond the maximum year 294276"
      end
    end
    def encode(_, %DateTime{} = date_time, _, _) do
      raise ArgumentError, "#{inspect date_time} is not in UTC"
    end
    def encode(type_info, value, _, _) do
      raise ArgumentError,
        Postgrex.Utils.encode_msg(type_info, value, DateTime)
    end

    def decode(_, <<microsecs :: int64>>, _, _) do
      DateTime.from_unix!(microsecs + @gs_epoch, :microseconds)
    end
  end

  defmodule Postgrex.Extensions.Calendar.Time do
    @moduledoc false
    import Postgrex.BinaryUtils
    use Postgrex.BinaryExtension, [send: "time_send"]

    def encode(_, %Time{microsecond: {microsec, _}} = time, _, _) do
      sec =
        time
        |> Time.to_erl()
        |> :calendar.time_to_seconds()

      <<sec * 1_000_000 + microsec :: int64>>
    end
    def encode(type_info, value, _, _) do
      raise ArgumentError,
        Postgrex.Utils.encode_msg(type_info, value, Time)
    end

    def decode(_, <<microsec :: int64>>, _, _) do
      sec = div(microsec, 1_000_000)
      microsec = rem(microsec, 1_000_000)
      sec
      |> :calendar.seconds_to_time()
      |> Time.from_erl!(microsec)
    end
  end
end
