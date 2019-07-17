defmodule Postgrex.Extensions.TimestampTZ do
  @moduledoc false
  import Postgrex.BinaryUtils, warn: false
  use Postgrex.BinaryExtension, send: "timestamptz_send"

  @gs_epoch :calendar.datetime_to_gregorian_seconds({{2000, 1, 1}, {0, 0, 0}})
  @max_year 294_276

  @gs_unix_epoch :calendar.datetime_to_gregorian_seconds({{1970, 1, 1}, {0, 0, 0}})
  @us_epoch (@gs_epoch - @gs_unix_epoch) * 1_000_000
  @gs_max :calendar.datetime_to_gregorian_seconds({{@max_year + 1, 1, 1}, {0, 0, 0}}) -
            @gs_unix_epoch
  @us_max @gs_max * 1_000_000

  def encode(_) do
    quote location: :keep do
      %DateTime{calendar: Calendar.ISO} = dt ->
        unquote(__MODULE__).encode_elixir(dt)

      other ->
        raise DBConnection.EncodeError,
              Postgrex.Utils.encode_msg(other, DateTime)
    end
  end

  def decode(_) do
    quote location: :keep do
      <<8::int32, microsecs::int64>> ->
        unquote(__MODULE__).microsecond_to_elixir(microsecs)
    end
  end

  ## Helpers

  def encode_elixir(%DateTime{utc_offset: 0, std_offset: 0} = datetime) do
    case DateTime.to_unix(datetime, :microsecond) do
      microsecs when microsecs < @us_max ->
        <<8::int32, microsecs - @us_epoch::int64>>

      _ ->
        raise ArgumentError, "#{inspect(datetime)} is beyond the maximum year 294276"
    end
  end

  def encode_elixir(%DateTime{} = datetime) do
    raise ArgumentError, "#{inspect(datetime)} is not in UTC"
  end

  def microsecond_to_elixir(microsecs) do
    DateTime.from_unix!(microsecs + @us_epoch, :microsecond)
  end
end
