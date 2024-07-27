defmodule Postgrex.Extensions.Time do
  @moduledoc false
  import Postgrex.BinaryUtils, warn: false
  use Postgrex.BinaryExtension, send: "time_send"

  @default_precision 6
  # -1: user did not specify precision
  # nil: coming from a super type that does not pass modifier for sub-type
  @unspecified_precision [-1, nil]

  def encode(_) do
    quote location: :keep do
      %Time{calendar: Calendar.ISO} = time ->
        unquote(__MODULE__).encode_elixir(time)

      other ->
        raise DBConnection.EncodeError, Postgrex.Utils.encode_msg(other, Time)
    end
  end

  def decode(_) do
    quote location: :keep do
      <<8::int32(), microsecs::int64()>> ->
        unquote(__MODULE__).microsecond_to_elixir(microsecs, var!(mod))
    end
  end

  ## Helpers

  def encode_elixir(%Time{hour: hour, minute: min, second: sec, microsecond: {usec, _}})
      when hour in 0..23 and min in 0..59 and sec in 0..59 and usec in 0..999_999 do
    time = {hour, min, sec}
    <<8::int32(), :calendar.time_to_seconds(time) * 1_000_000 + usec::int64()>>
  end

  def microsecond_to_elixir(microsec, precision) do
    precision = if precision in @unspecified_precision, do: @default_precision, else: precision
    sec = div(microsec, 1_000_000)
    microsec = rem(microsec, 1_000_000)

    sec
    |> :calendar.seconds_to_time()
    |> Time.from_erl!({microsec, precision})
  end
end
