defmodule Postgrex.Extensions.Time do
  @moduledoc false
  import Postgrex.BinaryUtils, warn: false
  use Postgrex.BinaryExtension, send: "time_send"

  @default_precision 6

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
    # use the default precision if the precision modifier from postgres is -1 (this means no precision specified)
    # or if the precision is missing because we are in a super type which does not give us the sub-type's modifier
    precision = if precision in [-1, nil], do: @default_precision, else: precision

    sec = div(microsec, 1_000_000)
    microsec = rem(microsec, 1_000_000)

    sec
    |> :calendar.seconds_to_time()
    |> Time.from_erl!({microsec, precision})
  end
end
