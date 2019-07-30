defmodule Postgrex.Extensions.Interval do
  @moduledoc false
  import Postgrex.BinaryUtils, warn: false
  use Postgrex.BinaryExtension, send: "interval_send"

  def encode(_) do
    quote location: :keep do
      %Postgrex.Interval{months: months, days: days, secs: secs, microsecs: microsecs} ->
        microsecs = secs * 1_000_000 + microsecs
        <<16::int32, microsecs::int64, days::int32, months::int32>>

      other ->
        raise DBConnection.EncodeError, Postgrex.Utils.encode_msg(other, Postgrex.Interval)
    end
  end

  def decode(_) do
    quote location: :keep do
      <<16::int32, microsecs::int64, days::int32, months::int32>> ->
        secs = div(microsecs, 1_000_000)
        microsecs = rem(microsecs, 1_000_000)
        %Postgrex.Interval{months: months, days: days, secs: secs, microsecs: microsecs}
    end
  end
end
