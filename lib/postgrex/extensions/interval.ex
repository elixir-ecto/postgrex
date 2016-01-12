defmodule Postgrex.Extensions.Interval do
  @moduledoc false
  import Postgrex.BinaryUtils
  use Postgrex.BinaryExtension, send: "interval_send"

  def encode(_, %Postgrex.Interval{months: months, days: days, secs: secs}, _, _) do
    microsecs = secs * 1_000_000
    <<microsecs :: int64, days :: int32, months :: int32>>
  end
  def encode(type_info, value, _, _) do
    raise ArgumentError,
      Postgrex.Utils.encode_msg(type_info, value, Postgrex.Interval)
  end

  def decode(_, <<s :: int64, d :: int32, m :: int32>>, _, _),
    do: decode_interval(s, d, m)

  ## Helpers

  defp decode_interval(microsecs, days, months) do
    secs = div(microsecs, 1_000_000)
    %Postgrex.Interval{months: months, days: days, secs: secs}
  end
end
