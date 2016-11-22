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

  def inline(_type_info, _types, _opts) do
    {__MODULE__, inline_encode(), inline_decode()}
  end

  defp inline_encode() do
    quote location: :keep do
      %Postgrex.Interval{months: months, days: days, secs: secs} ->
        microsecs = secs * 1_000_000
        <<16 :: int32, microsecs :: int64, days :: int32, months :: int32>>
      other ->
        raise ArgumentError, Postgrex.Utils.encode_msg(other, Postgrex.Interval)
    end
  end

  defp inline_decode() do
    quote location: :keep do
      <<16 :: int32, microsecs :: int64, days :: int32, months :: int32>> ->
        secs = div(microsecs, 1_000_000)
        %Postgrex.Interval{months: months, days: days, secs: secs}
    end
  end

  ## Helpers

  defp decode_interval(microsecs, days, months) do
    secs = div(microsecs, 1_000_000)
    %Postgrex.Interval{months: months, days: days, secs: secs}
  end
end
