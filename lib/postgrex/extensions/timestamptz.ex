defmodule Postgrex.Extensions.TimestampTZ do
  @moduledoc false
  import Postgrex.BinaryUtils, warn: false
  use Postgrex.BinaryExtension, send: "timestamptz_send"

  @gs_epoch NaiveDateTime.to_gregorian_seconds(~N[2000-01-01 00:00:00.0]) |> elem(0)
  @gs_unix_epoch NaiveDateTime.to_gregorian_seconds(~N[1970-01-01 00:00:00.0]) |> elem(0)
  @us_epoch (@gs_epoch - @gs_unix_epoch) * 1_000_000

  @plus_infinity 9_223_372_036_854_775_807
  @minus_infinity -9_223_372_036_854_775_808
  @default_precision 6
  # -1: user did not specify precision
  # nil: coming from a super type that does not pass modifier for sub-type
  @unspecified_precision [-1, nil]

  def init(opts), do: Keyword.get(opts, :allow_infinite_timestamps, false)

  def encode(_) do
    quote location: :keep do
      %DateTime{calendar: Calendar.ISO} = dt ->
        unquote(__MODULE__).encode_elixir(dt)

      other ->
        raise DBConnection.EncodeError,
              Postgrex.Utils.encode_msg(other, DateTime)
    end
  end

  def decode(infinity?) do
    quote location: :keep do
      <<8::int32(), microsecs::int64()>> ->
        unquote(__MODULE__).microsecond_to_elixir(microsecs, var!(mod), unquote(infinity?))
    end
  end

  ## Helpers

  def encode_elixir(%DateTime{utc_offset: 0, std_offset: 0} = datetime) do
    microsecs = DateTime.to_unix(datetime, :microsecond)
    <<8::int32(), microsecs - @us_epoch::int64()>>
  end

  def encode_elixir(%DateTime{} = datetime) do
    raise ArgumentError, "#{inspect(datetime)} is not in UTC"
  end

  def microsecond_to_elixir(@plus_infinity, _precision, infinity?) do
    if infinity?, do: :inf, else: raise_infinity("infinity")
  end

  def microsecond_to_elixir(@minus_infinity, _precision, infinity?) do
    if infinity?, do: :"-inf", else: raise_infinity("-infinity")
  end

  def microsecond_to_elixir(microsecs, precision, _infinity) do
    split(microsecs, precision)
  end

  defp split(microsecs, precision) when microsecs < 0 and rem(microsecs, 1_000_000) != 0 do
    secs = div(microsecs, 1_000_000) - 1
    microsecs = 1_000_000 + rem(microsecs, 1_000_000)
    split(secs, microsecs, precision)
  end

  defp split(microsecs, precision) do
    secs = div(microsecs, 1_000_000)
    microsecs = rem(microsecs, 1_000_000)
    split(secs, microsecs, precision)
  end

  defp split(secs, microsecs, precision) do
    precision = if precision in @unspecified_precision, do: @default_precision, else: precision
    DateTime.from_gregorian_seconds(secs + @gs_epoch, {microsecs, precision})
  end

  defp raise_infinity(type) do
    raise ArgumentError, """
    got \"#{type}\" from PostgreSQL. If you want to support infinity timestamps \
    in your application, you can enable them by defining your own types:

        Postgrex.Types.define(MyApp.PostgrexTypes, [], allow_infinite_timestamps: true)

    And then configuring your database to use it:

        types: MyApp.PostgrexTypes
    """
  end
end
