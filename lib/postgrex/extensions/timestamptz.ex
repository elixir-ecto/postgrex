defmodule Postgrex.Extensions.TimestampTZ do
  @moduledoc false
  import Postgrex.BinaryUtils, warn: false
  use Postgrex.BinaryExtension, send: "timestamptz_send"

  @gs_epoch NaiveDateTime.to_gregorian_seconds(~N[2000-01-01 00:00:00.0]) |> elem(0)

  @gs_unix_epoch NaiveDateTime.to_gregorian_seconds(~N[1970-01-01 00:00:00.0]) |> elem(0)
  @us_epoch (@gs_epoch - @gs_unix_epoch) * 1_000_000

  @gs_max elem(NaiveDateTime.to_gregorian_seconds(~N[9999-01-01 00:00:00.0]), 0) - @gs_unix_epoch
  @us_max @gs_max * 1_000_000

  @gs_min elem(NaiveDateTime.to_gregorian_seconds(~N[-4713-01-01 00:00:00.0]), 0) - @gs_unix_epoch
  @us_min @gs_min * 1_000_000

  @plus_infinity 9_223_372_036_854_775_807
  @minus_infinity -9_223_372_036_854_775_808

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
        unquote(__MODULE__).microsecond_to_elixir(microsecs, unquote(infinity?))
    end
  end

  ## Helpers

  def encode_elixir(%DateTime{utc_offset: 0, std_offset: 0} = datetime) do
    case DateTime.to_unix(datetime, :microsecond) do
      microsecs when microsecs in @us_min..@us_max ->
        <<8::int32(), microsecs - @us_epoch::int64()>>

      _ ->
        raise ArgumentError, "#{inspect(datetime)} is not in the year range -4713..9999"
    end
  end

  def encode_elixir(%DateTime{} = datetime) do
    raise ArgumentError, "#{inspect(datetime)} is not in UTC"
  end

  def microsecond_to_elixir(@plus_infinity, infinity?) do
    if infinity?, do: :inf, else: raise_infinity("infinity")
  end

  def microsecond_to_elixir(@minus_infinity, infinity?) do
    if infinity?, do: :"-inf", else: raise_infinity("-infinity")
  end

  def microsecond_to_elixir(microsecs, _infinity) do
    DateTime.from_unix!(microsecs + @us_epoch, :microsecond)
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
