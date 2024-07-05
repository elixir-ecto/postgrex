defmodule Postgrex.Extensions.Date do
  @moduledoc false
  import Postgrex.BinaryUtils, warn: false
  use Postgrex.BinaryExtension, send: "date_send"

  @gd_epoch Date.to_gregorian_days(~D[2000-01-01])

  # Elixir supports earlier dates but this is the
  # earliest supported in Postgresql.
  @min_days Date.to_gregorian_days(~D[-4713-01-01])

  def encode(_) do
    quote location: :keep do
      %Date{calendar: Calendar.ISO} = date ->
        unquote(__MODULE__).encode_elixir(date)

      other ->
        raise DBConnection.EncodeError, Postgrex.Utils.encode_msg(other, Date)
    end
  end

  def decode(_) do
    quote location: :keep do
      <<4::int32(), days::int32()>> ->
        unquote(__MODULE__).day_to_elixir(days)
    end
  end

  ## Helpers

  def encode_elixir(date) do
    <<4::int32(), Date.to_gregorian_days(date) - @gd_epoch::int32()>>
  end

  def day_to_elixir(days) do
    days = days + @gd_epoch

    if days > @min_days do
      Date.from_gregorian_days(days)
    else
      raise ArgumentError,
            "Postgrex can only decode dates with days after #{@min_days}, got: #{inspect(days)}"
    end
  end
end
