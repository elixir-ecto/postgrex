defmodule Postgrex.Extensions.Date do
  @moduledoc false
  import Postgrex.BinaryUtils, warn: false
  use Postgrex.BinaryExtension, send: "date_send"

  @gd_epoch :calendar.date_to_gregorian_days({2000, 1, 1})
  @max_year 9999
  @max_days 3_652_424

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
      <<4::int32, days::int32>> ->
        unquote(__MODULE__).day_to_elixir(days)
    end
  end

  ## Helpers

  def encode_elixir(%Date{year: year, month: month, day: day}) when year <= @max_year do
    date = {year, month, day}
    <<4::int32, :calendar.date_to_gregorian_days(date) - @gd_epoch::int32>>
  end

  def encode_elixir(%Date{} = date) do
    raise ArgumentError, "#{inspect(date)} is beyond the maximum year #{@max_year}"
  end

  def day_to_elixir(days) do
    days = days + @gd_epoch

    if days in 0..@max_days do
      days
      |> :calendar.gregorian_days_to_date()
      |> Date.from_erl!()
    else
      raise ArgumentError,
            "Postgrex can only decode dates with days between 0 and #{@max_days}, " <>
              "got: #{inspect(days)}"
    end
  end
end
