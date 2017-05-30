defmodule Postgrex.Extensions.Date do
  @moduledoc false
  import Postgrex.BinaryUtils, warn: false
  use Postgrex.BinaryExtension, send: "date_send"

  @gd_epoch :calendar.date_to_gregorian_days({2000, 1, 1})
  @max_year 5874897

  def encode(_) do
    quote location: :keep do
      %Date{} = date ->
        unquote(__MODULE__).encode_elixir(date)
      other ->
        raise ArgumentError, Postgrex.Utils.encode_msg(other, Date)
    end
  end
  def decode(_) do
    quote location: :keep do
      <<4 :: int32, days :: int32>> ->
        unquote(__MODULE__).day_to_elixir(days)
    end
  end

  ## Helpers

  def encode_elixir(%Date{year: year, month: month, day: day})
      when year <= @max_year do
    date = {year, month, day}
    <<4 :: int32, :calendar.date_to_gregorian_days(date) - @gd_epoch :: int32>>
  end
  def encode_elixir(%Date{} = date) do
    raise ArgumentError,
      "#{inspect date} is beyond the maximum year #{@max_year}"
  end

  def day_to_elixir(days) do
    days
    |> erl_date()
    |> Date.from_erl!()
  end

  defp erl_date(days) do
    :calendar.gregorian_days_to_date(days + @gd_epoch)
  end
end
