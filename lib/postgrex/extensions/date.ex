defmodule Postgrex.Extensions.Date do
  @moduledoc false
  import Postgrex.BinaryUtils
  use Postgrex.BinaryExtension, send: "date_send"

  @gd_epoch :calendar.date_to_gregorian_days({2000, 1, 1})
  @date_max_year 5874897

  def encode(_, %Postgrex.Date{} = date, _, _) do
    encode(date)
  end
  def encode(type_info, value, _, _) do
    raise ArgumentError
      Postgrex.Utils.encode_msg(type_info, value, Postgrex.Date)
  end

  def encode(%Postgrex.Date{year: year, month: month, day: day}, _, _)
      when year <= @date_max_year do
    date = {year, month, day}
    <<:calendar.date_to_gregorian_days(date) - @gd_epoch :: int32>>
  end
  def encode(%Postgrex.Date{} = date) do
    raise ArgumentError, "#{inspect date} has date above #{@date_max_year}"
  end

  def decode(_, <<days :: int32>>, _, _) do
    days_to_date(days)
  end

  def days_to_date(days) do
    {year, month, day} = :calendar.gregorian_days_to_date(days + @gd_epoch)
    %Postgrex.Date{year: year, month: month, day: day}
  end

  def inline(_type_info, _types, _opts) do
    {__MODULE__, inline_encode(), inline_decode()}
  end

  defp inline_encode() do
    quote location: :keep do
      %Postgrex.Date{} = date ->
        unquote(__MODULE__).encode(date)
      other ->
        raise ArgumentError, Postgrex.Utils.encode_msg(other, Postgrex.Date)
    end
  end

  defp inline_decode() do
    quote location: :keep do
      <<4 :: int32, days :: int32>> ->
        unquote(__MODULE__).days_to_date(days)
    end
  end
end
