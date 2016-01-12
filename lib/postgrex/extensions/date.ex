defmodule Postgrex.Extensions.Date do
  @moduledoc false
  import Postgrex.BinaryUtils
  use Postgrex.BinaryExtension, send: "date_send"

  @gd_epoch :calendar.date_to_gregorian_days({2000, 1, 1})
  @date_max_year 5874897

  def encode(_, %Postgrex.Date{year: year, month: month, day: day}, _, _)
  when year <= @date_max_year do
    date = {year, month, day}
    <<:calendar.date_to_gregorian_days(date) - @gd_epoch :: int32>>
  end
  def encode(type_info, value, _, _) do
    raise ArgumentError,
      Postgrex.Utils.encode_msg(type_info, value, Postgrex.Date)
  end

  def decode(_, <<days :: int32>>, _, _) do
    {year, month, day} = :calendar.gregorian_days_to_date(days + @gd_epoch)
    %Postgrex.Date{year: year, month: month, day: day}
  end
end
