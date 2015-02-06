defmodule Postgrex.TimeZone do
  defstruct [:hour, :min, :sec]
end

defmodule Postgrex.Date do
  defstruct [:year, :month, :day]
end

defmodule Postgrex.Time do
  defstruct [:hour, :min, :sec, :timezone]
end

defmodule Postgrex.Timestamp do
  defstruct [:year, :month, :day, :hour, :min, :sec, :timezone]
end

defmodule Postgrex.Interval do
  defstruct [:year, :month, :day, :hour, :min, :sec]
end

