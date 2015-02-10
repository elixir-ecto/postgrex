defmodule Postgrex.TimeZone do
  defstruct [
    hour: 0,
    min: 0,
    sec: 0]
end

defmodule Postgrex.Date do
  defstruct [
    year: 1,
    month: 1,
    day: 1,
    ad: true]
end

defmodule Postgrex.Time do
  defstruct [
    hour: 0,
    min: 0,
    sec: 0,
    timezone: nil]
end

defmodule Postgrex.Timestamp do
  defstruct [
    year: 1,
    month: 1,
    day: 1,
    hour: 0,
    min: 0,
    sec: 0,
    ad: true,
    timezone: nil]
end

defmodule Postgrex.Interval do
  defstruct [
    year: 0,
    month: 0,
    day: 0,
    hour: 0,
    min: 0,
    sec: 0]
end

defmodule Postgrex.Range do
  defstruct [
    lower: nil,
    upper: nil,
    lower_inclusive: true,
    upper_inclusive: true]
end
