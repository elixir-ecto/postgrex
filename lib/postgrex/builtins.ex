defmodule Postgrex.Date do
  defstruct [
    year: 0,
    month: 1,
    day: 1]
end

defmodule Postgrex.Time do
  defstruct [
    hour: 0,
    min: 0,
    sec: 0]
end

defmodule Postgrex.Timestamp do
  defstruct [
    year: 0,
    month: 1,
    day: 1,
    hour: 0,
    min: 0,
    sec: 0]
end

defmodule Postgrex.Interval do
  defstruct [
    months: 0,
    days: 0,
    secs: 0]
end

defmodule Postgrex.Range do
  defstruct [
    lower: nil,
    upper: nil,
    lower_inclusive: true,
    upper_inclusive: true]
end
