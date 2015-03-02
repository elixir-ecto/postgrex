defmodule Postgrex.Date do
  @moduledoc """
  Struct for Postgres date.

  ## Fields
    * `year`
    * `month`
    * `day`
  """

  @type t :: %__MODULE__{year: 0..10000, month: 1..12, day: 1..12}

  defstruct [
    year: 0,
    month: 1,
    day: 1]
end

defmodule Postgrex.Time do
  @moduledoc """
  Struct for Postgres time.

  ## Fields
    * `hour`
    * `min`
    * `sec`
    * `usec`
  """

  @type t :: %__MODULE__{hour: 0..23, min: 0..59, sec: 0..59, usec: 0..999_999}

  defstruct [
    hour: 0,
    min: 0,
    sec: 0,
    usec: 0]
end

defmodule Postgrex.Timestamp do
  @moduledoc """
  Struct for Postgres timestamp.

  ## Fields
    * `year`
    * `month`
    * `day`
    * `hour`
    * `min`
    * `sec`
    * `músec`
  """

  @type t :: %__MODULE__{year: 0..10000, month: 1..12, day: 1..12,
                         hour: 0..23, min: 0..59, sec: 0..59, usec: 0..999_999}

  defstruct [
    year: 0,
    month: 1,
    day: 1,
    hour: 0,
    min: 0,
    sec: 0,
    usec: 0]
end

defmodule Postgrex.Interval do
  @moduledoc """
  Struct for Postgres interval.

  ## Fields
    * `months`
    * `days`
    * `secs`
  """

  @type t :: %__MODULE__{months: integer, days: integer, secs: integer}

  defstruct [
    months: 0,
    days: 0,
    secs: 0]
end

defmodule Postgrex.Range do
  @moduledoc """
  Struct for Postgres range.

  ## Fields
    * `lower`
    * `upper`
    * `lower_inclusive`
    * `upper_inclusive`
  """

  @type t :: %__MODULE__{lower: term, upper: term, lower_inclusive: boolean,
                         upper_inclusive: boolean}

  defstruct [
    lower: nil,
    upper: nil,
    lower_inclusive: true,
    upper_inclusive: true]
end
