defmodule Postgrex.Date do
  @moduledoc """
  Struct for Postgres date.

  ## Fields
    * `year`
    * `month`
    * `day`
  """

  @type t :: %__MODULE__{year: 0..10000, month: 1..12, day: 1..31}

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
    * `usec`
  """

  @type t :: %__MODULE__{year: 0..10000, month: 1..12, day: 1..31,
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

defmodule Postgrex.INET do
  @moduledoc """
  Struct for Postgres inet.

  ## Fields
    * `address`
  """

  @type t :: %__MODULE__{address: :inet.ip_address}

  defstruct [address: nil]
end

defmodule Postgrex.CIDR do
  @moduledoc """
  Struct for Postgres cidr.

  ## Fields
    * `address`
    * `netmask`
  """

  @type t :: %__MODULE__{address: :inet.ip_address,
                         netmask: 0..128}

  defstruct [
    address: nil,
    netmask: nil]
end

defmodule Postgrex.MACADDR do
  @moduledoc """
  Struct for Postgres macaddr.

  ## Fields
    * `address`
  """

  @type macaddr :: {0..255, 0..255, 0..255, 0..255, 0..255, 0..255}

  @type t :: %__MODULE__{address: macaddr }

  defstruct [address: nil]
end

defmodule Postgrex.Point do
  @moduledoc """
  Struct for Postgres point.

  ## Fields
    * `x`
    * `y`
  """
  @type t :: %__MODULE__{x: float, y: float}

  defstruct [
    x: nil,
    y: nil]
end

defmodule Postgrex.Polygon do
  @moduledoc """
  Struct for Postgres polygon.

  ## Fields
    * `vertices`
  """
  @type t :: %__MODULE__{vertices: [Postgrex.Point.t]}

  defstruct [vertices: nil]
end

defmodule Postgrex.Line do
  @moduledoc """
  Struct for Postgres line.

  Note, lines are stored in Postgres in the form `{a,b,c}`, which
  parameterizes a line as `a*x + b*y + c = 0`.

  ## Fields
    * `a`
    * `b`
    * `c`
  """
  @type t :: %__MODULE__{a: float, b: float, c: float}

  defstruct [
    a: nil,
    b: nil,
    c: nil]
end

defmodule Postgrex.LineSegment do
  @moduledoc """
  Struct for Postgres line segment.

  ## Fields
    * `point1`
    * `point2`
  """
  @type t :: %__MODULE__{point1: Postgrex.Point.t, point2: Postgrex.Point.t}

  defstruct [
    point1: nil,
    point2: nil
  ]
end

defmodule Postgrex.Box do
  @moduledoc """
  Struct for Postgres rectangular box.

  ## Fields
    * `upper_right`
    * `bottom_left`
  """
  @type t :: %__MODULE__{
    upper_right: Postgrex.Point.t,
    bottom_left: Postgrex.Point.t
  }

  defstruct [
    upper_right: nil,
    bottom_left: nil
  ]
end

defmodule Postgrex.Path do
  @moduledoc """
  Struct for Postgres path.

  ## Fields
    * `open`
    * `points`
  """
  @type t :: %__MODULE__{points: [Postgrex.Point.t], open: boolean}

  defstruct [
    points: nil,
    open: nil
  ]
end

defmodule Postgrex.Circle do
  @moduledoc """
  Struct for Postgres circle.

  ## Fields
    * `center`
    * `radius`
  """
  @type t :: %__MODULE__{center: Postgrex.Point.t, radius: number}

  defstruct [
    center: nil,
    radius: nil
  ]
end

defmodule Postgrex.Lexeme do
  @moduledoc """
  Struct for Postgres Lexeme (A Tsvector type is composed of multiple lexemes)

  ## Fields
    * `word`
    * `positions`
  """

  @type t :: %__MODULE__{word: String.t, positions: [{pos_integer, :A | :B | :C | nil}]}

  defstruct [
    word: nil,
    positions: nil
  ]
end
