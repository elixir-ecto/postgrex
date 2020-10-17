defmodule Postgrex.Interval do
  @moduledoc """
  Struct for PostgreSQL `interval`.

  ## Fields

    * `months`
    * `days`
    * `secs`
    * `microsecs`

  """

  @type t :: %__MODULE__{months: integer, days: integer, secs: integer, microsecs: integer}

  defstruct months: 0, days: 0, secs: 0, microsecs: 0

  def compare(
        %__MODULE__{months: m1, days: d1, secs: s1, microsecs: ms1},
        %__MODULE__{months: m2, days: d2, secs: s2, microsecs: ms2}
      ) do
    t1 = {m1, d1, s1, ms1}
    t2 = {m2, d2, s2, ms2}

    cond do
      t1 > t2 -> :gt
      t1 < t2 -> :lt
      true -> :eq
    end
  end

  def to_string(%__MODULE__{months: months, days: days, secs: secs, microsecs: microsecs}) do
    optional_interval(months, :month) <>
      optional_interval(days, :day) <>
      Integer.to_string(secs) <>
      optional_microsecs(microsecs) <>
      " seconds"
  end

  defp optional_interval(0, _), do: ""
  defp optional_interval(1, key), do: "1 #{key}, "
  defp optional_interval(n, key), do: "#{n} #{key}s, "

  defp optional_microsecs(0),
    do: ""

  defp optional_microsecs(ms),
    do: "." <> (ms |> Integer.to_string() |> String.pad_leading(6, "0"))
end

defmodule Postgrex.Range do
  @moduledoc """
  Struct for PostgreSQL `range`.

  ## Fields

    * `lower`
    * `upper`
    * `lower_inclusive`
    * `upper_inclusive`

  """

  @type t :: %__MODULE__{
          lower: term | :empty | :unbound,
          upper: term | :empty | :unbound,
          lower_inclusive: boolean,
          upper_inclusive: boolean
        }

  defstruct lower: nil, upper: nil, lower_inclusive: true, upper_inclusive: true
end

defmodule Postgrex.INET do
  @moduledoc """
  Struct for PostgreSQL `inet` / `cidr`.

  ## Fields

    * `address`
    * `netmask`

  """

  @type t :: %__MODULE__{address: :inet.ip_address(), netmask: nil | 0..128}

  defstruct address: nil, netmask: nil
end

defmodule Postgrex.MACADDR do
  @moduledoc """
  Struct for PostgreSQL `macaddr`.

  ## Fields

    * `address`

  """

  @type macaddr :: {0..255, 0..255, 0..255, 0..255, 0..255, 0..255}

  @type t :: %__MODULE__{address: macaddr}

  defstruct address: nil
end

defmodule Postgrex.Point do
  @moduledoc """
  Struct for PostgreSQL `point`.

  ## Fields

    * `x`
    * `y`

  """

  @type t :: %__MODULE__{x: float, y: float}

  defstruct x: nil, y: nil
end

defmodule Postgrex.Polygon do
  @moduledoc """
  Struct for PostgreSQL `polygon`.

  ## Fields

    * `vertices`

  """

  @type t :: %__MODULE__{vertices: [Postgrex.Point.t()]}

  defstruct vertices: nil
end

defmodule Postgrex.Line do
  @moduledoc """
  Struct for PostgreSQL `line`.

  Note, lines are stored in PostgreSQL in the form `{a, b, c}`, which
  parameterizes a line as `a*x + b*y + c = 0`.

  ## Fields

    * `a`
    * `b`
    * `c`

  """

  @type t :: %__MODULE__{a: float, b: float, c: float}

  defstruct a: nil, b: nil, c: nil
end

defmodule Postgrex.LineSegment do
  @moduledoc """
  Struct for PostgreSQL `lseg`.

  ## Fields

    * `point1`
    * `point2`

  """

  @type t :: %__MODULE__{point1: Postgrex.Point.t(), point2: Postgrex.Point.t()}

  defstruct point1: nil, point2: nil
end

defmodule Postgrex.Box do
  @moduledoc """
  Struct for PostgreSQL `box`.

  ## Fields

    * `upper_right`
    * `bottom_left`

  """

  @type t :: %__MODULE__{
          upper_right: Postgrex.Point.t(),
          bottom_left: Postgrex.Point.t()
        }

  defstruct upper_right: nil, bottom_left: nil
end

defmodule Postgrex.Path do
  @moduledoc """
  Struct for PostgreSQL `path`.

  ## Fields

    * `open`
    * `points`

  """

  @type t :: %__MODULE__{points: [Postgrex.Point.t()], open: boolean}

  defstruct points: nil, open: nil
end

defmodule Postgrex.Circle do
  @moduledoc """
  Struct for PostgreSQL `circle`.

  ## Fields

    * `center`
    * `radius`

  """
  @type t :: %__MODULE__{center: Postgrex.Point.t(), radius: number}

  defstruct center: nil, radius: nil
end

defmodule Postgrex.Lexeme do
  @moduledoc """
  Struct for PostgreSQL `lexeme`.

  ## Fields

    * `word`
    * `positions`

  """

  @type t :: %__MODULE__{word: String.t(), positions: [{pos_integer, :A | :B | :C | nil}]}

  defstruct word: nil, positions: nil
end
