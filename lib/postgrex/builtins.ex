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
  require Decimal
  @type t :: %__MODULE__{x: float, y: float}

  defstruct [
    x: nil,
    y: nil]
end
