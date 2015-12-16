defmodule Postgrex.Parameters do
  @moduledoc false
  defstruct []
  @type t :: %__MODULE__{}
end

defimpl DBConnection.Query, for: Postgrex.Parameters do
  def parse(query, _), do: query
  def describe(query, _), do: query
  def encode(_, nil, _), do: nil
  def decode(_, parameters, _), do: parameters
end
