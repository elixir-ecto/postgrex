defmodule Postgrex.Parameters do
  @moduledoc false
  defstruct [parameters: nil]
  @type t :: %__MODULE__{parameters: nil | %{binary => binary}}
end

defimpl DBConnection.Query, for: Postgrex.Parameters do
  def parse(query, _), do: query
  def describe(query, _), do: query
  def encode(query, _), do: query
end

defimpl DBConnection.Result, for: Postgrex.Parameters do
  def decode(%{parameters: parameters}, _), do: parameters
end
