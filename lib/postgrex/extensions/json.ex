defmodule Postgrex.Extensions.JSON do
  @moduledoc """
  An extension that supports the `json` and `jsonb` types.

  This extension is not used by default, it needs to be included in the
  `:extensions` option to `Postgrex.Connection.start_link/1`.
  """

  alias Postgrex.TypeInfo

  @behaviour Postgrex.Extension

  def init(_parameters, opts),
    do: Keyword.fetch!(opts, :library)

  def matching(_library),
    do: [type: "json", type: "jsonb"]

  def format(_library),
    do: :binary

  def encode(%TypeInfo{type: "json"}, map, _state, library),
    do: library.encode!(map)
  def encode(%TypeInfo{type: "jsonb"}, map, _state, library),
    do: <<1, library.encode!(map)::binary>>

  def decode(%TypeInfo{type: "json"}, json, _state, library),
    do: library.decode!(json)
  def decode(%TypeInfo{type: "jsonb"}, <<1, json::binary>>, _state, library),
    do: library.decode!(json)
end
