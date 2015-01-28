defmodule Postgrex.Extensions.Text do
  alias Postgrex.TypeInfo

  @behaviour Postgrex.Extension

  def matching,
    do: [type: "void"]

  def format,
    do: :text

  def encode(%TypeInfo{type: "void"}, :void, _),
    do: ""

  def decode(%TypeInfo{type: "void"}, "", _),
    do: :void
end
