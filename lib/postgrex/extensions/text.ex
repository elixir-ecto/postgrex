defmodule Postgrex.Extensions.Text do
  alias Postgrex.TypeInfo

  @behaviour Postgrex.Extension

  def init(opts),
    do: opts

  def matching(_),
    do: [type: "void"]

  def format(_),
    do: :text

  def encode(%TypeInfo{type: "void"}, :void, _, _),
    do: ""

  def decode(%TypeInfo{type: "void"}, "", _, _),
    do: :void
end
