defmodule Postgrex.Extensions.Text do
  use Behaviour
  alias Postgrex.TypeInfo

  def matching,
    do: [type: "void"]

  def format,
    do: :text

  def encode(%TypeInfo{type: "void"}, :void, _),
    do: ""

  def decode(%TypeInfo{type: "void"}, "", _),
    do: :void
end
