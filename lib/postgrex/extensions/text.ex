defmodule Postgrex.Extensions.Text do
  alias Postgrex.TypeInfo

  @behaviour Postgrex.Extension

  # TODO: array and record

  def init(parameters, _opts),
    do: parameters["server_version"] |> Postgrex.Utils.version_to_int

  def matching(version) when version < 90_100,
    do: [output: "void_out"]

  def matching(_),
    do: []

  def format(_),
    do: :text

  def encode(%TypeInfo{output: "void_out"}, :void, _, _),
    do: ""

  def decode(%TypeInfo{output: "void_out"}, "", _, _),
    do: :void
end
