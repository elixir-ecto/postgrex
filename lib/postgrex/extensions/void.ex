defmodule Postgrex.Extensions.Void do
  @behaviour Postgrex.Extension

  def init(parameters, _opts) do
    case parameters["server_version"] |> Postgrex.Utils.parse_version do
      version when version >= {9, 1, 0} ->
        :binary
      _ ->
        :text
    end
  end

  def matching(:binary), do: [send: "void_send"]
  def matching(:text), do: [output: "void_out"]

  def format(format), do: format

  def encode(_, :void, _, _),
    do: ""
  def encode(type_info, value, _, _) do
    raise ArgumentError,
      Postgrex.Utils.encode_msg(type_info, value, "the atom :void")
  end

  def decode(_, "", _, _),
    do: :void
end
