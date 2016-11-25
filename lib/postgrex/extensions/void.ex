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

  def encode(_) do
    quote location: :keep do
      :void ->
        <<0 :: int32>>
      other ->
        raise ArgumentError, Postgrex.Utils.encode_msg(other, "the atom :void")
    end
  end

  def decode(_) do
    quote location: :keep do
      <<0 :: int32>> -> :void
    end
  end
end
