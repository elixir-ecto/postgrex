defmodule Postgrex.Extensions.Raw do
  @moduledoc false
  use Postgrex.BinaryExtension,
    [send: "bpcharsend", send: "textsend", send: "varcharsend",
      send: "byteasend", send: "enum_send", send: "unknownsend",
      type: "citext"]

  def encode(_, bin, _, _) when is_binary(bin),
    do: bin
  def encode(type_info, value, _, _) do
    raise ArgumentError, Postgrex.Utils.encode_msg(type_info, value, "a binary")
  end

  def decode(_, bin, _, _),
    do: bin
end
