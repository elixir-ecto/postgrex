defmodule Postgrex.Extensions.Raw do
  @moduledoc false
  use Postgrex.BinaryExtension,
    [send: "bpcharsend", send: "textsend", send: "varcharsend",
     send: "byteasend", send: "enum_send", send: "unknownsend",
     send: "citextsend", send: "charsend"]

  def init(_, opts), do: Keyword.fetch!(opts, :decode_binary)

  def encode(_, bin, _, _) when is_binary(bin),
    do: bin
  def encode(type_info, value, _, _) do
    raise ArgumentError, Postgrex.Utils.encode_msg(type_info, value, "a binary")
  end

  def decode(_, bin, _, :reference),
    do: bin
  def decode(_, bin, _, :copy),
    do: :binary.copy(bin)

  def inline(_type_info, _types, opts) do
    {__MODULE__, inline_encode(), inline_decode(opts)}
  end

  defp inline_encode() do
    quote location: :keep do
      bin when is_binary(bin) ->
        [<<byte_size(bin) :: int32>> | bin]
      other ->
        raise ArgumentError, Postgrex.Utils.encode_msg(other, "a binary")
    end
  end

  defp inline_decode(:copy) do
    quote location: :keep do
      <<len :: int32, value :: binary-size(len)>> -> :binary.copy(value)
    end
  end
  defp inline_decode(:reference) do
    quote location: :keep do
      <<len :: int32, value :: binary-size(len)>> -> value
    end
  end
end
