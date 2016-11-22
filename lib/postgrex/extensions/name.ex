defmodule Postgrex.Extensions.Name do
  @moduledoc false
  use Postgrex.BinaryExtension, send: "namesend"

  def init(_, opts), do: Keyword.fetch!(opts, :decode_binary)

  def encode(_, bin, _, _) when is_binary(bin) and byte_size(bin) < 64,
    do: bin

  def encode(type_info, value, _, _) do
    raise ArgumentError,
      Postgrex.Utils.encode_msg(type_info, value, "a binary string of less than 64 bytes")
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
      name when is_binary(name) and byte_size(name) < 64 ->
        [<<byte_size(name) :: int32>> | name]
      other ->
        msg = "a binary string of less than 64 bytes"
        raise ArgumentError, Postgrex.Utils.encode_msg(other, msg)
    end
  end

  defp inline_decode(:reference) do
    quote location: :keep do
      <<len :: int32, name :: binary-size(len)>> -> name
    end
  end
  defp inline_decode(:copy) do
    quote location: :keep do
      <<len :: int32, name :: binary-size(len)>> -> :binary.copy(name)
    end
  end
end
