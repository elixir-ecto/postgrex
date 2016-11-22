defmodule Postgrex.Extensions.UUID do
  @moduledoc false
  import Postgrex.BinaryUtils
  use Postgrex.BinaryExtension, send: "uuid_send"

  def init(_, opts), do: Keyword.fetch!(opts, :decode_binary)

  def encode(_, <<_ :: binary(16)>> = bin, _, _),
    do: bin
  def encode(type_info, value, _, _) do
    raise ArgumentError,
      Postgrex.Utils.encode_msg(type_info, value, "a binary of 16 bytes")
  end

  def decode(_, bin, _, :copy),
    do: :binary.copy(bin)
  def decode(_, bin, _, :reference),
    do: bin

  def inline(_type_info, _types, opts) do
    {__MODULE__, inline_encode(), inline_decode(opts)}
  end

  defp inline_encode() do
    quote location: :keep do
      uuid when is_binary(uuid) and byte_size(uuid) == 16 ->
        [<<16 :: int32>> | uuid]
      other ->
        raise ArgumentError,
          Postgrex.Utils.encode_msg(other, "a binary of 16 bytes")
    end
  end

  defp inline_decode(:copy) do
    quote location: :keep do
      <<16 :: int32, uuid :: binary-16>> -> :binary.copy(uuid)
    end
  end
  defp inline_decode(:reference) do
    quote location: :keep do
      <<16 :: int32, uuid :: binary-16>> -> uuid
    end
  end
end
