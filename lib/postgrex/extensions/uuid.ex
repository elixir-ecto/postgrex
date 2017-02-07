defmodule Postgrex.Extensions.UUID do
  @moduledoc false
  import Postgrex.BinaryUtils, warn: false
  use Postgrex.BinaryExtension, send: "uuid_send"

  def init(opts), do: Keyword.fetch!(opts, :decode_binary)

  def encode(_) do
    quote location: :keep do
      uuid when is_binary(uuid) and byte_size(uuid) == 16 ->
        [<<16 :: int32>> | uuid]
      other ->
        raise ArgumentError,
          Postgrex.Utils.encode_msg(other, "a binary of 16 bytes")
    end
  end

  def decode(:copy) do
    quote location: :keep do
      <<16 :: int32, uuid :: binary-16>> -> :binary.copy(uuid)
    end
  end
  def decode(:reference) do
    quote location: :keep do
      <<16 :: int32, uuid :: binary-16>> -> uuid
    end
  end
end
