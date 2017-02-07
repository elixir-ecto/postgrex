defmodule Postgrex.Extensions.Name do
  @moduledoc false
  import Postgrex.BinaryUtils, warn: false
  use Postgrex.BinaryExtension, send: "namesend"

  def init(opts), do: Keyword.fetch!(opts, :decode_binary)

  def encode(_) do
    quote location: :keep do
      name when is_binary(name) and byte_size(name) < 64 ->
        [<<byte_size(name) :: int32>> | name]
      other ->
        msg = "a binary string of less than 64 bytes"
        raise ArgumentError, Postgrex.Utils.encode_msg(other, msg)
    end
  end

  def decode(:reference) do
    quote location: :keep do
      <<len :: int32, name :: binary-size(len)>> -> name
    end
  end
  def decode(:copy) do
    quote location: :keep do
      <<len :: int32, name :: binary-size(len)>> -> :binary.copy(name)
    end
  end
end
