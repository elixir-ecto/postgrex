defmodule Postgrex.Extensions.Raw do
  @moduledoc false
  import Postgrex.BinaryUtils, warn: false
  use Postgrex.BinaryExtension,
    [send: "bpcharsend", send: "textsend", send: "varcharsend",
     send: "byteasend", send: "enum_send", send: "unknownsend",
     send: "citextsend", send: "charsend"]

  def init(opts), do: Keyword.fetch!(opts, :decode_binary)

  def encode(_) do
    quote location: :keep do
      bin when is_binary(bin) ->
        [<<byte_size(bin) :: int32>> | bin]
      other ->
        raise DBConnection.EncodeError, Postgrex.Utils.encode_msg(other, "a binary")
    end
  end

  def decode(:copy) do
    quote location: :keep do
      <<len :: int32, value :: binary-size(len)>> -> :binary.copy(value)
    end
  end
  def decode(:reference) do
    quote location: :keep do
      <<len :: int32, value :: binary-size(len)>> -> value
    end
  end
end
