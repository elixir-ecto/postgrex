defmodule Postgrex.Extensions.Ltree do
  @moduledoc false
  import Postgrex.BinaryUtils, warn: false
  use Postgrex.BinaryExtension, send: "ltree_send"

  def init(opts), do: Keyword.fetch!(opts, :decode_binary)

  def encode(_state) do
    quote location: :keep, generated: true do
      bin when is_binary(bin) ->
        # ltree binary formats are versioned
        # see: https://github.com/postgres/postgres/blob/master/contrib/ltree/ltree_io.c
        version = 1
        size = byte_size(bin) + 1
        [<<size::signed-size(32), version::int8()>> | bin]
    end
  end

  def decode(:reference) do
    quote location: :keep do
      <<len::int32(), bin::binary-size(len)>> ->
        <<_version::int8(), ltree::binary>> = bin
        ltree
    end
  end

  def decode(:copy) do
    quote location: :keep do
      <<len::int32(), bin::binary-size(len)>> ->
        <<_version::int8(), ltree::binary>> = bin
        :binary.copy(ltree)
    end
  end
end
