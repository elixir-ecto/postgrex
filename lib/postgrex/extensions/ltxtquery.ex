defmodule Postgrex.Extensions.Ltxtquery do
  @moduledoc false
  import Postgrex.BinaryUtils, warn: false
  use Postgrex.BinaryExtension, type: "ltxtquery"

  @impl true
  def init(opts), do: Keyword.get(opts, :decode_binary, :copy)

  # ltxtquery binary formats are versioned
  # https://github.com/postgres/postgres/blob/master/contrib/ltree/ltxtquery_io.c
  @impl true
  def encode(_state) do
    quote location: :keep, generated: true do
      bin when is_binary(bin) ->
        version = 1
        size = byte_size(bin) + 1
        [<<size::int32(), version::int8()>> | bin]
    end
  end

  @impl true
  def decode(:reference) do
    quote location: :keep do
      <<len::int32(), bin::binary-size(len)>> ->
        <<_version::int8(), ltxtquery::binary>> = bin
        ltxtquery
    end
  end

  def decode(:copy) do
    quote location: :keep do
      <<len::int32(), bin::binary-size(len)>> ->
        <<_version::int8(), ltxtquery::binary>> = bin
        :binary.copy(ltxtquery)
    end
  end
end
