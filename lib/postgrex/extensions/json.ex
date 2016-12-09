defmodule Postgrex.Extensions.JSON do
  @moduledoc false
  @behaviour Postgrex.Extension
  import Postgrex.BinaryUtils, warn: false

  def init(opts) do
    {Keyword.get(opts, :json), Keyword.get(opts, :decode_binary, :copy)}
  end

  def matching({nil, _}),
    do: []
  def matching(_),
    do: [type: "json"]

  def format(_),
    do: :binary

  def encode({library, _}) do
    quote location: :keep do
      map ->
        data = unquote(library).encode!(map)
        [<<IO.iodata_length(data) :: int32>> | data]
    end
  end

  def decode({library, :copy}) do
    quote location: :keep do
      <<len :: int32, json :: binary-size(len)>> ->
        json
        |> :binary.copy()
        |> unquote(library).decode!()
    end
  end
  def decode({library, :reference}) do
    quote location: :keep do
      <<len :: int32, json :: binary-size(len)>> ->
        unquote(library).decode!(json)
    end
  end
end
