defmodule Postgrex.Extensions.JSONB do
  @moduledoc false
  import Postgrex.BinaryUtils

  def init(_parameters, opts) do
    {Keyword.get(opts, :decode_binary, :copy), Keyword.get(opts, :json)}
  end

  def matching({_, nil}),
    do: []
  def matching(_),
    do: [type: "jsonb"]

  def format(_),
    do: :binary

  def encode({library, _}) do
    quote location: :keep do
      map ->
        data = unquote(library).encode!(map)
        [<<(IO.iodata_length(data)+1) :: int32, 1>> | data]
    end
  end

  def decode({library, :copy}) do
    quote location: :keep do
      <<len :: int32, data :: binary-size(len)>> ->
        <<1, json :: binary>> = data
        json
        |> :binary.copy()
        |> unquote(library).decode!()
    end
  end
  def decode({library, :reference}) do
    quote location: :keep do
      <<len :: int32, data :: binary-size(len)>> ->
        <<1, json :: binary>> = data
        unquote(library).decode!(json)
    end
  end
end
