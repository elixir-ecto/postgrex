defmodule Postgrex.Extensions.JSONB do
  @moduledoc false
  import Postgrex.BinaryUtils, warn: false

  def init(opts) do
    library = Keyword.get(opts, :json) || __MODULE__
    {library, Keyword.get(opts, :decode_binary, :copy)}
  end

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

  def encode!(json), do: json

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

  def decode!(json), do: json
end
