defmodule Postgrex.Extensions.JSON do
  @moduledoc """
  An extension that supports the `json` and `jsonb` types.

  This extension is not used by default, it needs to be included in the
  `:extensions` option to `Postgrex.start_link/1`.

  ## Options

    * `:library` - The module to encode and decode JSON binaries, calls
    `module.encode!/1` to encode and `module.decode!/1` to decode (required);
    * `:decode_binary` - Either `:copy` to copy binary values before decoding
    with the library module or `:reference` to use a reference counted binary of
    the binary received from the socket. Referencing a potentially larger binary
    can be more efficient if the binary value is going to be garbaged collected
    soon because a copy is avoided. However the larger binary can not be garbage
    collected until all references are garbage collected (defaults to `:copy`);
  """

  alias Postgrex.TypeInfo

  @behaviour Postgrex.Extension

  def init(_parameters, opts) do
    {Keyword.get(opts, :decode_binary, :copy), Keyword.fetch!(opts, :library)}
  end

  def matching(_),
    do: [type: "json", type: "jsonb"]

  def format(_),
    do: :binary

  def encode(%TypeInfo{type: "json"}, map, _state, {_, library}),
    do: library.encode!(map)
  def encode(%TypeInfo{type: "jsonb"}, map, _state, {_, library}),
    do: [1 | library.encode!(map)]

  def decode(%TypeInfo{type: "json"}, json, _state, {:reference, library}),
    do: library.decode!(json)
  def decode(%TypeInfo{type: "json"}, json, _state, {:copy, library}),
    do: json |> :binary.copy() |> library.decode!()
  def decode(%TypeInfo{type: "jsonb"}, <<1, json::binary>>, _state, {:reference, library}),
    do: library.decode!(json)
  def decode(%TypeInfo{type: "jsonb"}, <<1, json::binary>>, _state, {:copy, library}),
    do: json |> :binary.copy() |> library.decode!()

  def inline(%TypeInfo{type: "json"}, _types, opts) do
    {:json, inline_json_encode(opts), inline_json_decode(opts)}
  end
  def inline(%TypeInfo{type: "jsonb"}, _types, opts) do
    {:jsonb, inline_jsonb_encode(opts), inline_jsonb_decode(opts)}
  end

  defp inline_json_encode({library, _}) do
    quote location: :keep do
      map ->
        data = unquote(library).encode!(map)
        [<<IO.iodata_length(data) :: int32>> | data]
    end
  end

  defp inline_json_decode({library, :copy}) do
    quote location: :keep do
      <<len :: int32, json :: binary-size(len)>> ->
        json
        |> :binary.copy()
        |> unquote(library).decode!()
    end
  end
  defp inline_json_decode({library, :reference}) do
    quote location: :keep do
      <<len :: int32, json :: binary-size(len)>> ->
        unquote(library).decode!(json)
    end
  end

  defp inline_jsonb_encode({library, _}) do
    quote location: :keep do
      map ->
        data = unquote(library).encode!(map)
        [<<(IO.iodata_length(data)+1) :: int32, 1>> | data]
    end
  end

  defp inline_jsonb_decode({library, :copy}) do
    quote location: :keep do
      <<len :: int32, data :: binary-size(len)>> ->
        <<1, json :: binary>> = data
        json
        |> :binary.copy()
        |> unquote(library).decode!()
    end
  end
  defp inline_jsonb_decode({library, :reference}) do
    quote location: :keep do
      <<len :: int32, data :: binary-size(len)>> ->
        <<1, json :: binary>> = data
        unquote(library).decode!(json)
    end
  end
end
