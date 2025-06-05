defmodule Postgrex.Extensions.JSONB do
  @moduledoc false
  import Postgrex.BinaryUtils, warn: false

  def init(opts) do
    json_library =
      Keyword.get_lazy(opts, :json, fn ->
        default = default_json_backend()
        Application.get_env(:postgrex, :json_library, default)
      end)

    {encode_fn(json_library), decode_fn(json_library), Keyword.get(opts, :decode_binary, :copy)}
  end

  def matching({nil, _}),
    do: []

  def matching(_),
    do: [type: "jsonb"]

  def format(_),
    do: :binary

  def encode({encode_fn, _decode_fn, _}) do
    quote location: :keep do
      map ->
        data = unquote(encode_fn).(map)
        [<<IO.iodata_length(data) + 1::int32(), 1>> | data]
    end
  end

  def decode({_encode_fn, decode_fn, :copy}) do
    quote location: :keep do
      <<len::int32(), data::binary-size(len)>> ->
        <<1, json::binary>> = data

        json
        |> :binary.copy()
        |> unquote(decode_fn).()
    end
  end

  def decode({_encode_fn, decode_fn, :reference}) do
    quote location: :keep do
      <<len::int32(), data::binary-size(len)>> ->
        <<1, json::binary>> = data
        unquote(decode_fn).(json)
    end
  end

  defp encode_fn(:json), do: &:json.encode/1
  defp encode_fn(json_library), do: &json_library.encode_to_iodata!/1

  defp decode_fn(:json), do: &:json.decode/1
  defp decode_fn(json_library), do: &json_library.decode!/1

  defp default_json_backend() do
    try do
      # NOTE: it is not possible to see if the module is loaded, since it may
      # not be as it is only loaded on first use. So we try to use it with apply
      # which throws an exception if it doesn't not exist without also emitting
      # warnings to the logger.
      apply(&:json.encode/1, [""])
      :json
    rescue
      _ -> Jason
    end
  end

end
