defmodule Postgrex.Extensions.Hstore do
  @moduledoc false

  alias Postgrex.TypeInfo
  alias Postgrex.Extensions.Hstore.Decoder
  alias Postgrex.Extensions.Hstore.Encoder

  @behaviour Postgrex.Extension

  def init(parameters, _opts),
    do: parameters["server_version"] |> Postgrex.Utils.version_to_int

  def matching(_),
    do: [send: "hstore_send"]

  def decode(%TypeInfo{send: "hstore_send"}, bin, _types, _opts) do
    Decoder.decode bin
  end

  def encode(%TypeInfo{send: "hstore_send"}, nil, _types, _opts) do
    nil
  end

  def encode(%TypeInfo{send: "hstore_send"}, term, _types, _opts) do
    Encoder.encode term
  end

  def format(_opts), do: :binary

end
