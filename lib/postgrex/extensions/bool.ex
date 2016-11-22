defmodule Postgrex.Extensions.Bool do
  @moduledoc false
  import Postgrex.BinaryUtils
  use Postgrex.BinaryExtension, send: "boolsend"

  def encode(_, true, _, _),
    do: <<1>>
  def encode(_, false, _, _),
    do: <<0>>
  def encode(type_info, value, _, _) do
    raise ArgumentError,
      Postgrex.Utils.encode_msg(type_info, value, "a boolean")
  end

  def decode(_, <<1 :: int8>>, _, _),
    do: true
  def decode(_, <<0 :: int8>>, _, _),
    do: false

  def inline(_type_info, _types, _opts) do
    {__MODULE__, inline_encode(), inline_decode()}
  end

  defp inline_encode() do
    quote location: :keep do
      true ->
        <<1 :: int32, 1>>
      false ->
        <<1 :: int32, 0>>
      other ->
        raise ArgumentError, Postgrex.Utils.encode_msg(other, "a boolean")
    end
  end

  defp inline_decode() do
    quote location: :keep do
      <<1 :: int32, 1>> -> true
      <<1 :: int32, 0>> -> false
    end
  end
end
