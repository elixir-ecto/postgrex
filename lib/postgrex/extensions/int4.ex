defmodule Postgrex.Extensions.Int4 do
  @moduledoc false
  import Postgrex.BinaryUtils
  use Postgrex.BinaryExtension, send: "int4send"

  @int4_range -2147483648..2147483647

  def encode(_, n, _, _) when is_integer(n) and n in @int4_range,
    do: <<n :: int32>>
  def encode(type_info, value, _, _) do
    raise ArgumentError,
      Postgrex.Utils.encode_msg(type_info, value, @int4_range)
  end

  def decode(_, <<n :: int32>>, _, _),
    do: n

  def inline(_type_info, _types, _opts) do
    {__MODULE__, inline_encode(), inline_decode()}
  end

  defp inline_encode() do
    range = Macro.escape(@int4_range)
    quote location: :keep do
      int when is_integer(int) and int in unquote(range) ->
        <<4 :: int32, int :: int32>>
      other ->
        raise ArgumentError, Postgrex.Utils.encode_msg(other, unquote(range))
    end
  end

  defp inline_decode() do
    quote location: :keep do
      <<4 :: int32, int :: int32>> -> int
    end
  end
end
