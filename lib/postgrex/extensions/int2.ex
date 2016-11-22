defmodule Postgrex.Extensions.Int2 do
  @moduledoc false
  import Postgrex.BinaryUtils
  use Postgrex.BinaryExtension, send: "int2send"

  @int2_range -32768..32767

  def encode(_, n, _, _) when is_integer(n) and n in @int2_range,
    do: <<n :: int16>>
  def encode(type_info, value, _, _) do
    raise ArgumentError,
      Postgrex.Utils.encode_msg(type_info, value, @int2_range)
  end

  def decode(_, <<n :: int16>>, _, _),
    do: n

  def inline(_type_info, _types, _opts) do
    {__MODULE__, inline_encode(), inline_decode()}
  end

  defp inline_encode() do
    range = Macro.escape(@int2_range)
    quote location: :keep do
      int when is_integer(int) and int in unquote(range) ->
        <<2 :: int32, int :: int16>>
      other ->
        raise ArgumentError, Postgrex.Utils.encode_msg(other, unquote(range))
    end
  end

  defp inline_decode() do
    quote location: :keep do
      <<2 :: int32, int :: int16>> -> int
    end
  end
end
