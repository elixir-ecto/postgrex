defmodule Postgrex.Extensions.Int8 do
  @moduledoc false
  import Postgrex.BinaryUtils
  use Postgrex.BinaryExtension, send: "int8send"

  @int8_range -9223372036854775808..9223372036854775807

  def encode(_, n, _, _) when is_integer(n) and n in @int8_range,
    do: <<n :: int64>>
  def encode(type_info, value, _, _) do
    raise ArgumentError,
      Postgrex.Utils.encode_msg(type_info, value, @int8_range)
  end

  def decode(_, <<n :: int64>>, _, _),
    do: n

  def inline(_type_info, _types, _opts) do
    {__MODULE__, inline_encode(), inline_decode()}
  end

  defp inline_encode() do
    range = Macro.escape(@int8_range)
    quote location: :keep do
      int when is_integer(int) and int in unquote(range) ->
        <<8 :: int32, int :: int64>>
      other ->
        raise ArgumentError, Postgrex.Utils.encode_msg(other, unquote(range))
    end
  end

  defp inline_decode() do
    quote location: :keep do
      <<8 :: int32, int :: int64>> -> int
    end
  end
end
