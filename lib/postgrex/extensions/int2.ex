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
end
