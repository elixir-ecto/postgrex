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
end
