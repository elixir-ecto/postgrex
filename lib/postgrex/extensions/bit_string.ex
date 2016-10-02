defmodule Postgrex.Extensions.BitString do
  @moduledoc false
  import Postgrex.BinaryUtils
  use Postgrex.BinaryExtension, [send: "bit_send",send: "varbit_send"]

  def init(_, opts), do: Keyword.fetch!(opts, :decode_binary)

  # encode is a straight binary encode plus padding to ensure full bytes
  def encode(_, value, _, _) when is_binary(value) do
    [<<bit_size(value)::uint32>> | value]
  end
  def encode(_, value, _, _) when is_bitstring(value) do
    bit_count = bit_size(value)
    last_pos = bit_count - rem(bit_count, 8)
    <<binary::bits-size(last_pos), last::bits>> = value
    pad = 8 - bit_size(last)
    [<<bit_count::uint32>>, binary | <<last::bits, 0::size(pad)>>]
  end
  def encode(type_info, value, _, _) do
    raise ArgumentError, Postgrex.Utils.encode_msg(type_info, value, "a bitstring")
  end

  # decode is a straight binary decode with any padding removed in last byte
  def decode(_, value, _, state) do
    decode(value, state)
  end

  defp decode(<<bit_count::uint32, bits::bits-size(bit_count), _::bits>>, :reference) do
    bits
  end
  defp decode(<<bit_count::uint32, rest::binary>>, :copy) do
    <<bits::bits-size(bit_count), _::bits>> = :binary.copy(rest)
    bits
  end
end
