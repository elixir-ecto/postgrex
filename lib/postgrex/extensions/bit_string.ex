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

  def inline(_type_info, _types, opts) do
    {__MODULE__, inline_encode(), inline_decode(opts)}
  end

  defp inline_encode() do
    quote location: :keep do
      val when is_binary(val) ->
        [<<(byte_size(val) + 4)::int32, bit_size(val)::uint32>> | val]
      val when is_bitstring(val) ->
        bin_size = byte_size(val)
        last_pos = bin_size - 1
        <<binary::binary-size(last_pos), last::bits>> = val
        pad = 8 - bit_size(last)
        bit_count = bit_size(val)
        [<<(bin_size+4)::int32, bit_count::uint32>>, binary |
          <<last::bits, 0::size(pad)>>]
      val ->
        raise ArgumentError, Postgrex.Utils.encode_msg(val, "a bitstring")
    end
  end

  defp inline_decode(:copy) do
    quote location: :keep do
      <<len :: int32, value :: binary-size(len)>> ->
        copy = :binary.copy(value)
        <<len::unsigned-32, bits::bits-size(len), _::bits>> = copy
        bits
    end
  end
  defp inline_decode(:reference) do
    quote location: :keep do
      <<len :: int32, value :: binary-size(len)>> ->
        <<len :: int32, bits :: bits-size(len), _::bits>> = value
        bits
    end
  end
end
