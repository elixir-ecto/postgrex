defmodule Postgrex.Extensions.BitString do
  @moduledoc false
  import Postgrex.BinaryUtils, warn: false
  use Postgrex.BinaryExtension, send: "bit_send", send: "varbit_send"

  def init(opts), do: Keyword.fetch!(opts, :decode_binary)

  def encode(_) do
    quote location: :keep, generated: true do
      val when is_binary(val) ->
        [<<byte_size(val) + 4::int32(), bit_size(val)::uint32()>> | val]

      val when is_bitstring(val) ->
        bin_size = byte_size(val)
        last_pos = bin_size - 1
        <<binary::binary-size(last_pos), last::bits>> = val
        pad = 8 - bit_size(last)
        bit_count = bit_size(val)

        [
          <<bin_size + 4::int32(), bit_count::uint32()>>,
          binary
          | <<last::bits, 0::size(pad)>>
        ]

      val ->
        raise DBConnection.EncodeError, Postgrex.Utils.encode_msg(val, "a bitstring")
    end
  end

  def decode(:copy) do
    quote location: :keep do
      <<len::int32(), value::binary-size(len)>> ->
        copy = :binary.copy(value)
        <<len::unsigned-32, bits::bits-size(len), _::bits>> = copy
        bits
    end
  end

  def decode(:reference) do
    quote location: :keep do
      <<len::int32(), value::binary-size(len)>> ->
        <<len::int32(), bits::bits-size(len), _::bits>> = value
        bits
    end
  end
end
