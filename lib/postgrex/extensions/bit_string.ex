defmodule Postgrex.Extensions.BitString do
  @moduledoc false

  use Postgrex.BinaryExtension, [send: "bit_send",send: "varbit_send"]
  
  # encode is a straight binary encode plus padding to ensure full bytes
  def encode(_, value, _, _) do
    bit_count = bit_size(value)
    pad = 8 - rem(bit_count, 8)
    << << bit_count :: size(32) >>, << value :: bits >>, << 0 :: size(pad) >> >>
  end

  # decode is a straight binary decode, except if the bit count is
  # larger than the payload, in which case we pad with zeros
  def decode(_, value, _, _) do
    << bit_count :: size(32), bytes :: binary >> = value
    got_count = bit_size(bytes)

    if bit_count <= got_count do
        << v :: size(bit_count), _ :: bits >> = bytes
        << v :: size(bit_count) >>
    else
      pad = bit_count - got_count
      << << bytes :: bits >>, << 0 :: size(pad) >> >>
    end
  end
end
