defmodule Postgrex.Extensions.BitString do
  use Postgrex.BinaryExtension,
    [receive: "bit_recv", send: "bit_send", type: "bit",
     receive: "varbit_recv", send: "varbit_send", type: "varbit"]
  
  def encode(_, value, _, _) do
    bit_count = bit_size(value)
    pad = 8 - rem(bit_count, 8)
    << << bit_count :: size(32) >>, << value :: bits >>, << 0 :: size(pad) >> >>
  end

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
