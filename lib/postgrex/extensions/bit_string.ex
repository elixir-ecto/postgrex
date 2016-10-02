defmodule Postgrex.Extensions.BitString do
  use Postgrex.BinaryExtension,
    [receive: "bit_recv", send: "bit_send", type: "bit",
     receive: "varbit_recv", send: "varbit_send", type: "varbit"]
  
  def encode(_, value, _, _) do
    bit_count = bit_size(value)
    out = encode_binary(value, <<>>)
    << << bit_count :: size(32) >>, << out :: bits >> >>
  end

  def decode(_, value, _, _) do
    << bit_count :: size(32), bytes :: binary >> = value

    got_count = bit_size(bytes)
    cond do
      bit_count > got_count ->
        pad = bit_count - got_count
        << << bytes :: bits >>, << 0 :: size(pad) >> >>
      bit_count < got_count ->
        pad = bit_count - got_count
        << v :: size(bit_count), _ :: bits >> = bytes
        << v :: size(bit_count) >>
      true ->
        bytes
    end
  end

  defp encode_binary(<< value :: 8, rest :: bits >>, so_far) do
    encode_binary(rest, << << so_far :: bits >> , << value :: 8 >> >> )
  end
  defp encode_binary(<< rest :: bits >>, so_far) do
    bit_count = bit_size(rest)
    pad = 8 - bit_count
    << << so_far :: bits >>, << rest :: bits >>, << 0 :: size(pad) >> >>
  end
end
