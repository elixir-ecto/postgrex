defmodule Postgrex.Extensions.Line do
  @moduledoc false

  import Postgrex.BinaryUtils
  use Postgrex.BinaryExtension, send: "line_send"

  def encode(_, %Postgrex.Line{a: a, b: b, c: c}, _, _)
  when is_number(a) and is_number(b) and is_number(c) do
    <<a::float64, b::float64, c::float64>>
  end
  def encode(type_info, value, _, _) do
    raise ArgumentError,
      Postgrex.Utils.encode_msg(type_info, value, Postgrex.Line)
  end

  def decode(_, data, _, _) do
    <<a::float64, b::float64, c::float64>> = data
    %Postgrex.Line{a: a, b: b, c: c}
  end
end
