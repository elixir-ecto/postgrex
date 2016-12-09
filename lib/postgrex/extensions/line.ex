defmodule Postgrex.Extensions.Line do
  @moduledoc false
  import Postgrex.BinaryUtils, warn: false
  use Postgrex.BinaryExtension, send: "line_send"

  def encode(_) do
    quote location: :keep do
      %Postgrex.Line{a: a, b: b, c: c} when is_number(a) and is_number(b) and is_number(c) ->
        # a, b, c are 8 bytes each
        <<24::int32, a::float64, b::float64, c::float64>>
      other ->
        raise ArgumentError, Postgrex.Utils.encode_msg(other, Postgrex.Line)
    end
  end

  def decode(_) do
    quote location: :keep do
      # a, b, c are 8 bytes each
      <<24::int32, a::float64, b::float64, c::float64>> ->
        %Postgrex.Line{a: a, b: b, c: c}
    end
  end
end
