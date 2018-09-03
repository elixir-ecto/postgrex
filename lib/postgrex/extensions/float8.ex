defmodule Postgrex.Extensions.Float8 do
  @moduledoc false
  import Postgrex.BinaryUtils, warn: false
  use Postgrex.BinaryExtension, send: "float8send"

  def encode(_) do
    quote location: :keep do
      n when is_number(n) ->
        <<8 :: int32, n :: float64>>
      :NaN ->
        <<8 :: int32, 0::1, 2047::11, 1::1, 0::51>>
      :inf ->
        <<8 :: int32, 0::1, 2047::11, 0::52>>
      :"-inf" ->
        <<8 :: int32, 1::1, 2047::11, 0::52>>
      other ->
        raise DBConnection.EncodeError, Postgrex.Utils.encode_msg(other, "a float")
    end
  end

  def decode(_) do
    quote location: :keep do
      <<8::int32, 0::1, 2047::11, 0::52>> -> :inf
      <<8::int32, 1::1, 2047::11, 0::52>> -> :"-inf"
      <<8::int32, _::1, 2047::11, _::52>> -> :NaN
      <<8::int32, float::float64>>        -> float
    end
  end
end
