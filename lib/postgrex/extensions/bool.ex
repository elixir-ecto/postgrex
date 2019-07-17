defmodule Postgrex.Extensions.Bool do
  @moduledoc false
  import Postgrex.BinaryUtils, warn: false
  use Postgrex.BinaryExtension, send: "boolsend"

  def encode(_) do
    quote location: :keep do
      true ->
        <<1::int32, 1>>

      false ->
        <<1::int32, 0>>

      other ->
        raise DBConnection.EncodeError, Postgrex.Utils.encode_msg(other, "a boolean")
    end
  end

  def decode(_) do
    quote location: :keep do
      <<1::int32, 1>> -> true
      <<1::int32, 0>> -> false
    end
  end
end
