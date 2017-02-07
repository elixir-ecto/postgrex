defmodule Postgrex.Extensions.VoidText do
  @moduledoc false
  @behaviour Postgrex.Extension
  import Postgrex.BinaryUtils, warn: false

  def init(_), do: nil

  def matching(_), do: [output: "void_out"]

  def format(_), do: :text

  def encode(_) do
    quote location: :keep do
      :void ->
        <<0 :: int32>>
      other ->
        raise ArgumentError, Postgrex.Utils.encode_msg(other, "the atom :void")
    end
  end

  def decode(_) do
    quote location: :keep do
      <<0 :: int32>> -> :void
    end
  end
end
