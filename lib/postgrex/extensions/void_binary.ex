defmodule Postgrex.Extensions.VoidBinary do
  @moduledoc false
  import Postgrex.BinaryUtils, warn: false
  # CockroachDB names void's send/output procs without underscores
  # (voidsend / voidout), so match its spelling in addition to Postgres'.
  use Postgrex.BinaryExtension, send: "void_send", send: "voidsend"

  def encode(_) do
    quote location: :keep do
      :void ->
        <<0::int32()>>

      other ->
        raise DBConnection.EncodeError, Postgrex.Utils.encode_msg(other, "the atom :void")
    end
  end

  def decode(_) do
    quote location: :keep do
      <<0::int32()>> -> :void
    end
  end
end
