defmodule Postgrex.Extension do
  @moduledoc """
  Am extension knows how to encode and decode Postgres types to and from Elixir
  values.
  """

  use Behaviour
  alias Postgrex.Types
  alias Postgrex.TypeInfo

  @type t :: module

  @doc """
  Specifies the types the extension matches, see `Postgrex.TypeInfo` for
  specification of the fields.
  """
  defcallback matching() :: [type: String.t,
                             send: String.t,
                             receive: String.t,
                             input: String.t,
                             output: String.t]

  @doc """
  Returns the format the type should be encoded as. See
  http://www.postgresql.org/docs/9.4/static/protocol-overview.html#PROTOCOL-FORMAT-CODES.
  """
  defcallback format() :: :binary | :text

  @doc """
  Should encode an Elixir value to a binary in the specified Postgres protocol
  format.
  """
  defcallback encode(TypeInfo.t, term, Types.types) :: binary

  @doc """
  Should decode a binary in the specified Postgres protocol format to an Elixir
  value.
  """
  defcallback decode(TypeInfo.t, binary, Types.types) :: term
end
