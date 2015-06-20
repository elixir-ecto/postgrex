defmodule Postgrex.Extension do
  @moduledoc """
  An extension knows how to encode and decode Postgres types to and from Elixir
  values.
  """

  use Behaviour
  alias Postgrex.Types
  alias Postgrex.TypeInfo

  @type t :: module
  @type opts :: term

  @doc """
  Should perform any initialization of the extension. The function receives the
  server parameters (http://www.postgresql.org/docs/9.4/static/runtime-config.html)
  and user options. The options returned from this function will be passed to
  all other callbacks.
  """
  defcallback init(Map.t, term) :: opts

  @doc """
  Specifies the types the extension matches, see `Postgrex.TypeInfo` for
  specification of the fields.
  """
  defcallback matching(opts) :: [type: String.t,
                                 send: String.t,
                                 receive: String.t,
                                 input: String.t,
                                 output: String.t]

  @doc """
  Returns the format the type should be encoded as. See
  http://www.postgresql.org/docs/9.4/static/protocol-overview.html#PROTOCOL-FORMAT-CODES.
  """
  defcallback format(opts) :: :binary | :text

  @doc """
  Should encode an Elixir value to a binary in the specified Postgres protocol
  format.
  """
  defcallback encode(TypeInfo.t, term, Types.types, opts) :: binary

  @doc """
  Should decode a binary in the specified Postgres protocol format to an Elixir
  value.
  """
  defcallback decode(TypeInfo.t, binary, Types.types, opts) :: term
end
