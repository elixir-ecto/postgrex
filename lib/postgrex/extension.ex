defmodule Postgrex.Extension do
  use Behaviour
  alias Postgrex.TypeInfo

  @type oid :: pos_integer

  defcallback matching() :: [type: String.t,
                             send: String.t,
                             receive: String.t,
                             input: String.t,
                             output: String.t]

  defcallback format() :: :binary | :text

  defcallback encode(TypeInfo.t, term, (oid, term -> binary)) :: binary

  defcallback decode(TypeInfo.t, binary, (oid, binary -> term)) :: term
end
