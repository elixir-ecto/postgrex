defmodule Postgrex.Extension do
  @moduledoc """
  An extension knows how to encode and decode Postgres types to and from Elixir
  values. Custom extensions can be enabled using the `:extension` option in
  `Postgrex.start_link/1`.


  For example to support label trees using the text encoding format:

      defmodule MyApp.LTree do

        @behaviour Postgrex.Extension

        # It can be memory efficient to copy the decoded binary because a
        # reference counted binary that points to a larger binary will be passed
        # to the decode/4 callback. Copying the binary can allow the larger
        # binary to be garbage collected sooner if the copy is going to be kept
        # for a longer period of time. See `:binary.copy/1` for more
        # information.
        def init(_parameters, opts) when opts in [:reference, :copy], do: opts

        # Use this extension when `type` from %Postgrex.TypeInfo{} is "ltree"
        def matching(_opts), do: [type: "ltree"]

        def format(_opts), do: :text

        # Use a string that is the same as postgres's ltree text format
        def encode(_type_info, bin, _types, _opts) when is_binary(bin), do: bin

        def decode(_type_info, bin, _types, :reference), do: bin
        def decode(_type_info, bin, _types, :copy),      do: :binary.copy(bin)

      end

  This example is enabled with
  `Postgrex.start_link([extensions: [{MyApp.LTree, :copy}]])`.
  """

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
  @callback init(Map.t, term) :: opts

  @doc """
  Specifies the types the extension matches, see `Postgrex.TypeInfo` for
  specification of the fields.
  """
  @callback matching(opts) :: [type: String.t,
                                 send: String.t,
                                 receive: String.t,
                                 input: String.t,
                                 output: String.t]

  @doc """
  Returns the format the type should be encoded as. See
  http://www.postgresql.org/docs/9.4/static/protocol-overview.html#PROTOCOL-FORMAT-CODES.
  """
  @callback format(opts) :: :binary | :text

  @doc """
  Should encode an Elixir value to a binary in the specified Postgres protocol
  format.
  """
  @callback encode(TypeInfo.t, term, Types.types, opts) :: iodata

  @doc """
  Should decode a binary in the specified Postgres protocol format to an Elixir
  value.
  """
  @callback decode(TypeInfo.t, binary, Types.types, opts) :: term
end
