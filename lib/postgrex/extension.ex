defmodule Postgrex.Extension do
  @moduledoc """
  An extension knows how to encode and decode Postgres types to and
  from Elixir values.

  Custom extensions can be enabled using the `:extensions` option in
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
        def init(opts) when opts in [:reference, :copy], do: opts

        # Use this extension when `type` from %Postgrex.TypeInfo{} is "ltree"
        def matching(_opts), do: [type: "ltree"]

        # Use the text format, "ltree" does not have a binary format.
        def format(_opts), do: :text

        # Use quoted expression to encode a string that is the same as
        # postgresql's ltree text format. The quoted expression should contain
        # clauses that match those of a `case` or `fn`. Encoding matches on the
        # value and returns encoded `iodata()`. The first 4 bytes in the
        # `iodata()` must be the byte size of the rest of the encoded data, as a
        # signed 32bit big endian integer.
        def encode(_opts) do
          quote do
            bin when is_binary(bin) ->
              [<<byte_size(bin) :: signed-size(32)>> | bin]
          end
        end

        # Use quoted expression to decode the data to a string. Decoding matches
        # on an encoded binary with the same signed 32bit big endian integer
        # length header.
        def decode(:reference) do
          quote do
            <<len::signed-size(32), bin::binary-size(len)>> ->
              bin
          end
        end
        def decode(:copy) do
          quote do
            <<len::signed-size(32), bin::binary-size(len)>> ->
              :binary.copy(bin)
          end
        end
      end

  This example is enabled with
  `Postgrex.start_link([extensions: [{MyApp.LTree, :copy}]])`.

  When starting Postgrex with a custom extension, you may see new warnings
  coming from "lib/postgrex/type_modules.ex" caused by your extension. Those
  warnings are triggered when Postgrex is compiling the extensions into a
  unified parser. To retrieve the proper file and line, you can set `:postgrex`
  to debug mode with the following configuration:

      config :postgrex, :debug_extensions, true

  Once such configuration is set, starting Postgrex is slower but provides
  proper feedback.
  """

  alias Postgrex.Types
  alias Postgrex.TypeInfo

  @type t :: module
  @type opts :: term

  @doc """
  Should perform any initialization of the extension. The function receives the
  user options. The options returned from this function will be passed to other
  callbacks.
  """
  @callback init(term) :: opts

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
  Returns a quoted list of clauses that encode an Elixir value to iodata with
  a signed 32 bit big endian integer byte length header.

      def encode(_) do
        quote do
          integer ->
            <<8 :: signed-32, integer :: signed-64>>
        end
      end

  """
  @callback encode(opts) :: Macro.expr

  @doc """
  Returns a quoted list of clauses that decode a binary to an Elixir value. The
  pattern must use binary syntax and decode a fixed length using the signed 32
  bit big endian integer byte length header.

      def decode(_) do
        quote do
          # length header is in bytes
          <<len :: signed-32, integer :: signed-size(len)-unit(8)>> ->
            integer
        end
      end
  """
  @callback decode(opts) :: Macro.expr
end
