defmodule Postgrex.Encoder do
  @moduledoc """
  Behaviour for custom encoding of elixir values to postgres' binary format.

  ## Example

      defmodule MyEncoder do
        use Postgrex.Encoder

        # Encoder of a direction enum
        def post_encode(:direction, _sender, _oid, param) do
          case param do
            :up -> "up"
            :down -> "down"
          end
        end
      end
  """

  use Behaviour

  @doc """
  Converts an elixir value to another elixir vaulue. Useful when you have a
  custom elixir value that can be converted to a value Postgrex can encode.
  """
  defcallback pre_encode(type :: atom, sender :: atom, oid :: integer, param :: term) :: term

  @doc """
  Encodes an elixir value to postgres' binary format. Also receives the binary
  as Postgrex encoded it in encoded or `nil` if Postgrex could not encode it.
  """
  defcallback post_encode(type :: atom, sender :: atom, oid :: integer, param :: term, encoded :: binary | nil) :: binary

  defmacro __using__(_opts) do
    quote do
      @behaviour unquote(__MODULE__)

      def pre_encode(_type, _sender, _oid, param), do: param
      def post_encode(_type, _sender, _oid, _param, encoded), do: encoded

      defoverridable [pre_encode: 4, post_encode: 5]
    end
  end
end

defmodule Postgrex.Decoder do
  @moduledoc """
  Behaviour for custom decoding of postgres' binary format to elixir values.

  ## Example

      defmodule MyDecoder do
        use Postgrex.Decoder

        # Encoder of a direction enum
        def decode(:direction, _sender, _oid, value, nil) do
          case value do
            "up" -> :up
            "down" -> :down
          end
        end
      end
  """

  use Behaviour

  @doc """
  Decodes a binary from postgres' binary format to an elixir value. Also
  receives the decoded value as Postgrex decoded it or `nil` if it could not be
  decoded.
  """
  defcallback decode(type :: atom, sender :: atom, oid :: integer, value :: binary, decoded :: term | nil) :: term

  defmacro __using__(_opts) do
    quote do
      @behaviour unquote(__MODULE__)
      def decode(_type, _sender, _oid, _value, decoded), do: decoded
      defoverridable [decode: 5]
    end
  end
end
