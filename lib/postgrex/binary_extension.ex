defmodule Postgrex.BinaryExtension do
  @moduledoc false

  defmacro __using__(matching) do
    quote location: :keep do
      @behaviour Postgrex.Extension

      def init(_), do: nil

      def matching(_), do: unquote(matching)

      def format(_), do: :binary

      defoverridable init: 1
    end
  end
end
