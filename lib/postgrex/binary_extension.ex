defmodule Postgrex.BinaryExtension do
  @moduledoc false

  defmacro __using__(matching) do
    quote location: :keep do

      @behaviour Postgrex.Extension

      def init(_, _), do: nil

      def matching(_), do: unquote(matching)

      def format(_), do: :binary

      defoverridable [init: 2]
    end
  end
end
