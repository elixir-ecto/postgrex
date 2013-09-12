defmodule Postgrex.BinaryUtils do
  defmacro int32 do
    quote do: size(32)
  end

  defmacro int16 do
    quote do: size(16)
  end

  defmacro int8 do
    quote do: size(8)
  end

  defmacro binary(size) do
    quote do: [binary, unit(8), size(unquote(size))]
  end
end
