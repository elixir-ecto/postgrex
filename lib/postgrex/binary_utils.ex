defmodule Postgrex.BinaryUtils do
  defmacro int64 do
    quote do: [signed, size(64)]
  end

  defmacro int32 do
    quote do: [signed, size(32)]
  end

  defmacro int16 do
    quote do: [signed, size(16)]
  end

  defmacro int8 do
    quote do: [signed, size(8)]
  end

  defmacro float64 do
    quote do: [float, size(64)]
  end

  defmacro float32 do
    quote do: [float, size(32)]
  end

  defmacro binary(size) do
    quote do: [binary, unit(8), size(unquote(size))]
  end
end
