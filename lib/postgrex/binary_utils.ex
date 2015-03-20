defmodule Postgrex.BinaryUtils do
  @moduledoc false

  defmacro int64 do
    quote do: signed-64
  end

  defmacro int32 do
    quote do: signed-32
  end

  defmacro int16 do
    quote do: signed-16
  end

  defmacro uint16 do
    quote do: unsigned-16
  end

  defmacro uint32 do
    quote do: unsigned-32
  end

  defmacro int8 do
    quote do: signed-8
  end

  defmacro float64 do
    quote do: float-64
  end

  defmacro float32 do
    quote do: float-32
  end

  defmacro binary(size) do
    quote do: binary-size(unquote(size))
  end

  defmacro binary(size, unit) do
    quote do: binary-size(unquote(size))-unit(unquote(unit))
  end
end
