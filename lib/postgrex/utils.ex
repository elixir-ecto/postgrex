defmodule Postgrex.Utils do
  @moduledoc false

  def error(error, s) do
    reply(error, s)
    {:stop, error, s}
  end

  def reply(reply, %{queue: queue}) do
    case :queue.out(queue) do
      {:empty, _queue} ->
        false
      {{:value, %{from: nil}}, _queue} ->
        false
      {{:value, %{reply: :no_reply, from: from}}, _queue} ->
        GenServer.reply(from, reply)
        true
      {{:value, %{reply: {:reply, reply}, from: from}}, _queue} ->
        GenServer.reply(from, reply)
        true
    end
  end

  def reply(reply, {_, _} = from) do
    GenServer.reply(from, reply)
    true
  end

  def version_to_int(version) do
    parts = binary_split(version, ".", 2)
    [major, minor, patch] = Enum.map(parts, &:erlang.binary_to_integer/1)
    major*10_000 + minor*100 + patch
  end

  def binary_split(binary, _pattern, 0) do
    [binary]
  end

  def binary_split(binary, pattern, max) do
    case :binary.split(binary, pattern) do
      [match, rest] ->
        [match|binary_split(rest, pattern, max-1)]
      [binary] ->
        [binary]
    end
  end
end
