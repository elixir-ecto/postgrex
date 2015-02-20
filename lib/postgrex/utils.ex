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

  @doc """
  Converts pg major.minor.patch (http://www.postgresql.org/support/versioning) version to an integer
  """
  def version_to_int(version) do
    case version |> String.split(".") |> Enum.map(fn (part) -> elem(Integer.parse(part),0) end) do
      [major, minor, patch] -> major*10_000 + minor*100 + patch
      [major, minor] -> major*10_000 + minor*100
      [major] -> major*10_000 
    end
  end
end
