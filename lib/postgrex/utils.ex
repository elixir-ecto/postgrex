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
    parts = Regex.run(~r/(\d{1,2})\.?(\d{1,3})?\.?(\d{1,3})?/, version, capture: :all_but_first)
    case Enum.map(parts, &:erlang.binary_to_integer/1) do
        [major, minor, patch] -> major*10_000 + minor*100 + patch
        [major, minor] -> major*10_000 + minor*100
        [major] -> major*10_000 
    end
  end
end
