defmodule Postgrex.Utils do
  @moduledoc false

  import Postgrex.BinaryUtils

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
end
