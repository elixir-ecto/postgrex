defmodule Postgrex.Utils do
  @moduledoc false

  def error(error, s) do
    if reply(error, s) do
      {:stop, :normal, s}
    else
      {:stop, error, s}
    end
  end

  def reply(reply, %{queue: queue}) do
    case :queue.out(queue) do
      {{:value, {_command, from, _timer}}, _queue} ->
        :gen_server.reply(from, reply)
        true
      {:empty, _queue} ->
        false
    end
  end

  def reply(reply, {_, _} = from) do
    :gen_server.reply(from, reply)
    true
  end
end
