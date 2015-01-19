defmodule Postgrex.Utils do
  @moduledoc false

  import Postgrex.BinaryUtils

  def encode_param(<<-1 :: int32>>),
    do: <<-1 :: int32>>
  def encode_param(param),
    do: [<<IO.iodata_length(param) :: int32>>, param]

  def error(error, s) do
    if reply(error, s) do
      {:stop, :normal, s}
    else
      {:stop, error, s}
    end
  end

  def reply(reply, %{queue: queue}) do
    case :queue.out(queue) do
      {{:value, {_command, from}}, _queue} ->
        GenServer.reply(from, reply)
        true
      {:empty, _queue} ->
        false
    end
  end

  def reply(reply, {_, _} = from) do
    GenServer.reply(from, reply)
    true
  end
end
