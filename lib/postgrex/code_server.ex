defmodule Postgrex.CodeServer do
  @moduledoc false

  require Logger
  use GenServer

  def get(key) do
    GenServer.call(__MODULE__, {:get, key, self()})
  end

  def start_link() do
    GenServer.start_link(__MODULE__, nil, [name: __MODULE__])
  end

  def init(nil) do
    _ = Process.flag(:trap_exit, true)
    {:ok, {%{}, %{}}}
  end

  def handle_call({:get, key, pid}, _, {keys, mods} = state) do
    case keys do
      %{^key => mod} ->
        {:reply, mod, state}
      %{} ->
        mod = :"Postgrex.TypeModule#{System.unique_integer([:positive])}"
        {:ok, _} = Postgrex.TypeSupervisor.start_server(mod, pid)
        _ = Process.monitor(mod)
        {:reply, mod, {Map.put(keys, key, mod), Map.put(mods, mod, key)}}
    end
  end

  def handle_info({:DOWN, _, _, {mod, _}, _} = msg, {keys, mods} = state) do
    case mods do
      %{^mod => key} ->
        delete(mod)
        {:noreply, {Map.delete(keys, key), Map.delete(mods, mod)}}
      %{} ->
        unexpected(msg, state)
    end
  end
  def handle_info(msg, state) do
    unexpected(msg, state)
  end

  def terminate(_, {_, mods}) do
    for {mod, _} <- mods do
      kill(mod)
      delete(mod)
    end
  end

  ## Helpers

  defp unexpected(msg, state) do
    _ = Logger.warn(fn() ->
      [inspect(__MODULE__), " received unexpected message: " | inspect(msg)]
    end)
    {:noreply, state}
  end

  # Ensure type server has stopped before deleting its module. Should only be
  # needed if the CodeServer crashes!
  defp kill(mod) do
    case Process.whereis(mod) do
      pid when is_pid(pid) ->
        mref = Process.monitor(mod)
        Process.exit(pid, :kill)
        receive do: ({:DOWN, ^mref, _, _, _} -> :ok)
      nil ->
        :ok
    end
  end

  defp delete(mod) do
    _ = :code.purge(mod)
    :code.delete(mod) && (:code.soft_purge(mod) || brutal_purge(mod))
  end

  defp brutal_purge(mod) do
    _ = Logger.error(fn() ->
      [inspect(__MODULE__), " purging ", inspect(mod)]
    end)
    :code.purge(mod)
  end
end
