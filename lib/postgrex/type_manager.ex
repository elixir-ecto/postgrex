defmodule Postgrex.TypeManager do
  @moduledoc false

  use GenServer

  def get(module, key) do
    GenServer.call(__MODULE__, {:get, module, key})
  end

  def start_link() do
    GenServer.start_link(__MODULE__, nil, [name: __MODULE__])
  end

  def init(nil) do
    {:ok, {%{}, %{}}}
  end

  def handle_call({:get, module, key}, {pid, _}, {keys, mons} = state) do
    key = {module, key}
    case keys do
      %{^key => server} ->
        {:reply, server, state}
      %{} ->
        {:ok, server} = Postgrex.TypeSupervisor.start_server(module, pid)
        mref = Process.monitor(server)
        state = {Map.put(keys, key, server), Map.put(mons, mref, key)}
        {:reply, server, state}
    end
  end

  def handle_info({:DOWN, mref, _, _, _}, {keys, mons}) do
    {key, mons} = Map.pop(mons, mref)
    state = {Map.delete(keys, key), mons}
    {:noreply, state}
  end
end
