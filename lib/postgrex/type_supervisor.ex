defmodule Postgrex.TypeSupervisor do
  @moduledoc false

  use Supervisor

  @manager Postgrex.TypeManager
  @supervisor Postgrex.TypeSupervisor

  @doc """
  Starts a type supervisor with a manager and a simple
  one for one supervisor for each server.
  """
  def start_link(_) do
    Supervisor.start_link(__MODULE__, :ok)
  end

  @doc """
  Locates a type server for the given module-key pair.
  """
  def locate(module, key) do
    pair = {module, key}

    case Registry.lookup(@manager, pair) do
      [{pid, _}] -> if Process.alive?(pid), do: pid, else: start_server(module, pair)
      [] -> start_server(module, pair)
    end
  end

  defp start_server(module, pair) do
    opts = [name: {:via, Registry, {Postgrex.TypeManager, pair}}]

    case DynamicSupervisor.start_child(@supervisor, {Postgrex.TypeServer, {module, self(), opts}}) do
      {:ok, pid} -> pid
      {:error, {:already_started, pid}} -> pid
    end
  end

  # Callbacks

  def init(:ok) do
    manager = {Registry, keys: :unique, name: @manager}
    server_sup = {DynamicSupervisor, strategy: :one_for_one, name: @supervisor}
    Supervisor.init([manager, server_sup], strategy: :rest_for_one)
  end
end
