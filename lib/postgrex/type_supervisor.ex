defmodule Postgrex.TypeSupervisor do
  @moduledoc false

  use Supervisor

  @manager Postgrex.TypeManager
  @servers Postgrex.TypeSupervisor

  @doc """
  Starts a type supervisor with a manager and a simple
  one for one supervisor for each server.
  """
  def start_link(mode \\ :manager)

  def start_link(:manager) do
    Supervisor.start_link(__MODULE__, :manager)
  end

  def start_link(:servers) do
    Supervisor.start_link(__MODULE__, :servers, name: @servers)
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
    args = [module, self(), [name: {:via, Registry, {Postgrex.TypeManager, pair}}]]

    case Supervisor.start_child(@servers, args) do
      {:ok, pid} -> pid
      {:error, {:already_started, pid}} -> pid
    end
  end

  # Callbacks

  def init(:manager) do
    manager = supervisor(Registry, [:unique, @manager])
    server_sup = supervisor(__MODULE__, [:servers])
    supervise([manager, server_sup], strategy: :rest_for_one)
  end

  def init(:servers) do
    # TypeServer is temporary so that a bad extension does not bubble up the
    # Postgrex supervision tree. Instead it should bubble up the supervision
    # tree of the application starting the Postgrex pool.
    type_server = worker(Postgrex.TypeServer, [], restart: :temporary)
    supervise([type_server], strategy: :simple_one_for_one)
  end
end
