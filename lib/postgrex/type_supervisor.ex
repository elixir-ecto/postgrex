defmodule Postgrex.TypeSupervisor do
  @moduledoc false

  use Supervisor

  def start_link(mode \\ :manager)

  def start_link(:manager) do
    Supervisor.start_link(__MODULE__, :manager)
  end
  def start_link(:servers) do
    Supervisor.start_link(__MODULE__, :servers, [name: __MODULE__])
  end

  def start_server(module, starter) do
    Supervisor.start_child(__MODULE__, [module, starter])
  end

  def init(:manager) do
    manager    = worker(Postgrex.TypeManager, [])
    server_sup = supervisor(__MODULE__, [:servers])
    supervise([manager, server_sup], [strategy: :rest_for_one])
  end
  def init(:servers) do
    # TypeServer is temporary so that a bad extension does not bubble up the
    # Postgrex supervision tree. Instead it should bubble up the supervision
    # tree of the application starting the Postgrex pool.
    type_server = worker(Postgrex.TypeServer, [], [restart: :temporary])
    supervise([type_server], [strategy: :simple_one_for_one])
  end
end
