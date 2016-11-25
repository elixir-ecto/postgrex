defmodule Postgrex.TypeSupervisor do
  @moduledoc false

  use Supervisor

  def start_link(mode \\ :code)

  def start_link(:code) do
    Supervisor.start_link(__MODULE__, :code)
  end
  def start_link(:types) do
    Supervisor.start_link(__MODULE__, :types, [name: __MODULE__])
  end

  def start_server(module, starter) do
    Supervisor.start_child(__MODULE__, [module, starter])
  end

  def init(:code) do
    code     = worker(Postgrex.CodeServer, [])
    type_sup = supervisor(__MODULE__, [:types])
    supervise([code, type_sup], [strategy: :rest_for_one])
  end
  def init(:types) do
    # TypeServer is temporary so that a bad extension does not bubble up the
    # Postgrex supervision tree. Instead it should bubble up the supervision
    # tree of the application starting the Postgrex pool.
    type_server = worker(Postgrex.TypeServer, [], [restart: :temporary])
    supervise([type_server], [strategy: :simple_one_for_one])
  end
end
