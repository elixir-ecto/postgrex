defmodule Postgrex.App do
  @moduledoc false
  use Application

  def start(_, _) do
    import Supervisor.Spec
    opts = [strategy: :one_for_one, name: Postgrex.Supervisor]
    children = [supervisor(Postgrex.TypeSupervisor, []),
                worker(Postgrex.Parameters, [])]
    Supervisor.start_link(children, opts)
  end
end
