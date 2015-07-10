defmodule Postgrex do
  @moduledoc """
  PostgreSQL driver for Elixir.
  """

  use Application

  def start(_, _) do
    import Supervisor.Spec
    opts = [strategy: :one_for_one, name: Postgrex.Supervisor]
    Supervisor.start_link([worker(Postgrex.TypeServer, [])], opts)
  end
end
