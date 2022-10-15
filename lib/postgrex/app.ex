defmodule Postgrex.App do
  @moduledoc false
  use Application

  def start(_, _) do
    opts = [strategy: :one_for_one, name: Postgrex.Supervisor]

    children = [
      {Postgrex.TypeSupervisor, :manager},
      Postgrex.Parameters,
      Postgrex.SCRAM.LockedCache
    ]

    Supervisor.start_link(children, opts)
  end
end
