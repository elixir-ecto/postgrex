defmodule Postgrex.Mixfile do
  use Mix.Project

  @version "0.14.0-dev"

  def project do
    [app: :postgrex,
     version: @version,
     elixir: "~> 1.3.4 or ~> 1.4",
     deps: deps(),
     name: "Postgrex",
     source_url: "https://github.com/elixir-ecto/postgrex",
     docs: [source_ref: "v#{@version}", main: "readme", extras: ["README.md"]],
     description: description(),
     package: package()]
  end

  # Configuration for the OTP application
  def application do
    [applications: [:logger, :db_connection, :decimal, :crypto],
     mod: {Postgrex.App, []},
     env: [type_server_reap_after: 3 * 60_000]]
  end

  defp deps do
    [{:ex_doc, "~> 0.14", only: :docs},
     {:decimal, "~> 1.0"},
     {:db_connection, "~> 1.1", github: "elixir-ecto/db_connection", branch: "jf-stream_stage"},
     {:connection, "~> 1.0"},
     {:gen_stage, "~> 0.11", github: "elixir-lang/gen_stage",
        branch: "jv-exit-signals", optional: true},
     {:flow, "~> 0.11", github: "elixir-lang/flow",
        branch: "jv-exit-signals", optional: true}]
  end

  defp description do
    "PostgreSQL driver for Elixir."
  end

  defp package do
    [maintainers: ["Eric Meadows-Jönsson", "James Fish"],
     licenses: ["Apache 2.0"],
     links: %{"Github" => "https://github.com/elixir-ecto/postgrex"}]
  end
end
