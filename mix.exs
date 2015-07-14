defmodule Postgrex.Mixfile do
  use Mix.Project

  def project do
    [app: :postgrex,
     version: "0.9.1",
     elixir: "~> 1.0",
     deps: deps,
     build_per_environment: false,
     name: "Postgrex",
     source_url: "https://github.com/ericmj/postgrex",
     docs: fn ->
       {ref, 0} = System.cmd("git", ["rev-parse", "--verify", "--quiet", "HEAD"])
       [source_ref: ref, main: "README", readme: "README.md"]
     end,
     description: description,
     package: package]
  end

  # Configuration for the OTP application
  def application do
    [applications: [:logger],
     mod: {Postgrex, []},
     env: [type_server_reap_after: 3 * 60_000]]
  end

  defp deps do
    [{:ex_doc, only: :dev},
     {:earmark, only: :dev},
     {:decimal, "~> 1.0"}]
  end

  defp description do
    "PostgreSQL driver for Elixir."
  end

  defp package do
    [contributors: ["Eric Meadows-Jönsson"],
     licenses: ["Apache 2.0"],
     links: %{"Github" => "https://github.com/ericmj/postgrex"}]
  end
end
