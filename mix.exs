defmodule Postgrex.Mixfile do
  use Mix.Project

  def project do
    [app: :postgrex,
     version: "0.5.5-dev",
     elixir: "~> 0.15.0",
     deps: deps,
     build_per_environment: false,
     name: "Postgrex",
     source_url: "https://github.com/ericmj/postgrex",
     docs: fn ->
       {ref, 0} = System.cmd("git", ["rev-parse", "--verify", "--quiet", "HEAD"])
       [source_ref: ref, main: "README", readme: true]
     end,
     description: description,
     package: package]
  end

  # Configuration for the OTP application
  def application do
    []
  end

  defp deps do
    [{:ex_doc, github: "elixir-lang/ex_doc", only: :dev},
     {:markdown, github: "devinus/markdown", only: :dev},
     {:decimal, "~> 0.2.3"}]
  end

  defp description do
    """
    PostgreSQL driver for Elixir.
    """
  end

  defp package do
    [contributors: ["Eric Meadows-Jönsson"],
     licenses: ["Apache 2.0"],
     links: %{"Github" => "https://github.com/ericmj/postgrex",
              "Documentation" => "http://ericmj.github.io/postgrex"}]
  end
end
