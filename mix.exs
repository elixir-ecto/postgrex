defmodule Postgrex.Mixfile do
  use Mix.Project

  def project do
    [ app: :postgrex,
      version: "0.0.1",
      elixir: "~> 0.10.4-dev",
      deps: deps(Mix.env),
      name: "Postgrex",
      source_url: "https://github.com/ericmj/postgrex",
      docs: fn -> [
        source_ref: System.cmd("git rev-parse --verify --quiet HEAD"),
        main: "overview",
        readme: true ]
      end ]
  end

  # Configuration for the OTP application
  def application do
    []
  end

  defp deps(:prod), do: []

  defp deps(_) do
    deps(:prod) ++
      [ { :ex_doc, github: "elixir-lang/ex_doc" } ]
  end
end
