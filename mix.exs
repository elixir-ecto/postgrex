defmodule Postgrex.Mixfile do
  use Mix.Project

  def project do
    [ app: :postgrex,
      version: "0.3.0-dev",
      elixir: "~> 0.11.2 or ~> 0.12.0",
      deps: deps(Mix.env),
      name: "Postgrex",
      source_url: "https://github.com/ericmj/postgrex",
      docs: fn -> [
        source_ref: System.cmd("git rev-parse --verify --quiet HEAD"),
        main: "README",
        readme: true ]
      end ]
  end

  # Configuration for the OTP application
  def application do
    []
  end

  defp deps(:dev) do
    [ { :ex_doc, github: "elixir-lang/ex_doc" } ]
  end

  defp deps(_), do: []
end
