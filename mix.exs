defmodule Postgrex.Mixfile do
  use Mix.Project

  def project do
    [ app: :postgrex,
      version: "0.4.0",
      elixir: "~> 0.12.2",
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
    [ { :ex_doc, github: "elixir-lang/ex_doc" } ] ++ deps()
  end

  defp deps(_), do: deps()

  defp deps() do
    [ { :decimal, "~> 0.0.1", github: "ericmj/decimal" } ]
  end
end
