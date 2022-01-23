defmodule Postgrex.Mixfile do
  use Mix.Project

  @source_url "https://github.com/elixir-ecto/postgrex"
  @version "0.16.0"

  def project do
    [
      app: :postgrex,
      version: @version,
      elixir: "~> 1.11",
      deps: deps(),
      name: "Postgrex",
      description: "PostgreSQL driver for Elixir",
      docs: docs(),
      package: package(),
      xref: [exclude: [Jason, :ssl]]
    ]
  end

  # Configuration for the OTP application
  def application do
    [
      extra_applications: [:logger, :crypto],
      mod: {Postgrex.App, []},
      env: [type_server_reap_after: 3 * 60_000, json_library: Jason]
    ]
  end

  defp deps do
    [
      {:ex_doc, ">= 0.0.0", only: :docs},
      {:jason, "~> 1.0", optional: true},
      {:decimal, "~> 1.5 or ~> 2.0"},
      {:db_connection, "~> 2.1"},
      {:connection, "~> 1.1"}
    ]
  end

  defp docs do
    [
      source_url: @source_url,
      source_ref: "v#{@version}",
      main: "readme",
      extras: ["README.md", "CHANGELOG.md"],
      skip_undefined_reference_warnings_on: ["CHANGELOG.md"],
      groups_for_modules: [
        # Postgrex
        # Postgrex.Notifications
        # Postgrex.Query
        # Postgrex.ReplicationConnection
        # Postgrex.SimpleConnection
        # Postgrex.Stream
        # Postgrex.Result
        "Data Types": [
          Postgrex.Box,
          Postgrex.Circle,
          Postgrex.INET,
          Postgrex.Interval,
          Postgrex.Lexeme,
          Postgrex.Line,
          Postgrex.LineSegment,
          Postgrex.MACADDR,
          Postgrex.Path,
          Postgrex.Point,
          Postgrex.Polygon,
          Postgrex.Range
        ],
        "Custom types and Extensions": [
          Postgrex.DefaultTypes,
          Postgrex.Extension,
          Postgrex.TypeInfo,
          Postgrex.Types
        ]
      ]
    ]
  end

  defp package do
    [
      maintainers: ["Eric Meadows-Jönsson", "James Fish"],
      licenses: ["Apache-2.0"],
      links: %{"GitHub" => @source_url}
    ]
  end
end
