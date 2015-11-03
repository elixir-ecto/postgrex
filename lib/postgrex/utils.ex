defmodule Postgrex.Utils do
  @moduledoc false

  @doc """
  Converts pg major.minor.patch (http://www.postgresql.org/support/versioning) version to an integer
  """
  def parse_version(version) do
    list =
      version
      |> String.split(".")
      |> Enum.map(&elem(Integer.parse(&1), 0))

    case list do
      [major, minor, patch] -> {major, minor, patch}
      [major, minor] -> {major, minor, 0}
      [major] -> {major, 0, 0}
    end
  end
end
