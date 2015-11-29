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

  @doc """
  Fills in the given `opts` with default options.
  """
  @spec default_opts(Keyword.t) :: Keyword.t
  def default_opts(opts) do
    opts
    |> Keyword.put_new(:username, System.get_env("PGUSER") || System.get_env("USER"))
    |> Keyword.put_new(:password, System.get_env("PGPASSWORD"))
    |> Keyword.put_new(:hostname, System.get_env("PGHOST") || "localhost")
    |> Keyword.put_new(:port, System.get_env("PGPORT"))
    |> Enum.reject(fn {_k, v} -> is_nil(v) end)
  end
end
