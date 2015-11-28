defmodule Postgrex.ConnectionUtils do
  @moduledoc false

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
