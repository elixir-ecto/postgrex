defmodule Postgrex.Pgpass do
  @moduledoc """
  Parses .pgpass file to obtain database credentials
  """

  @pgpass_path Application.get_env(:postgrex, :pgpass, Path.join(System.user_home, '.pgpass'))
  use Bitwise, only_operators: true

  @doc """
  Obtains the username given a keyword list containing hostname, database & port
  """
  def username(opts) do
    with [_h, _p, _d, username, _pwd] <- pgpass_for(opts[:hostname], opts[:database], opts[:port]) do
      username
    else
      _ -> nil
    end
  end

  @doc """
  Obtains the password given a keyword list containing hostname, database, port and optionally the username.
  """
  def password(opts) do
    with [_h, _p, _d, _u, password] <- pgpass_for(opts[:hostname], opts[:database], opts[:port], opts[:username]) do
      password
    else
      _ -> nil
    end
  end

  defp pgpass_for(hostname, database, port, username \\ nil) do
    with {:ok, stat}     <- File.stat(@pgpass_path),
         0o0600          <- stat.mode &&& 0o0777, # don't read pgpass without proper permissions
         {:ok, contents} <- File.read(@pgpass_path) do
      contents
      |> parse
      |> Enum.filter(&(Enum.at(&1, 0) == "*" || (hostname && hostname == Enum.at(&1, 0)))) # matches hostname
      |> Enum.filter(&(Enum.at(&1, 1) == "*" || (port && to_string(port) == Enum.at(&1, 1)))) # matches port
      |> Enum.filter(&(Enum.at(&1, 2) == "*" || (database && database == Enum.at(&1, 2)))) # matches database
      |> Enum.filter(&(Enum.at(&1, 3) == "*" || !username || (username && username == Enum.at(&1, 3)))) # matches username
      |> List.first
    end
  end

  defp parse(contents) do
    contents
    |> String.split("\n")
    |> Enum.reject(&(String.match?(&1,~r/^#/)))
    |> Enum.map(&(String.split(&1, ":")))
  end
end
