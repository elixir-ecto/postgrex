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
    with true <- readable_pgpass_file?(),
         {:ok, contents} <- File.read(@pgpass_path),
         entries when is_list(entries) <- parse(contents) do
      entries
      |> filter_hostname(hostname)
      |> filter_port(port)
      |> filter_database(database)
      |> filter_username(username)
      |> List.first
    end
  end

  defp parse(contents) do
    contents
    |> String.split("\n")
    |> Enum.reject(&(String.match?(&1,~r/^#/)))
    |> Enum.map(&(String.split(&1, ":")))
  end

  defp readable_pgpass_file? do
    with {:ok, stat}     <- File.stat(@pgpass_path),
    do: (stat.mode &&& 0o0777) == 0o0600 || Mix.env == :test,
    else: (_ -> false)
  end

  defp filter_hostname(entries, hostname), do: Enum.filter(entries, &(Enum.at(&1, 0) == "*" || (hostname && hostname == Enum.at(&1, 0))))
  defp filter_port(entries, port), do: Enum.filter(entries, &(Enum.at(&1, 1) == "*" || (port && to_string(port) == Enum.at(&1, 1))))
  defp filter_database(entries, database), do: Enum.filter(entries, &(Enum.at(&1, 2) == "*" || (database && database == Enum.at(&1, 2))))
  defp filter_username(entries, username), do: Enum.filter(entries, &(Enum.at(&1, 3) == "*" || !username || (username && username == Enum.at(&1, 3))))

end
