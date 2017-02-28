defmodule Postgrex.Pgpass do
  @moduledoc """
  Parses .pgpass file to obtain database credentials
  """

  use Bitwise, only_operators: true

  @doc """
  Obtains the password given a keyword list containing hostname, database, port and optionally the username.
  """
  def password(opts) do
    case pgpass_for(opts[:hostname], opts[:database], opts[:port], opts[:username], opts[:passfile]) do
      [_h, _p, _d, _u, password] -> password
      _ -> nil
    end
  end

  defp pgpass_for(hostname, database, port, username, passfile) do
    with {:ok, path} <- readable_pgpass_file?(passfile),
         {:ok, contents} <- File.read(path),
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

  defp readable_pgpass_file?(nil) do
    # when the pgpass is not passed via the :passfile option, simply ignore and move on
    with default_path <- pgpass_path(),
         {:ok, stat} <- File.stat(default_path),
         0o0600 <- stat.mode &&& 0o0777 do
      {:ok, default_path}
    else
      permissions when is_integer(permissions) ->
        IO.warn "WARNING: passfile \"#{pgpass_path()}\" has group or world access (#{inspect(permissions, [base: :octal])}); permissions should be u=rw (0600) or less"
        false
      _ ->
        false
    end
  end
  defp readable_pgpass_file?(passfile) do
    # when using the :passfile options, the file must be readable or an error is raised
    with stat <- File.stat!(passfile),
         0o0600 <- stat.mode &&& 0o0777 do
      {:ok, passfile}
    else
      permissions ->
        raise RuntimeError, "ERROR: passfile \"#{passfile}\" has group or world access (#{inspect(permissions, [base: :octal])}); permissions should be u=rw (0600) or less"
    end
  end

  defp filter_hostname(entries, hostname), do: Enum.filter(entries, &(Enum.at(&1, 0) == "*" || (hostname && hostname == Enum.at(&1, 0))))
  defp filter_port(entries, port), do: Enum.filter(entries, &(Enum.at(&1, 1) == "*" || (port && to_string(port) == Enum.at(&1, 1))))
  defp filter_database(entries, database), do: Enum.filter(entries, &(Enum.at(&1, 2) == "*" || (database && database == Enum.at(&1, 2))))
  defp filter_username(entries, username), do: Enum.filter(entries, &(Enum.at(&1, 3) == "*" || (username && username == Enum.at(&1, 3))))

  defp pgpass_path, do: System.get_env("PGPASSFILE") || os_default_location()

  defp os_default_location do
    case :os.type do
      {:unix, _} -> Path.join(System.user_home, ".pgpass")
      {:win32, _} -> System.get_env("APPDATA") && Path.join(System.get_env("APPDATA"), "postgresql/pgpass.conf")
      _ -> nil
    end
  end

end
