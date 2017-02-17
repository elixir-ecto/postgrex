defmodule Postgrex.Pgpass do
  @moduledoc """
  Parses .pgpass file to obtain database credentials
  """

  use Bitwise, only_operators: true

  @doc """
  Obtains the password given a keyword list containing hostname, database, port and optionally the username.
  """
  def password(opts) do
    with [_h, _p, _d, _u, password] <- pgpass_for(opts[:hostname], opts[:database], opts[:port], opts[:username], opts[:passfile]) do
      password
    else
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
         0o0600 <- stat.mode &&& 0o0777,
    do: {:ok, default_path},
    else: (_ -> false)
  end
  defp readable_pgpass_file?(passfile) do
    # when using the :passfile options, the file must be readable or an error is raised
    case File.stat(passfile) do
      {:ok, stat} ->
         case stat.mode &&& 0o0777 do
           0o0600 ->
             {:ok, passfile}
           permissions_decimal ->
             permissions =
               permissions_decimal
               |> Integer.digits(8)
               |> Enum.join
            raise RuntimeError, "passfile #{passfile} must have permissions 0600; yours was #{permissions}"
         end
      {:error,:enoent} ->
        raise RuntimeError, "passfile '#{passfile}' does not exist"
      {:error,:eacces} ->
        raise RuntimeError, "passfile '#{passfile}' cannot be read"
      {:error,:eisdir} ->
        raise RuntimeError, "passfile '#{passfile}' is a directory"
      {:error,:enomem} ->
        raise RuntimeError, "passfile '#{passfile}' is to big to read into memory"
      _ ->
        raise RuntimeError, "passfile '#{passfile}' is invalid"
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
