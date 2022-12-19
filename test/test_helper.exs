defmodule PSQL do
  @pg_env %{"PGUSER" => System.get_env("PGUSER") || "postgres"}

  def cmd(args) do
    {output, status} = System.cmd("psql", args, stderr_to_stdout: true, env: @pg_env)

    if status != 0 do
      IO.puts("""
      Command:

      psql #{Enum.join(args, " ")}

      error'd with:

      #{output}

      Please verify the user "postgres" exists and it has permissions to
      create databases and users. If not, you can create a new user with:

      $ createuser postgres -s --no-password
      """)

      System.halt(1)
    end

    output
  end

  def vsn do
    vsn_select = cmd(["-c", "SELECT version();"])
    [_, major, minor] = Regex.run(~r/PostgreSQL (\d+).(\d+)/, vsn_select)
    {String.to_integer(major), String.to_integer(minor)}
  end

  def supports_sockets? do
    otp_release = :otp_release |> :erlang.system_info() |> List.to_integer()
    unix_socket_dir = System.get_env("PG_SOCKET_DIR") || "/tmp"
    port = System.get_env("PGPORT") || "5432"
    unix_socket_path = Path.join(unix_socket_dir, ".s.PGSQL.#{port}")
    otp_release >= 20 and File.exists?(unix_socket_path)
  end

  def supports_ssl? do
    cmd(["-c", "SHOW ssl"]) =~ "on"
  end

  def supports_logical_replication? do
    cmd(["-c", "SHOW wal_level"]) =~ "logical"
  end
end

pg_version = PSQL.vsn()
unix_exclude = if PSQL.supports_sockets?(), do: [], else: [unix: true]
ssl_exclude = if PSQL.supports_ssl?(), do: [], else: [ssl: true]
notify_exclude = if pg_version == {8, 4}, do: [requires_notify_payload: true], else: []

replication_exclude =
  if pg_version < {10, 0} or PSQL.supports_logical_replication?() do
    []
  else
    IO.puts(:stderr, """
    !!! Skipping replication tests because wal_level is not set to logical.

    To run them, you must run the following commands and restart your database:

        ALTER SYSTEM SET wal_level='logical';
        ALTER SYSTEM SET max_wal_senders='10';
        ALTER SYSTEM SET max_replication_slots='10';
    """)

    [logical_replication: true]
  end

version_exclude =
  [{8, 4}, {9, 0}, {9, 1}, {9, 2}, {9, 3}, {9, 4}, {9, 5}, {10, 0}, {13, 0}, {14, 0}]
  |> Enum.filter(fn x -> x > pg_version end)
  |> Enum.map(fn {major, minor} -> {:min_pg_version, "#{major}.#{minor}"} end)

excludes = version_exclude ++ replication_exclude ++ notify_exclude ++ unix_exclude ++ ssl_exclude
ExUnit.start(exclude: excludes, assert_receive_timeout: 1000)

sql_test = """
DROP ROLE IF EXISTS postgrex_cleartext_pw;
DROP ROLE IF EXISTS postgrex_md5_pw;

CREATE USER postgrex_cleartext_pw WITH PASSWORD 'postgrex_cleartext_pw';
CREATE USER postgrex_md5_pw WITH PASSWORD 'postgrex_md5_pw';

DROP TABLE IF EXISTS composite1;
CREATE TABLE composite1 (a int, b text);

DROP TABLE IF EXISTS composite2;
CREATE TABLE composite2 (a int, b int, c int);

DROP TYPE IF EXISTS enum1;
CREATE TYPE enum1 AS ENUM ('elixir', 'erlang');

CREATE TABLE uniques (a int UNIQUE);

DROP TABLE IF EXISTS missing_oid;
DROP TYPE IF EXISTS missing_enum;
DROP TYPE IF EXISTS missing_comp;

CREATE TABLE altering (a int2);

CREATE TABLE calendar (a timestamp without time zone, b timestamp with time zone);

DROP DOMAIN IF EXISTS points_domain;
CREATE DOMAIN points_domain AS point[] CONSTRAINT is_populated CHECK (COALESCE(array_length(VALUE, 1), 0) >= 1);

DROP DOMAIN IF EXISTS floats_domain;
CREATE DOMAIN floats_domain AS float[] CONSTRAINT is_populated CHECK (COALESCE(array_length(VALUE, 1), 0) >= 1);
"""

sql_test =
  if pg_version >= {10, 0} do
    sql_test <>
      """
      DROP ROLE IF EXISTS postgrex_scram_pw;
      SET password_encryption = 'scram-sha-256';
      CREATE USER postgrex_scram_pw WITH PASSWORD 'postgrex_scram_pw';
      CREATE PUBLICATION postgrex_example FOR ALL TABLES;
      """
  else
    sql_test
  end

sql_test_with_schemas = """
DROP SCHEMA IF EXISTS test;
CREATE SCHEMA test;
"""

PSQL.cmd(["-c", "DROP DATABASE IF EXISTS postgrex_test;"])
PSQL.cmd(["-c", "DROP DATABASE IF EXISTS postgrex_test_with_schemas;"])

PSQL.cmd([
  "-c",
  "CREATE DATABASE postgrex_test TEMPLATE=template0 ENCODING='UTF8' LC_COLLATE='en_US.UTF-8' LC_CTYPE='en_US.UTF-8';"
])

PSQL.cmd([
  "-c",
  "CREATE DATABASE postgrex_test_with_schemas TEMPLATE=template0 ENCODING='UTF8' LC_COLLATE='en_US.UTF-8' LC_CTYPE='en_US.UTF-8';"
])

PSQL.cmd(["-d", "postgrex_test", "-c", sql_test])
PSQL.cmd(["-d", "postgrex_test_with_schemas", "-c", sql_test_with_schemas])

cond do
  pg_version >= {9, 1} ->
    PSQL.cmd([
      "-d",
      "postgrex_test_with_schemas",
      "-c",
      "CREATE EXTENSION IF NOT EXISTS hstore WITH SCHEMA test;"
    ])

    PSQL.cmd(["-d", "postgrex_test", "-c", "CREATE EXTENSION IF NOT EXISTS hstore;"])

  pg_version == {9, 0} ->
    pg_path = System.get_env("PGPATH")
    PSQL.cmd(["-d", "postgrex_test", "-f", "#{pg_path}/contrib/hstore.sql"])

  pg_version < {9, 0} ->
    PSQL.cmd(["-d", "postgrex_test", "-c", "CREATE LANGUAGE plpgsql;"])

  true ->
    :ok
end

PSQL.cmd(["-d", "postgrex_test", "-c", "CREATE EXTENSION IF NOT EXISTS ltree;"])

defmodule Postgrex.TestHelper do
  defmacro query(stat, params, opts \\ []) do
    quote do
      case Postgrex.query(var!(context)[:pid], unquote(stat), unquote(params), unquote(opts)) do
        {:ok, %Postgrex.Result{rows: nil}} -> :ok
        {:ok, %Postgrex.Result{rows: rows}} -> rows
        {:error, err} -> err
      end
    end
  end

  defmacro prepare(name, stat, opts \\ []) do
    quote do
      case Postgrex.prepare(var!(context)[:pid], unquote(name), unquote(stat), unquote(opts)) do
        {:ok, %Postgrex.Query{} = query} -> query
        {:error, err} -> err
      end
    end
  end

  defmacro prepare_execute(name, stat, params, opts \\ []) do
    quote do
      case Postgrex.prepare_execute(
             var!(context)[:pid],
             unquote(name),
             unquote(stat),
             unquote(params),
             unquote(opts)
           ) do
        {:ok, %Postgrex.Query{} = query, %Postgrex.Result{rows: rows}} -> {query, rows}
        {:error, err} -> err
      end
    end
  end

  defmacro execute(query, params, opts \\ []) do
    quote do
      case Postgrex.execute(var!(context)[:pid], unquote(query), unquote(params), unquote(opts)) do
        {:ok, %Postgrex.Query{}, %Postgrex.Result{rows: nil}} -> :ok
        {:ok, %Postgrex.Query{}, %Postgrex.Result{rows: rows}} -> rows
        {:error, err} -> err
      end
    end
  end

  defmacro stream(query, params, opts \\ []) do
    quote do
      Postgrex.stream(var!(conn), unquote(query), unquote(params), unquote(opts))
    end
  end

  defmacro close(query, opts \\ []) do
    quote do
      case Postgrex.close(var!(context)[:pid], unquote(query), unquote(opts)) do
        :ok -> :ok
        {:error, err} -> err
      end
    end
  end

  defmacro transaction(fun, opts \\ []) do
    quote do
      Postgrex.transaction(var!(context)[:pid], unquote(fun), unquote(opts))
    end
  end
end
