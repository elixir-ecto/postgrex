exclude = if System.get_env("PGVERSION") == "8.4" do
  [requires_notify_payload: true]
else
  []
end

version_exclusions = case System.get_env("PGVERSION") do
  v when is_binary(v) ->
    ["8.4", "9.0", "9.1", "9.2", "9.3", "9.4"]
    |> Enum.filter(fn x -> x > v end)
    |> Enum.map(&{:min_pg_version, &1})
  _ ->
    []
end

ExUnit.configure exclude: version_exclusions ++ exclude

ExUnit.start
{:ok, _} = :application.ensure_all_started(:crypto)

run_cmd = fn cmd ->
  key = :ecto_setup_cmd_output
  Process.put(key, "")
  status = Mix.Shell.cmd(cmd, fn(data) ->
    current = Process.get(key)
    Process.put(key, current <> data)
  end)
  output = Process.get(key)
  Process.put(key, "")
  {status, output}
end

sql = """
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
"""

sql_with_schemas = """
DROP SCHEMA IF EXISTS test;
CREATE SCHEMA test;
"""

cmds = [
  ~s(psql -U postgres -c "DROP DATABASE IF EXISTS postgrex_test;"),
  ~s(psql -U postgres -c "DROP DATABASE IF EXISTS postgrex_test_with_schemas;"),
  ~s(psql -U postgres -c "CREATE DATABASE postgrex_test TEMPLATE=template0 ENCODING='UTF8' LC_COLLATE='en_US.UTF-8' LC_CTYPE='en_US.UTF-8';"),
  ~s(psql -U postgres -c "CREATE DATABASE postgrex_test_with_schemas TEMPLATE=template0 ENCODING='UTF8' LC_COLLATE='en_US.UTF-8' LC_CTYPE='en_US.UTF-8';"),
  ~s(psql -U postgres -d postgrex_test -c "#{sql}"),
  ~s(psql -U postgres -d postgrex_test_with_schemas -c "#{sql_with_schemas}")
]

pg_version = System.get_env("PGVERSION")
pg_path = System.get_env("PGPATH")

cmds =
  cond do
    !pg_version || String.to_float(pg_version) >= 9.1 ->
      cmds ++ [~s(psql -U postgres -d postgrex_test_with_schemas -c "CREATE EXTENSION IF NOT EXISTS hstore WITH SCHEMA test;"),
               ~s(psql -U postgres -d postgrex_test -c "CREATE EXTENSION IF NOT EXISTS hstore;")]
    String.to_float(pg_version) == 9.0 ->
      cmds ++ [~s(psql -U postgres -d postgrex_test -f "#{pg_path}/contrib/hstore.sql")]
    true ->
      cmds
end

Enum.each(cmds, fn cmd ->
  {status, output} = run_cmd.(cmd)

  if status != 0 do
    IO.puts """
    Command:

    #{cmd}

    error'd with:

    #{output}

    Please verify the user "postgres" exists and it has permissions to
    create databases and users. If not, you can create a new user with:

    $ createuser postgres -s --no-password
    """
    System.halt(1)
  end
end)

defmodule Postgrex.TestHelper do
  defmacro query(stat, params, opts \\ []) do
    quote do
      case Postgrex.Connection.query(var!(context)[:pid], unquote(stat),
                                     unquote(params), unquote(opts)) do
        {:ok, %Postgrex.Result{rows: nil}} -> :ok
        {:ok, %Postgrex.Result{rows: rows}} -> rows
        {:error, %Postgrex.Error{} = err} -> err
      end
    end
  end

  defmacro prepare(name, stat, opts \\ []) do
    quote do
      case Postgrex.Connection.prepare(var!(context)[:pid], unquote(name),
                                     unquote(stat), unquote(opts)) do
        {:ok, %Postgrex.Query{} = query} -> query
        {:error, %Postgrex.Error{} = err} -> err
      end
    end
  end

  defmacro execute(query, params, opts \\ []) do
    quote do
      case Postgrex.Connection.execute(var!(context)[:pid], unquote(query),
                                       unquote(params), unquote(opts)) do
        {:ok, %Postgrex.Result{rows: nil}} -> :ok
        {:ok, %Postgrex.Result{rows: rows}} -> rows
        {:error, %Postgrex.Error{} = err} -> err
      end
    end
  end

  defmacro close(query, opts \\ []) do
    quote do
      case Postgrex.Connection.close(var!(context)[:pid], unquote(query),
                                     unquote(opts)) do
        :ok -> :ok
        {:error, %Postgrex.Error{} = err} -> err
      end
    end
  end

  defmacro transaction(fun, opts \\ []) do
    quote do
      Postgrex.Connection.transaction(var!(context)[:pid], unquote(fun),
                                      unquote(opts))
    end
  end

  def capture_log(fun) do
    Logger.remove_backend(:console)
    fun.()
    Logger.add_backend(:console, flush: true)
  end
end
