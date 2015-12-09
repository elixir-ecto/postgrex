defmodule Postgrex.Connection do
  @moduledoc """
  Main API for Postgrex. This module handles the connection to postgres.

  Note that the notifications API (pub/sub) supported by Postgres is handled by
  `Postgrex.Notifications` and not by this module. Hence, to use this feature,
  you need to start a separate (notifications) connection.
  """

  alias Postgrex.Query

  @timeout 5000

  ### PUBLIC API ###

  @doc """
  Start the connection process and connect to postgres.

  ## Options

    * `:hostname` - Server hostname (default: PGHOST env variable, then localhost);
    * `:port` - Server port (default: 5432);
    * `:database` - Database (required);
    * `:username` - Username (default: PGUSER env variable, then USER env var);
    * `:password` - User password (default PGPASSWORD);
    * `:parameters` - Keyword list of connection parameters;
    * `:timeout` - Connect timeout in milliseconds (default: `#{@timeout}`);
    * `:ssl` - Set to `true` if ssl should be used (default: `false`);
    * `:ssl_opts` - A list of ssl options, see ssl docs;
    * `:socket_options` - Options to be given to the underlying socket;
    * `:sync_connect` - Block in `start_link/1` until connection is set up (default: `false`)
    * `:extensions` - A list of `{module, opts}` pairs where `module` is
      implementing the `Postgrex.Extension` behaviour and `opts` are the
      extension options;
  """
  @spec start_link(Keyword.t) :: {:ok, pid} | {:error, Postgrex.Error.t | term}
  def start_link(opts) do
    DBConnection.start_link(Postgrex.Protocol, Postgrex.Utils.default_opts(opts))
  end

  @doc """
  Runs an (extended) query and returns the result as `{:ok, %Postgrex.Result{}}`
  or `{:error, %Postgrex.Error{}}` if there was an error. Parameters can be
  set in the query as `$1` embedded in the query string. Parameters are given as
  a list of elixir values. See the README for information on how Postgrex
  encodes and decodes Elixir values by default. See `Postgrex.Result` for the
  result data.

  ## Options

    * `:timeout` - Call timeout (default: `#{@timeout}`)
    * `:decode`  - Decode method: `:auto` decodes the result and `:manual` does
    not (default: `:auto`)

  ## Examples

      Postgrex.Connection.query(pid, "CREATE TABLE posts (id serial, title text)", [])

      Postgrex.Connection.query(pid, "INSERT INTO posts (title) VALUES ('my title')", [])

      Postgrex.Connection.query(pid, "SELECT title FROM posts", [])

      Postgrex.Connection.query(pid, "SELECT id FROM posts WHERE title like $1", ["%my%"])

  """
  @spec query(pid, iodata, list, Keyword.t) :: {:ok, Postgrex.Result.t} | {:error, Postgrex.Error.t}
  def query(pid, statement, params, opts \\ []) do
    query = %Query{name: "", statement: statement, params: params}
    DBConnection.query(pid, query, opts)
  end

  @doc """
  Runs an (extended) query and returns the result or raises `Postgrex.Error` if
  there was an error. See `query/3`.
  """
  @spec query!(pid, iodata, list, Keyword.t) :: Postgrex.Result.t
  def query!(pid, statement, params, opts \\ []) do
    query = %Query{name: "", statement: statement, params: params}
    DBConnection.query!(pid, query, opts)
  end

  @doc """
  Prepares an (extended) query and returns the result as
  `{:ok, %Postgrex.Query{}}` or `{:error, %Postgrex.Error{}}` if there was an
  error. Parameters can be set in the query as `$1` embedded in the query
  string. To execute the query call `execute/4`. To close the prepared query
  call `close/3`. See `Postgrex.Query` for the query data.

  ## Options

    * `:timeout` - Call timeout (default: `#{@timeout}`)
    * `:decode`  - Decode method: `:auto` decodes the result and `:manual` does

  ## Examples

      Postgrex.Connection.prepare(pid, "CREATE TABLE posts (id serial, title text)")
  """
  @spec prepare(pid, iodata, iodata, Keyword.t) :: {:ok, Postgrex.Query.t} | {:error, Postgrex.Error.t}
  def prepare(pid, name, statement, opts \\ []) do
    DBConnection.prepare(pid, %Query{name: name, statement: statement}, opts)
  end

  @doc """
  Prepared an (extended) query and returns the prepared query or raises
  `Postgrex.Error` if there was an error. See `prepare/4`.
  """
  @spec prepare!(pid, iodata, iodata, Keyword.t) :: Postgrex.Query.t
  def prepare!(pid, name, statement, opts \\ []) do
    DBConnection.prepare!(pid, %Query{name: name, statement: statement}, opts)
  end

  @doc """
  Runs an (extended) prepared query and returns the result as
  `{:ok, %Postgrex.Result{}}` or `{:error, %Postgrex.Error{}}` if there was an
  error. Parameters are given as part of the prepared query, `%Postgrex.Query{}`.
  See the README for information on how Postgrex encodes and decodes Elixir
  values by default. See `Postgrex.Query` for the query data and
  `Postgrex.Result` for the result data.

  ## Options

    * `:timeout` - Call timeout (default: `#{@timeout}`)
    * `:decode`  - Decode method: `:auto` decodes the result and `:manual` does
    not (default: `:auto`)

  ## Examples

      query = Postgrex.Connection.prepare!(pid, "CREATE TABLE posts (id serial, title text)")
      Postgrex.Connection.execute(pid, query)

      query = Postgrex.Connection.prepare!(pid, "SELECT id FROM posts WHERE title like $1")
      Postgrex.Connection.execute(pid, %Postgrex.Query{query | params: ["%my%"]}
  """
  @spec execute(pid, Postgrex.Query.t, Keyword.t) ::
    {:ok, Postgrex.Result.t} | {:error, Postgrex.Error.t}
  def execute(pid, query, opts \\ []) do
    DBConnection.execute(pid, query, opts)
  end

  @doc """
  Runs an (extended) prepared query and returns the result or raises
  `Postgrex.Error` if there was an error. See `execute/3`.
  """
  @spec execute!(pid, Postgrex.Query.t, Keyword.t) :: Postgrex.Result.t
  def execute!(pid, query, opts \\ []) do
    DBConnection.execute!(pid, query, opts)
  end

  @doc """
  Closes an (extended) prepared query and returns `:ok` or
  `{:error, %Postgrex.Error{}}` if there was an error. Closing a query releases
  any resources held by postgresql for a prepared query with that name. See
  `Postgrex.Query` for the query data.

  ## Options

    * `:timeout` - Call timeout (default: `#{@timeout}`)

  ## Examples

      query = Postgrex.Connection.prepare!(pid, "CREATE TABLE posts (id serial, title text)")
      Postgrex.Connection.close(pid, query)
  """
  @spec close(pid, Postgrex.Query.t, Keyword.t) :: :ok | {:error, Postgrex.Error.t}
  def close(pid, query, opts \\ []) do
    DBConnection.close(pid, query, opts)
  end

  @doc """
  Closes an (extended) prepared query and returns `:ok` or raises
  `Postgrex.Error` if there was an error. See `close/3`.
  """
  @spec close!(pid, Postgrex.Query.t, Keyword.t) :: :ok
  def close!(pid, query, opts \\ []) do
    DBConnection.close!(pid, query, opts)
  end

  @doc """
  Returns a cached map of connection parameters.
  ## Options
  * `:timeout` - Call timeout (default: `#{@timeout}`)
  """
  @spec parameters(pid, Keyword.t) :: %{binary => binary}
  def parameters(pid, opts \\ []) do
    DBConnection.execute!(pid, %Postgrex.Parameters{}, opts)
  end
end
