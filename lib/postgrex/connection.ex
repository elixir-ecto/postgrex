defmodule Postgrex.Connection do
  @moduledoc """
  Main API for Postgrex. This module handles the connection to postgres.

  Note that the notifications API (pub/sub) supported by Postgres is handled by
  `Postgrex.Notifications` and not by this module. Hence, to use this feature,
  you need to start a separate (notifications) connection.
  """

  alias Postgrex.Query

  @typedoc """
  A connection process name, pid or reference.

  A connection reference is used when making multiple requests to the same
  connection, see `transaction/3` and `:after_connect` in `start_link/1`.
  """
  @type conn :: DBConnection.conn

  @pool_timeout 5000
  @timeout 5000
  @idle_timeout 5000

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
    * `:after_connect` - A function to run on connect, either a 1-arity fun
    called with a connection reference, `{module, function, args}` with the
    connection reference prepended to `args` or `nil`, (default: `nil`)
    * `:idle_timeout` - Idle timeout to ping postgres to maintain a connection
    (default: `#{@idle_timeout}`)
    * `:backoff_start` - The first backoff interval when reconnecting (default:
    `200`);
    * `:backoff_max` - The maximum backoff interval when reconnecting (default:
    `15_000`);
    * `:backoff_type` - The backoff strategy when reconnecting, `:stop` for no
    backoff and to stop (see `:backoff`, default: `:jitter`)
    * `:transactions` - Set to `:strict` to error on unexpected transaction
    state, otherwise set to `naive` (default: `:naive`);
    * `:pool` - The pool module to use, see `DBConnection`, it must be
    included with all requests if not the default (default:
    `DBConnection.Connection`);
  """
  @spec start_link(Keyword.t) :: {:ok, pid} | {:error, Postgrex.Error.t | term}
  def start_link(opts) do
    opts = [types: true] ++ Postgrex.Utils.default_opts(opts)
    DBConnection.start_link(Postgrex.Protocol, opts)
  end

  @doc """
  Runs an (extended) query and returns the result as `{:ok, %Postgrex.Result{}}`
  or `{:error, %Postgrex.Error{}}` if there was an error. Parameters can be
  set in the query as `$1` embedded in the query string. Parameters are given as
  a list of elixir values. See the README for information on how Postgrex
  encodes and decodes Elixir values by default. See `Postgrex.Result` for the
  result data.

  ## Options

    * `:pool_timeout` - Time to wait in the queue for the connection
    (default: `#{@pool_timeout}`)
    * `:queue` - Whether to wait for connection in a queue (default: `true`);
    * `:timeout` - Query request timeout (default: `#{@timeout}`);
    * `:encode_mapper` - Fun to map each parameter before encoding, see
    (default: `fn x -> x end`)
    * `:decode_mapper` - Fun to map each row in the result to a term after
    decoding, (default: `fn x -> x end`);
    * `:pool` - The pool module to use, must match that set on
    `start_link/1`, see `DBConnection`
    * `:proxy` - The proxy module for the request, if any, see
    `DBConnection.Proxy` (default: `nil`);

  ## Examples

      Postgrex.Connection.query(conn, "CREATE TABLE posts (id serial, title text)", [])

      Postgrex.Connection.query(conn, "INSERT INTO posts (title) VALUES ('my title')", [])

      Postgrex.Connection.query(conn, "SELECT title FROM posts", [])

      Postgrex.Connection.query(conn, "SELECT id FROM posts WHERE title like $1", ["%my%"])

  """
  @spec query(conn, iodata, list, Keyword.t) :: {:ok, Postgrex.Result.t} | {:error, Postgrex.Error.t}
  def query(conn, statement, params, opts \\ []) do
    query = %Query{name: "", statement: statement}
    case DBConnection.query(conn, query, params, defaults(opts)) do
      {:error, %ArgumentError{} = err} ->
        raise err
      other ->
        other
    end
  end

  @doc """
  Runs an (extended) query and returns the result or raises `Postgrex.Error` if
  there was an error. See `query/3`.
  """
  @spec query!(conn, iodata, list, Keyword.t) :: Postgrex.Result.t
  def query!(conn, statement, params, opts \\ []) do
    query = %Query{name: "", statement: statement}
    DBConnection.query!(conn, query, params, defaults(opts))
  end

  @doc """
  Prepares an (extended) query and returns the result as
  `{:ok, %Postgrex.Query{}}` or `{:error, %Postgrex.Error{}}` if there was an
  error. Parameters can be set in the query as `$1` embedded in the query
  string. To execute the query call `execute/4`. To close the prepared query
  call `close/3`. See `Postgrex.Query` for the query data.

  ## Options

    * `:pool_timeout` - Time to wait in the queue for the connection
    (default: `#{@pool_timeout}`)
    * `:queue` - Whether to wait for connection in a queue (default: `true`);
    * `:timeout` - Prepare request timeout (default: `#{@timeout}`);
    * `:pool` - The pool module to use, must match that set on
    `start_link/1`, see `DBConnection`
    * `:proxy` - The proxy module for the request, if any, see
    `DBConnection.Proxy` (default: `nil`);

  ## Examples

      Postgrex.Connection.prepare(conn, "CREATE TABLE posts (id serial, title text)")
  """
  @spec prepare(conn, iodata, iodata, Keyword.t) :: {:ok, Postgrex.Query.t} | {:error, Postgrex.Error.t}
  def prepare(conn, name, statement, opts \\ []) do
    query = %Query{name: name, statement: statement}
    case DBConnection.prepare(conn, query, defaults(opts)) do
      {:error, %ArgumentError{} = err} ->
        raise err
      other ->
        other
    end
  end

  @doc """
  Prepared an (extended) query and returns the prepared query or raises
  `Postgrex.Error` if there was an error. See `prepare/4`.
  """
  @spec prepare!(conn, iodata, iodata, Keyword.t) :: Postgrex.Query.t
  def prepare!(conn, name, statement, opts \\ []) do
    DBConnection.prepare!(conn, %Query{name: name, statement: statement}, defaults(opts))
  end

  @doc """
  Runs an (extended) prepared query and returns the result as
  `{:ok, %Postgrex.Result{}}` or `{:error, %Postgrex.Error{}}` if there was an
  error. Parameters are given as part of the prepared query, `%Postgrex.Query{}`.
  See the README for information on how Postgrex encodes and decodes Elixir
  values by default. See `Postgrex.Query` for the query data and
  `Postgrex.Result` for the result data.

  ## Options

    * `:pool_timeout` - Time to wait in the queue for the connection
    (default: `#{@pool_timeout}`)
    * `:queue` - Whether to wait for connection in a queue (default: `true`);
    * `:timeout` - Execute request timeout (default: `#{@timeout}`);
    * `:encode_mapper` - Fun to map each parameter before encoding, see
    (default: `fn x -> x end`)
    * `:decode_mapper` - Fun to map each row in the result to a term after
    decoding, (default: `fn x -> x end`);
    * `:pool` - The pool module to use, must match that set on
    `start_link/1`, see `DBConnection`
    * `:proxy` - The proxy module for the request, if any, see
    `DBConnection.Proxy` (default: `nil`);

  ## Examples

      query = Postgrex.Connection.prepare!(conn, "CREATE TABLE posts (id serial, title text)")
      Postgrex.Connection.execute(conn, query, [])

      query = Postgrex.Connection.prepare!(conn, "SELECT id FROM posts WHERE title like $1")
      Postgrex.Connection.execute(conn, query, ["%my%"])
  """
  @spec execute(conn, Postgrex.Query.t, list, Keyword.t) ::
    {:ok, Postgrex.Result.t} | {:error, Postgrex.Error.t}
  def execute(conn, query, params, opts \\ []) do
    case DBConnection.execute(conn, query, params, defaults(opts)) do
      {:error, %ArgumentError{} = err} ->
        raise err
      other ->
        other
    end
  end

  @doc """
  Runs an (extended) prepared query and returns the result or raises
  `Postgrex.Error` if there was an error. See `execute/4`.
  """
  @spec execute!(conn, Postgrex.Query.t, list, Keyword.t) :: Postgrex.Result.t
  def execute!(conn, query, params, opts \\ []) do
    DBConnection.execute!(conn, query, params, defaults(opts))
  end

  @doc """
  Closes an (extended) prepared query and returns `:ok` or
  `{:error, %Postgrex.Error{}}` if there was an error. Closing a query releases
  any resources held by postgresql for a prepared query with that name. See
  `Postgrex.Query` for the query data.

  ## Options

    * `:pool_timeout` - Time to wait in the queue for the connection
    (default: `#{@pool_timeout}`)
    * `:queue` - Whether to wait for connection in a queue (default: `true`);
    * `:timeout` - Close request timeout (default: `#{@timeout}`);
    * `:pool` - The pool module to use, must match that set on
    `start_link/1`, see `DBConnection`
    * `:proxy` - The proxy module for the request, if any, see
    `DBConnection.Proxy` (default: `nil`);

  ## Examples

      query = Postgrex.Connection.prepare!(conn, "CREATE TABLE posts (id serial, title text)")
      Postgrex.Connection.close(conn, query)
  """
  @spec close(conn, Postgrex.Query.t, Keyword.t) :: :ok | {:error, Postgrex.Error.t}
  def close(conn, query, opts \\ []) do
    case DBConnection.close(conn, query, defaults(opts)) do
      {:error, %ArgumentError{} = err} ->
        raise err
      other ->
        other
    end
  end

  @doc """
  Closes an (extended) prepared query and returns `:ok` or raises
  `Postgrex.Error` if there was an error. See `close/3`.
  """
  @spec close!(conn, Postgrex.Query.t, Keyword.t) :: :ok
  def close!(conn, query, opts \\ []) do
    DBConnection.close!(conn, query, defaults(opts))
  end

  @doc """
  Acquire a lock on a connection and run a series of requests inside a
  transaction. The result of the transaction fun is return inside an `:ok`
  tuple: `{:ok result}`.

  To use the locked connection call the request with the connection
  reference passed as the single argument to the `fun`. If the
  connection disconnects all future calls using that connection
  reference will fail.

  `rollback/2` rolls back the transaction and causes the function to
  return `{:error, reason}`.

  `transaction/3` can be nested multiple times if the connection
  reference is used to start a nested transaction. The top level
  transaction function is the actual transaction.

  ## Options

    * `:pool_timeout` - Time to wait in the queue for the connection
    (default: `#{@pool_timeout}`)
    * `:queue` - Whether to wait for connection in a queue (default: `true`);
    * `:timeout` - Transaction timeout (default: `#{@timeout}`);
    * `:pool` - The pool module to use, must match that set on
    `start_link/1`, see `DBConnection`
    * `:proxy` - The proxy module for the request, if any, see
    `DBConnection.Proxy` (default: `nil`);

  The `:timeout` is for the duration of the transaction and all nested
  transactions and requests. This timeout overrides timeouts set by internal
  transactions and requests. The `:pool` and `:proxy` will be used
  for all requests inside the transaction function.

  ## Example

      {:ok, res} = Postgrex.Connection.transaction(pid, fn(conn) ->
        Postgrex.Connection.query!(conn, "SELECT title FROM posts", [])
      end)
  """
  @spec transaction(conn, ((DBConnection.t) -> result), Keyword.t) ::
    {:ok, result} | {:error, any} when result: var
  def transaction(conn, fun, opts \\ []) do
    DBConnection.transaction(conn, fun, defaults(opts))
  end

  @doc """
  Rollback a transaction, does not return.

  Aborts the current transaction fun. If inside multiple `transaction/3`
  functions, bubbles up to the top level.

  ## Example

      {:error, :oops} = Postgrex.Connection.transaction(pid, fn(conn) ->
        DBConnection.rollback(conn, :bar)
        IO.puts "never reaches here!"
      end)
  """
  @spec rollback(DBConnection.t, any) :: no_return()
  defdelegate rollback(conn, any), to: DBConnection

  @doc """
  Returns a cached map of connection parameters.

  ## Options

    * `:pool_timeout` - Call timeout (default: `#{@pool_timeout}`)
    * `:pool` - The pool module to use, must match that set on
    `start_link/1`, see `DBConnection`

  """
  @spec parameters(conn, Keyword.t) :: %{binary => binary}
  def parameters(conn, opts \\ []) do
    DBConnection.execute!(conn, %Postgrex.Parameters{}, nil, defaults(opts))
  end

  ## Helpers

  defp defaults(opts) do
    Keyword.put_new(opts, :timeout, @timeout)
  end
end
