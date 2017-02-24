defmodule Postgrex do
  @moduledoc """
  PostgreSQL driver for Elixir.

  This module handles the connection to Postgres, providing support
  for queries, transactions, connection backoff, logging, pooling and
  more.

  Note that the notifications API (pub/sub) supported by Postgres is
  handled by `Postgrex.Notifications`. Hence, to use this feature,
  you need to start a separate (notifications) connection.
  """

  alias Postgrex.Query

  @typedoc """
  A connection process name, pid or reference.

  A connection reference is used when making multiple requests to the same
  connection, see `transaction/3`.
  """
  @type conn :: DBConnection.conn

  @pool_timeout 5000
  @timeout 15_000
  @max_rows 500

  ### PUBLIC API ###

  @doc """
  Start the connection process and connect to postgres.

  ## Options

    * `:hostname` - Server hostname (default: PGHOST env variable, then localhost);
    * `:port` - Server port (default: PGPORT env variable, then 5432);
    * `:database` - Database (default: PGDATABASE env variable; otherwise required);
    * `:username` - Username (default: PGUSER env variable, then USER env var);
    * `:password` - User password (default: PGPASSWORD env variable);
    * `:parameters` - Keyword list of connection parameters;
    * `:timeout` - Socket receive timeout when idle in milliseconds (default:
    `#{@timeout}`);
    * `:connect_timeout` - Socket connect timeout in milliseconds (defaults to
    `:timeout` value);
    * `:handshake_timeout` - Connection handshake timeout in milliseconds
    (defaults to `:timeout` value);
    * `:ssl` - Set to `true` if ssl should be used (default: `false`);
    * `:ssl_opts` - A list of ssl options, see ssl docs;
    * `:socket_options` - Options to be given to the underlying socket;
    * `:prepare` - How to prepare queries, either `:named` to use named queries
    or `:unnamed` to force unnamed queries (default: `:named`);
    * `:transactions` - Set to `:strict` to error on unexpected transaction
    state, otherwise set to `naive` (default: `:naive`);
    * `:pool` - The pool module to use, see `DBConnection` for pool dependent
    options, this option must be included with all requests contacting the pool
    if not `DBConnection.Connection` (default: `DBConnection.Connection`);
    * `:types` - The types module to use, see `Postgrex.TypeModule`, this
    option is only required when using custom encoding or decoding (default:
    `Postgrex.DefaultTypes`);

  `Postgrex` uses the `DBConnection` framework and supports all `DBConnection`
  options like `:idle`, `:after_connect` etc.
  See `DBConnection.start_link/2` for more information.

  ## Examples

      iex> {:ok, pid} = Postgrex.start_link(database: "postgres")
      {:ok, #PID<0.69.0>}

  Run a query after connection has been established:

      iex> {:ok, pid} = Postgrex.start_link(after_connect: &Postgrex.query!(&1, "SET TIME ZONE 'UTC';", []))
      {:ok, #PID<0.69.0>}

  ## PgBouncer

  When using PgBouncer with transaction or statement pooling named prepared
  queries can not be used because the bouncer may route requests from
  the same postgrex connection to different PostgreSQL backend processes
  and discards named queries after the transactions closes.
  To force unnamed prepared queries set the `:prepare` option to `:unnamed`.

  """
  @spec start_link(Keyword.t) :: {:ok, pid} | {:error, Postgrex.Error.t | term}
  def start_link(opts) do
    opts = Postgrex.Utils.default_opts(opts)
    DBConnection.start_link(Postgrex.Protocol, opts)
  end

  @doc """
  Runs an (extended) query and returns the result as `{:ok, %Postgrex.Result{}}`
  or `{:error, %Postgrex.Error{}}` if there was a database error. Parameters can
  be set in the query as `$1` embedded in the query string. Parameters are given
  as a list of elixir values. See the README for information on how Postgrex
  encodes and decodes Elixir values by default. See `Postgrex.Result` for the
  result data.

  This function may still raise an exception if there is an issue with types
  (`ArgumentError`), connection (`DBConnection.ConnectionError`), ownership
  (`DBConnection.OwnershipError`) or other error (`RuntimeError`).

  ## Options

    * `:pool_timeout` - Time to wait in the queue for the connection
    (default: `#{@pool_timeout}`)
    * `:queue` - Whether to wait for connection in a queue (default: `true`);
    * `:timeout` - Query request timeout (default: `#{@timeout}`);
    * `:decode_mapper` - Fun to map each row in the result to a term after
    decoding, (default: `fn x -> x end`);
    * `:pool` - The pool module to use, must match that set on
    `start_link/1`, see `DBConnection`
    * `:mode` - set to `:savepoint` to use a savepoint to rollback to before the
    query on error, otherwise set to `:transaction` (default: `:transaction`);

  ## Examples

      Postgrex.query(conn, "CREATE TABLE posts (id serial, title text)", [])

      Postgrex.query(conn, "INSERT INTO posts (title) VALUES ('my title')", [])

      Postgrex.query(conn, "SELECT title FROM posts", [])

      Postgrex.query(conn, "SELECT id FROM posts WHERE title like $1", ["%my%"])

      Postgrex.query(conn, "COPY posts TO STDOUT", [])
  """
  @spec query(conn, iodata, list, Keyword.t) :: {:ok, Postgrex.Result.t} | {:error, Postgrex.Error.t}
  def query(conn, statement, params, opts \\ []) do
    query = %Query{name: "", statement: statement}
    opts =
      opts
      |> defaults()
      |> Keyword.put(:function, :prepare_execute)
    case DBConnection.prepare_execute(conn, query, params, opts) do
      {:ok, _, result} ->
        {:ok, result}
      {:error, %Postgrex.Error{}} = error ->
        error
      {:error, err} ->
        raise err
    end
  end

  @doc """
  Runs an (extended) query and returns the result or raises `Postgrex.Error` if
  there was an error. See `query/3`.
  """
  @spec query!(conn, iodata, list, Keyword.t) :: Postgrex.Result.t
  def query!(conn, statement, params, opts \\ []) do
    query = %Query{name: "", statement: statement}
    opts =
      opts
      |> defaults()
      |> Keyword.put(:function, :prepare_execute)
    {_, result} = DBConnection.prepare_execute!(conn, query, params, opts)
    result
  end

  @doc """
  Prepares an (extended) query and returns the result as
  `{:ok, %Postgrex.Query{}}` or `{:error, %Postgrex.Error{}}` if there was an
  error. Parameters can be set in the query as `$1` embedded in the query
  string. To execute the query call `execute/4`. To close the prepared query
  call `close/3`. See `Postgrex.Query` for the query data.

  This function may still raise an exception if there is an issue with types
  (`ArgumentError`), connection (`DBConnection.ConnectionError`), ownership
  (`DBConnection.OwnershipError`) or other error (`RuntimeError`).

  ## Options

    * `:pool_timeout` - Time to wait in the queue for the connection
    (default: `#{@pool_timeout}`)
    * `:queue` - Whether to wait for connection in a queue (default: `true`);
    * `:timeout` - Prepare request timeout (default: `#{@timeout}`);
    * `:pool` - The pool module to use, must match that set on
    `start_link/1`, see `DBConnection`
    * `:mode` - set to `:savepoint` to use a savepoint to rollback to before the
    prepare on error, otherwise set to `:transaction` (default: `:transaction`);

  ## Examples

      Postgrex.prepare(conn, "", "CREATE TABLE posts (id serial, title text)")
  """
  @spec prepare(conn, iodata, iodata, Keyword.t) :: {:ok, Postgrex.Query.t} | {:error, Postgrex.Error.t}
  def prepare(conn, name, statement, opts \\ []) do
    query = %Query{name: name, statement: statement}
    opts =
      opts
      |> defaults()
      |> Keyword.put(:function, :prepare)
    case DBConnection.prepare(conn, query, opts) do
      {:ok, _} = ok ->
        ok
      {:error, %Postgrex.Error{}} = error ->
        error
      {:error, err} ->
        raise err
    end
  end

  @doc """
  Prepares an (extended) query and returns the prepared query or raises
  `Postgrex.Error` if there was an error. See `prepare/4`.
  """
  @spec prepare!(conn, iodata, iodata, Keyword.t) :: Postgrex.Query.t
  def prepare!(conn, name, statement, opts \\ []) do
    opts =
      opts
      |> defaults()
      |> Keyword.put(:function, :prepare)
    DBConnection.prepare!(conn, %Query{name: name, statement: statement}, opts)
  end

  @doc """
  Runs an (extended) prepared query and returns the result as
  `{:ok, %Postgrex.Result{}}` or `{:error, %Postgrex.Error{}}` if there was an
  error. Parameters are given as part of the prepared query, `%Postgrex.Query{}`.
  See the README for information on how Postgrex encodes and decodes Elixir
  values by default. See `Postgrex.Query` for the query data and
  `Postgrex.Result` for the result data.

  This function may still raise an exception if there is an issue with types
  (`ArgumentError`), connection (`DBConnection.ConnectionError`), ownership
  (`DBConnection.OwnershipError`) or other error (`RuntimeError`).

  ## Options

    * `:pool_timeout` - Time to wait in the queue for the connection
    (default: `#{@pool_timeout}`)
    * `:queue` - Whether to wait for connection in a queue (default: `true`);
    * `:timeout` - Execute request timeout (default: `#{@timeout}`);
    * `:decode_mapper` - Fun to map each row in the result to a term after
    decoding, (default: `fn x -> x end`);
    * `:pool` - The pool module to use, must match that set on
    `start_link/1`, see `DBConnection`
    * `:mode` - set to `:savepoint` to use a savepoint to rollback to before the
    execute on error, otherwise set to `:transaction` (default: `:transaction`);

  ## Examples

      query = Postgrex.prepare!(conn, "", "CREATE TABLE posts (id serial, title text)")
      Postgrex.execute(conn, query, [])

      query = Postgrex.prepare!(conn, "", "SELECT id FROM posts WHERE title like $1")
      Postgrex.execute(conn, query, ["%my%"])
  """
  @spec execute(conn, Postgrex.Query.t, list, Keyword.t) ::
    {:ok, Postgrex.Result.t} | {:error, Postgrex.Error.t}
  def execute(conn, query, params, opts \\ []) do
    case DBConnection.execute(conn, query, params, defaults(opts)) do
      {:ok, _} = ok ->
        ok
      {:error, %Postgrex.Error{}} = error ->
        error
      {:error, err} ->
        raise err
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

  This function may still raise an exception if there is an issue with types
  (`ArgumentError`), connection (`DBConnection.ConnectionError`), ownership
  (`DBConnection.OwnershipError`) or other error (`RuntimeError`).

  ## Options

    * `:pool_timeout` - Time to wait in the queue for the connection
    (default: `#{@pool_timeout}`)
    * `:queue` - Whether to wait for connection in a queue (default: `true`);
    * `:timeout` - Close request timeout (default: `#{@timeout}`);
    * `:pool` - The pool module to use, must match that set on
    `start_link/1`, see `DBConnection`
    * `:mode` - set to `:savepoint` to use a savepoint to rollback to before the
    close on error, otherwise set to `:transaction` (default: `:transaction`);

  ## Examples

      query = Postgrex.prepare!(conn, "", "CREATE TABLE posts (id serial, title text)")
      Postgrex.close(conn, query)
  """
  @spec close(conn, Postgrex.Query.t, Keyword.t) :: :ok | {:error, Postgrex.Error.t}
  def close(conn, query, opts \\ []) do
    case DBConnection.close(conn, query, defaults(opts)) do
      {:ok, _} ->
        :ok
      {:error, %Postgrex.Error{}} = error ->
        error
      {:error, err} ->
        raise err
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
  tuple: `{:ok, result}`.

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
    `start_link/1`, see `DBConnection`;
    * `:mode` - Set to `:savepoint` to use savepoints instead of an SQL
    transaction, otherwise set to `:transaction` (default: `:transaction`);


  The `:timeout` is for the duration of the transaction and all nested
  transactions and requests. This timeout overrides timeouts set by internal
  transactions and requests. The `:pool` and `:mode` will be used for all
  requests inside the transaction function.

  ## Example

      {:ok, res} = Postgrex.transaction(pid, fn(conn) ->
        Postgrex.query!(conn, "SELECT title FROM posts", [])
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

      {:error, :oops} = Postgrex.transaction(pid, fn(conn) ->
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

  @doc """
  Returns a supervisor child specification for a DBConnection pool.
  """
  @spec child_spec(Keyword.t) :: Supervisor.Spec.spec
  def child_spec(opts) do
    opts = Postgrex.Utils.default_opts(opts)
    DBConnection.child_spec(Postgrex.Protocol, opts)
  end

  @doc """
  Returns a stream for a query on a connection.

  Stream consumes memory in chunks of at most `max_rows` rows (see Options).
  This is useful for processing _large_ datasets.

  A stream must be wrapped in a transaction and may be used as an `Enumerable`
  or a `Collectable`.

  When used as an `Enumerable` with a `COPY .. TO STDOUT` SQL query no other
  queries or streams can be interspersed until the copy has finished. Otherwise
  it is possible to intersperse enumerable streams and queries.

  When used as a `Collectable` the values are passed as copy data with the
  query. No other queries or streams can be interspersed until the copy has
  finished. If the query is not copying to the database the copy data will still
  be sent but is silently discarded.

  ### Options

    * `:max_rows` - Maximum numbers of rows in a result (default to `#{@max_rows}`)
    * `:decode_mapper` - Fun to map each row in the result to a term after
    decoding, (default: `fn x -> x end`);
    * `:mode` - set to `:savepoint` to use a savepoint to rollback to before an
    execute on error, otherwise set to `:transaction` (default: `:transaction`);

  ## Examples

      Postgrex.transaction(pid, fn(conn) ->
        query = Postgrex.prepare!(conn, "", "COPY posts TO STDOUT")
        stream = Postgrex.stream(conn, query, [])
        result_to_iodata = fn(%Postgrex.Result{rows: rows}) -> rows end
        Enum.into(stream, File.stream!("posts"), result_to_iodata)
      end)

      Postgrex.transaction(pid, fn(conn) ->
        stream = Postgrex.stream(conn, "COPY posts FROM STDIN", [])
        Enum.into(File.stream!("posts"), stream)
      end)
  """
  @spec stream(DBConnection.t, iodata | Postgrex.Query.t, list, Keyword.t) ::
    Postgrex.Stream.t
  def stream(%DBConnection{} = conn, query, params, options \\ [])  do
    options =
      options
      |> defaults()
      |> Keyword.put_new(:max_rows, @max_rows)
    %Postgrex.Stream{conn: conn, query: query, params: params, options: options}
  end

  ## Helpers
  defp defaults(opts) do
    Keyword.put_new(opts, :timeout, @timeout)
  end
end
