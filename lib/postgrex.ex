defmodule Postgrex do
  @moduledoc """
  PostgreSQL driver for Elixir.

  Postgrex is a partial implementation of the Postgres [frontend/backend
  message protocol](https://www.postgresql.org/docs/current/protocol.html).
  It performs wire messaging in Elixir, as opposed to binding to a library
  such as `libpq` in C.

  A Postgrex query is performed as "[extended query](https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY)".
  An "extended query"  involves separate server-side parse, bind, and execute
  stages, each of which may be re-used for efficiency. For example, libraries
  like Ecto caches queries, so a query only has to be parsed and planned once.
  This is all done via wire messaging, without relying on `PREPARE q AS (...)`
  and `EXECUTE q()` SQL statements directly.

  This module handles the connection to PostgreSQL, providing support
  for queries, transactions, connection backoff, logging, pooling and
  more.

  Note that the notifications API (pub/sub) supported by PostgreSQL is
  handled by `Postgrex.Notifications`. Hence, to use this feature,
  you need to start a separate (notifications) connection.
  """

  alias Postgrex.Query

  @typedoc """
  A connection process name, pid or reference.

  A connection reference is used when making multiple requests to the same
  connection, see `transaction/3`.
  """
  @type conn :: DBConnection.conn()

  @type start_option ::
          {:hostname, String.t()}
          | {:endpoints, [tuple()]}
          | {:socket_dir, Path.t()}
          | {:socket, Path.t()}
          | {:port, :inet.port_number()}
          | {:database, String.t()}
          | {:username, String.t()}
          | {:password, String.t()}
          | {:parameters, keyword}
          | {:timeout, timeout}
          | {:connect_timeout, timeout}
          | {:handshake_timeout, timeout}
          | {:ping_timeout, timeout}
          | {:ssl, boolean | [:ssl.tls_client_option()]}
          | {:socket_options, [:gen_tcp.connect_option()]}
          | {:prepare, :named | :unnamed}
          | {:transactions, :strict | :naive}
          | {:types, module}
          | {:search_path, [String.t()]}
          | {:disconnect_on_error_codes, [atom]}
          | DBConnection.start_option()

  @type option ::
          {:mode, :transaction | :savepoint}
          | DBConnection.option()

  @type execute_option ::
          {:decode_mapper, (list -> term)}
          | option

  @max_rows 500
  @timeout 15_000

  ### PUBLIC API ###

  @doc """
  Start the connection process and connect to postgres.

  ## Options

  Postgrex provides multiple ways to connect to the server, listed in order of
  precedence below:

    * `:hostname` - Server hostname (default: PGHOST env variable, then localhost);
    * `:port` - Server port (default: PGPORT env variable, then 5432);
    * `:endpoints` - A list of endpoints (host and port pairs, with an optional
      extra_opts keyword list);
      Postgrex will try each endpoint in order, one by one, until the connection succeeds;
      The syntax is `[{host1, port1},{host2, port2},{host3, port3}]` or
      `[{host1, port1, extra_opt1: value},{host2, port2, extra_opt2: value}}]`;
      This option takes precedence over `:hostname+:port`;
    * `:socket_dir` - Connect to PostgreSQL via UNIX sockets in the given directory;
      The socket name is derived based on the port. This is the preferred method
      for configuring sockets and it takes precedence over the hostname. If you are
      connecting to a socket outside of the PostgreSQL convention, use `:socket` instead;
    * `:socket` - Connect to PostgreSQL via UNIX sockets in the given path.
      This option takes precedence over the `:hostname`, `:endpoints` and `:socket_dir`;

  Once a server is specified, you can configure the connection with the following:

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

    * `:ping_timeout` - Socket receive timeout when idle in milliseconds (defaults to
      `:timeout` value);

    * `:idle_interval` - Ping connections after a period of inactivity in milliseconds.
      Defaults to 1000ms;

    * `:ssl` - Enables SSL. Setting it to `true` enables SSL without server certificate verification,
      which emits a warning. Instead, prefer to set it to a keyword list, with either
      `:cacerts` or `:cacertfile` set to a CA trust store, to enable server certificate
      verification. Defaults to `false`;

    * `:socket_options` - Options to be given to the underlying socket
      (applies to both TCP and UNIX sockets);

    * `:target_server_type` - Allows opening connections to a server in the given
      replica mode. The allowed values are `:any`, `:primary` and `:secondary`
      (default: `:any`). If this option is used together with `endpoints`, we will
      traverse all endpoints until we find an endpoint matching the server type;

    * `:disconnect_on_error_codes` - List of error code atoms that when encountered
      will disconnect the connection. This is useful when using Postgrex against systems that
      support failover, which when it occurs will emit certain error codes
      e.g. `:read_only_sql_transaction` (default: `[]`);

    * `:show_sensitive_data_on_connection_error` - By default, `Postgrex`
      hides all information during connection errors to avoid leaking credentials
      or other sensitive information. You can set this option if you wish to
      see complete errors and stacktraces during connection errors;

  The following options controls the pool and other Postgrex features:

    * `:prepare` - How to prepare queries, either `:named` to use named queries
    or `:unnamed` to force unnamed queries (default: `:named`);

    * `:transactions` - Set to `:strict` to error on unexpected transaction
      state, otherwise set to `:naive` (default: `:strict`);

    * `:pool` - The pool module to use, defaults to `DBConnection.ConnectionPool`.
      See the pool documentation for more options. The default `:pool_size` for
      the default pool is 1. If you set a different pool, this option must be
      included with all requests contacting the pool;

    * `:types` - The types module to use, see `Postgrex.Types.define/3`, this
      option is only required when using custom encoding or decoding (default:
      `Postgrex.DefaultTypes`);

    * `:search_path` - A list of strings used to set the search path for the connection.
      This is useful when, for instance, an extension like `citext` is installed in a
      separate schema. If that schema is not in the connection's search path, Postgrex
      might not be able to recognize the extension's data type. When this option is `nil`,
      the search path is not modified. (default: `nil`).
      See the [PostgreSQL docs](https://www.postgresql.org/docs/current/ddl-schemas.html#DDL-SCHEMAS-PATH)
      for more details.

    * `:disable_composite_types` - Set to `true` to disable composite types support.
      This is useful when using Postgrex against systems that do not support composite types
      (default: `false`).

  `Postgrex` uses the `DBConnection` library and supports all `DBConnection`
  options like `:idle`, `:after_connect` etc. See `DBConnection.start_link/2`
  for more information.

  ## Examples

      iex> {:ok, pid} = Postgrex.start_link(database: "postgres")
      {:ok, #PID<0.69.0>}

  Run a query after connection has been established:

      iex> {:ok, pid} = Postgrex.start_link(after_connect: &Postgrex.query!(&1, "SET TIME ZONE 'UTC';", []))
      {:ok, #PID<0.69.0>}

  Connect to postgres instance through a unix domain socket

      iex> {:ok, pid} = Postgrex.start_link(socket_dir: "/tmp", database: "postgres")
      {:ok, #PID<0.69.0>}

  ## SSL client authentication

  When connecting to Postgres or CockroachDB instances over SSL it is idiomatic to use
  certificate authentication:

      [ssl: [cacertfile: System.get_env("DB_CA_CERT_FILE")]]

  The server name indication (SNI) will be automatically set based on the `:hostname`
  configuration, if one was provided. Other options, such as `depth: 3`, may be necessary
  depending on the server.

  ## PgBouncer

  When using PgBouncer with transaction or statement pooling named prepared
  queries can not be used because the bouncer may route requests from
  the same postgrex connection to different PostgreSQL backend processes
  and discards named queries after the transactions closes.
  To force unnamed prepared queries set the `:prepare` option to `:unnamed`.

  ## Handling failover

  Some services, such as AWS Aurora, support failovers. The 2 options
  `endpoints` and `target_server_type` can be used together to achieve a faster fail-over.

  Imagine an AWS Aurora cluster named "test" with 2 instances. Use the
  following options minimize downtime by ensuring that Postgrex connects to
  the new primary instance as soon as possible.

      {:ok, pid} = Postgrex.start_link(
        endpoints: [
          {"test.cluster-xyz.eu-west-1.rds.amazonaws.com", 5432},
          {"test.cluster-ro-xyz.eu-west-1.rds.amazonaws.com", 5432}
        ],
        target_server_type: :primary,
        (...)
      )

  In the event of a fail-over, Postgrex gets first disconnected from what used
  to be the primary instance. The primary instance will then reboot and turn into
  a secondary instance. Meanwhile, one of the secondary instances will have turned
  into the new primary instance. However, the DNS entry of the primary endpoint
  provided by AWS can take some time to get updated. That is why it can be faster to
  let Postgrex iterate over all the instances of the cluster to find the new
  primary instance instead of waiting for the DNS update.

  If the cluster does not have DNS-backed primary and secondary endpoints (like the
  ones provided by AWS Aurora) or if the cluster is made of more than 2 instances,
  the hostname (and port) of all of the individual instances can be specified
  in the `endpoints` list:

      endpoints: [
        {"test-instance-1.xyz.eu-west-1.rds.amazonaws.com", 5432},
        {"test-instance-2.xyz.eu-west-1.rds.amazonaws.com", 5432},
        (...),
        {"test-instance-N.xyz.eu-west-1.rds.amazonaws.com", 5432}
      ]

  """
  @spec start_link([start_option]) :: {:ok, pid} | {:error, Postgrex.Error.t() | term}
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

    * `:queue` - Whether to wait for connection in a queue (default: `true`);
    * `:timeout` - Query request timeout (default: `#{@timeout}`);
    * `:decode_mapper` - Fun to map each row in the result to a term after
    decoding, (default: `fn x -> x end`);
    * `:mode` - set to `:savepoint` to use a savepoint to rollback to before the
    query on error, otherwise set to `:transaction` (default: `:transaction`);
    * `:cache_statement` - Caches the query with the given name

  ## Examples

      Postgrex.query(conn, "CREATE TABLE posts (id serial, title text)", [])

      Postgrex.query(conn, "INSERT INTO posts (title) VALUES ('my title')", [])

      Postgrex.query(conn, "SELECT title FROM posts", [])

      Postgrex.query(conn, "SELECT id FROM posts WHERE title like $1", ["%my%"])

      Postgrex.query(conn, "COPY posts TO STDOUT", [])
  """
  @spec query(conn, iodata, list, [execute_option]) ::
          {:ok, Postgrex.Result.t()} | {:error, Exception.t()}
  def query(conn, statement, params, opts \\ []) do
    if name = Keyword.get(opts, :cache_statement) do
      query = %Query{name: name, cache: :statement, statement: IO.iodata_to_binary(statement)}

      case DBConnection.prepare_execute(conn, query, params, opts) do
        {:ok, _, result} ->
          {:ok, result}

        {:error, %Postgrex.Error{postgres: %{code: :feature_not_supported}}} = error ->
          with %DBConnection{} <- conn,
               :error <- DBConnection.status(conn) do
            error
          else
            _ -> query_prepare_execute(conn, query, params, opts)
          end

        {:error, _} = error ->
          error
      end
    else
      query_prepare_execute(conn, %Query{name: "", statement: statement}, params, opts)
    end
  end

  defp query_prepare_execute(conn, query, params, opts) do
    case DBConnection.prepare_execute(conn, query, params, opts) do
      {:ok, _, result} -> {:ok, result}
      {:error, _} = error -> error
    end
  end

  @doc """
  Runs an (extended) query and returns the result or raises `Postgrex.Error` if
  there was an error. See `query/3`.
  """
  @spec query!(conn, iodata, list, [execute_option]) :: Postgrex.Result.t()
  def query!(conn, statement, params, opts \\ []) do
    case query(conn, statement, params, opts) do
      {:ok, result} -> result
      {:error, err} -> raise err
    end
  end

  @doc """
  Prepares an (extended) query.

  It returns the result as `{:ok, %Postgrex.Query{}}` or `{:error, %Postgrex.Error{}}`
  if there was an error. Parameters can be set in the query as `$1` embedded in the
  query string. To execute the query call `execute/4`. To close the prepared query
  call `close/3`. See `Postgrex.Query` for the query data.

  This function may still raise an exception if there is an issue with types
  (`ArgumentError`), connection (`DBConnection.ConnectionError`), ownership
  (`DBConnection.OwnershipError`) or other error (`RuntimeError`).

  For unnamed prepared statements, pass an empty string for `name`. This can be useful
  when trying to avoid the generic query plan Postgres creates for named prepared
  statements. You may also set `prepare: :unnamed` at the connection level so that
  every prepared statement using that connection will be unnamed.

  ## Options

    * `:queue` - Whether to wait for connection in a queue (default: `true`);
    * `:timeout` - Prepare request timeout (default: `#{@timeout}`);
    * `:mode` - set to `:savepoint` to use a savepoint to rollback to before the
    prepare on error, otherwise set to `:transaction` (default: `:transaction`);

  ## Examples

      Postgrex.prepare(conn, "", "CREATE TABLE posts (id serial, title text)")
  """
  @spec prepare(conn, iodata, iodata, [option]) ::
          {:ok, Postgrex.Query.t()} | {:error, Exception.t()}
  def prepare(conn, name, statement, opts \\ []) do
    query = %Query{name: name, statement: statement}
    opts = Keyword.put(opts, :postgrex_prepare, true)
    DBConnection.prepare(conn, query, opts)
  end

  @doc """
  Prepares an (extended) query and returns the prepared query or raises
  `Postgrex.Error` if there was an error. See `prepare/4`.
  """
  @spec prepare!(conn, iodata, iodata, [option]) :: Postgrex.Query.t()
  def prepare!(conn, name, statement, opts \\ []) do
    opts = Keyword.put(opts, :postgrex_prepare, true)
    DBConnection.prepare!(conn, %Query{name: name, statement: statement}, opts)
  end

  @doc """
  Prepares and executes a query in a single step.

  It returns the result as `{:ok, %Postgrex.Query{}, %Postgrex.Result{}}` or
  `{:error, %Postgrex.Error{}}` if there was an error. Parameters are given as
  part of the prepared query, `%Postgrex.Query{}`.

  For unnamed prepared statements, pass an empty string for `name`. This can be useful
  when trying to avoid the generic query plan Postgres creates for named prepared
  statements. You may also set `prepare: :unnamed` at the connection level so that
  every prepared statement using that connection will be unnamed.

  See the README for information on how Postgrex encodes and decodes Elixir
  values by default. See `Postgrex.Query` for the query data and
  `Postgrex.Result` for the result data.

  ## Options

    * `:queue` - Whether to wait for connection in a queue (default: `true`);
    * `:timeout` - Execute request timeout (default: `#{@timeout}`);
    * `:decode_mapper` - Fun to map each row in the result to a term after
    decoding, (default: `fn x -> x end`);
    * `:mode` - set to `:savepoint` to use a savepoint to rollback to before the
    execute on error, otherwise set to `:transaction` (default: `:transaction`);

  ## Examples

      Postgrex.prepare_execute(conn, "", "SELECT id FROM posts WHERE title like $1", ["%my%"])

  """
  @spec prepare_execute(conn, iodata, iodata, list, [execute_option]) ::
          {:ok, Postgrex.Query.t(), Postgrex.Result.t()} | {:error, Postgrex.Error.t()}
  def prepare_execute(conn, name, statement, params, opts \\ []) do
    query = %Query{name: name, statement: statement}
    DBConnection.prepare_execute(conn, query, params, opts)
  end

  @doc """
  Prepares and runs a query and returns the result or raises
  `Postgrex.Error` if there was an error. See `prepare_execute/5`.
  """
  @spec prepare_execute!(conn, iodata, iodata, list, [execute_option]) ::
          {Postgrex.Query.t(), Postgrex.Result.t()}
  def prepare_execute!(conn, name, statement, params, opts \\ []) do
    query = %Query{name: name, statement: statement}
    DBConnection.prepare_execute!(conn, query, params, opts)
  end

  @doc """
  Runs an (extended) prepared query.

  It returns the result as `{:ok, %Postgrex.Query{}, %Postgrex.Result{}}` or
  `{:error, %Postgrex.Error{}}` if there was an error. Parameters are given as
  part of the prepared query, `%Postgrex.Query{}`.

  See the README for information on how Postgrex encodes and decodes Elixir
  values by default. See `Postgrex.Query` for the query data and
  `Postgrex.Result` for the result data.

  ## Options

    * `:queue` - Whether to wait for connection in a queue (default: `true`);
    * `:timeout` - Execute request timeout (default: `#{@timeout}`);
    * `:decode_mapper` - Fun to map each row in the result to a term after
    decoding, (default: `fn x -> x end`);
    * `:mode` - set to `:savepoint` to use a savepoint to rollback to before the
    execute on error, otherwise set to `:transaction` (default: `:transaction`);

  ## Examples

      query = Postgrex.prepare!(conn, "", "CREATE TABLE posts (id serial, title text)")
      Postgrex.execute(conn, query, [])

      query = Postgrex.prepare!(conn, "", "SELECT id FROM posts WHERE title like $1")
      Postgrex.execute(conn, query, ["%my%"])
  """
  @spec execute(conn, Postgrex.Query.t(), list, [execute_option]) ::
          {:ok, Postgrex.Query.t(), Postgrex.Result.t()} | {:error, Postgrex.Error.t()}
  def execute(conn, query, params, opts \\ []) do
    DBConnection.execute(conn, query, params, opts)
  end

  @doc """
  Runs an (extended) prepared query and returns the result or raises
  `Postgrex.Error` if there was an error. See `execute/4`.
  """
  @spec execute!(conn, Postgrex.Query.t(), list, [execute_option]) ::
          Postgrex.Result.t()
  def execute!(conn, query, params, opts \\ []) do
    DBConnection.execute!(conn, query, params, opts)
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

    * `:queue` - Whether to wait for connection in a queue (default: `true`);
    * `:timeout` - Close request timeout (default: `#{@timeout}`);
    * `:mode` - set to `:savepoint` to use a savepoint to rollback to before the
    close on error, otherwise set to `:transaction` (default: `:transaction`);

  ## Examples

      query = Postgrex.prepare!(conn, "", "CREATE TABLE posts (id serial, title text)")
      Postgrex.close(conn, query)
  """
  @spec close(conn, Postgrex.Query.t(), [option]) :: :ok | {:error, Exception.t()}
  def close(conn, query, opts \\ []) do
    with {:ok, _} <- DBConnection.close(conn, query, opts) do
      :ok
    end
  end

  @doc """
  Closes an (extended) prepared query and returns `:ok` or raises
  `Postgrex.Error` if there was an error. See `close/3`.
  """
  @spec close!(conn, Postgrex.Query.t(), [option]) :: :ok
  def close!(conn, query, opts \\ []) do
    DBConnection.close!(conn, query, opts)
    :ok
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

    * `:queue` - Whether to wait for connection in a queue (default: `true`);
    * `:timeout` - Transaction timeout (default: `#{@timeout}`);
    * `:mode` - Set to `:savepoint` to use savepoints instead of an SQL
    transaction, otherwise set to `:transaction` (default: `:transaction`);
    * `:commit_comment` - When a binary string is provided, prepends the text as
    a comment attached to the `COMMIT` statement issued to close the transaction (default: `nil`);

  The `:timeout` is for the duration of the transaction and all nested
  transactions and requests. This timeout overrides timeouts set by internal
  transactions and requests. The `:mode` will be used for all requests inside
  the transaction function. The `:commit_comment` can be helpful in distinguishing
  between transactions in query performance monitoring tools.

  ## Example

      {:ok, res} = Postgrex.transaction(pid, fn(conn) ->
        Postgrex.query!(conn, "SELECT title FROM posts", [])
      end)
  """
  @spec transaction(conn, (DBConnection.t() -> result), [option]) ::
          {:ok, result} | {:error, any}
        when result: var
  def transaction(conn, fun, opts \\ []) do
    DBConnection.transaction(conn, fun, opts)
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
  @spec rollback(DBConnection.t(), reason :: any) :: no_return()
  defdelegate rollback(conn, reason), to: DBConnection

  @doc """
  Returns a cached map of connection parameters.

  ## Options

    * `:timeout` - Call timeout (default: `#{@timeout}`)

  """
  @spec parameters(conn, [option]) :: %{binary => binary}
        when option: {:timeout, timeout}
  def parameters(conn, opts \\ []) do
    DBConnection.execute!(conn, %Postgrex.Parameters{}, nil, opts)
  end

  @doc """
  Returns a supervisor child specification for a DBConnection pool.
  """
  @spec child_spec([start_option]) :: :supervisor.child_spec()
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
  @spec stream(DBConnection.t(), iodata | Postgrex.Query.t(), list, [option]) ::
          Postgrex.Stream.t()
        when option: execute_option | {:max_rows, pos_integer}
  def stream(%DBConnection{} = conn, query, params, options \\ []) do
    options = Keyword.put_new(options, :max_rows, @max_rows)
    %Postgrex.Stream{conn: conn, query: query, params: params, options: options}
  end
end
