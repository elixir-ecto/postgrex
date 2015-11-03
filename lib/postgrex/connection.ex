defmodule Postgrex.Connection do
  @moduledoc """
  Main API for Postgrex. This module handles the connection to postgres.
  """

  use Connection
  alias Postgrex.Protocol

  @timeout 5000

  defstruct [parameters: nil, protocol: nil, listeners: HashDict.new(),
             listener_channels: HashDict.new()]

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
    opts = opts
      |> Keyword.put_new(:username, System.get_env("PGUSER") || System.get_env("USER"))
      |> Keyword.put_new(:password, System.get_env("PGPASSWORD"))
      |> Keyword.put_new(:hostname, System.get_env("PGHOST") || "localhost")
      |> Keyword.put_new(:port, System.get_env("PGPORT"))
      |> Enum.reject(fn {_k,v} -> is_nil(v) end)

    Connection.start_link(__MODULE__, opts)
  end

  @doc """
  Stop the process and disconnect.

  ## Options

    * `:timeout` - Call timeout (default: `#{@timeout}`)
  """
  @spec stop(pid, Keyword.t) :: :ok
  def stop(pid, opts \\ []) do
    Connection.call(pid, :stop, opts[:timeout] || @timeout)
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

  ## Examples

      Postgrex.Connection.query(pid, "CREATE TABLE posts (id serial, title text)", [])

      Postgrex.Connection.query(pid, "INSERT INTO posts (title) VALUES ('my title')", [])

      Postgrex.Connection.query(pid, "SELECT title FROM posts", [])

      Postgrex.Connection.query(pid, "SELECT id FROM posts WHERE title like $1", ["%my%"])

  """
  @spec query(pid, iodata, list, Keyword.t) :: {:ok, Postgrex.Result.t} | {:error, Postgrex.Error.t}
  def query(pid, statement, params, opts \\ []) do
    message = {:query, statement, params}
    timeout = opts[:timeout] || @timeout
    case Connection.call(pid, message, timeout) do
      {:exit, reason} ->
        exit({reason, {__MODULE__, :query, [pid, statement, params, opts]}})
      result ->
        result
    end
  end

  @doc """
  Works like `query/3` and `query/4` but is asynchronous. This returns a `%Task{}`
  and is useful for starting multiple async queries and then waiting on the result
  later on.

  This function expects a pid as first argument for Elixir 1.0. In Elixir 1.1,
  a pid or a name could be passed as first argument.

  When server name passed has no process associated, it will raise an error.

  ## Examples

      iex> task = Postgrex.Connection.async_query(pid, "SELECT title FROM posts", [])
      iex> Task.await(task)
      {:ok, %Postgrex.Result{...}}

  ## Return values

  When awaited on, the task will return one of:

    * `{:ok, %Postgrex.Result{}}` - the query was successful
    * `{:error, %Postgrex.Error{}}` - the query failed on Postgres side
    * `{:exit, term}` - there was an error when processing the query or its results in the connection

  """
  @spec async_query(pid, iodata, list) :: Task.t
  def async_query(pid, statement, params) do
    message = {:query, statement, params}

    # TODO: Remove branches when we no longer support Elixir v1.0
    process =
      cond do
        is_pid(pid) ->
          pid
        function_exported?(GenServer, :whereis, 1) ->
          GenServer.whereis(pid) || raise ArgumentError, "no process is associated with #{inspect pid}"
        true ->
          raise ArgumentError, "requires Elixir 1.1 when passing server name as first argument"
      end

    monitor = Process.monitor(process)
    from = {self(), monitor}

    :ok  = Connection.cast(pid, {message, from})
    task = %Task{ref: monitor}

    # TODO: Remove branches when we no longer support Elixir v1.1
    if Map.has_key?(task, :owner) do
      %{task | owner: self()}
    else
      task
    end
  end

  @doc """
  Runs an (extended) query and returns the result or raises `Postgrex.Error` if
  there was an error. See `query/3`.
  """
  @spec query!(pid, iodata, list, Keyword.t) :: Postgrex.Result.t
  def query!(pid, statement, params, opts \\ []) do
    case query(pid, statement, params, opts) do
      {:ok, res}    -> res
      {:error, err} -> raise err
    end
  end

  @doc """
  Listens to an asynchronous notification channel using the `LISTEN` command.
  A message `{:notification, connection_pid, ref, channel, payload}` will be
  sent to the calling process when a notification is received.

  ## Options

    * `:timeout` - Call timeout (default: `#{@timeout}`)
  """
  @spec listen(pid, String.t, Keyword.t) :: {:ok, reference} | {:error, Postgrex.Error.t}
  def listen(pid, channel, opts \\ []) do
    message = {:listen, channel, self()}
    timeout = opts[:timeout] || @timeout
    case Connection.call(pid, message, timeout) do
      {:exit, reason} ->
        exit({reason, {__MODULE__, :listen, [pid, channel, opts]}})
      result ->
        result
    end
  end

  @doc """
  Listens to an asynchronous notification channel `channel`. See `listen/2`.
  """
  @spec listen!(pid, String.t, Keyword.t) :: reference
  def listen!(pid, channel, opts \\ []) do
    case listen(pid, channel, opts) do
      {:ok, ref}    -> ref
      {:error, err} -> raise err
    end
  end

  @doc """
  Stops listening on the given channel by passing the reference returned from
  `listen/2`.

  ## Options

    * `:timeout` - Call timeout (default: `#{@timeout}`)
  """
  @spec unlisten(pid, reference, Keyword.t) :: :ok | {:error, Postgrex.Error.t}
  def unlisten(pid, ref, opts \\ []) do
    message = {:unlisten, ref}
    timeout = opts[:timeout] || @timeout
    case Connection.call(pid, message, timeout) do
      {:exit, reason} ->
        exit({reason, {__MODULE__, :unlisten, [pid, ref, opts]}})
      {:error, %ArgumentError{} = err} -> raise err
      result                           -> result
    end
  end

  @doc """
  Stops listening on the given channel by passing the reference returned from
  `listen/2`.
  """
  @spec unlisten!(pid, reference, Keyword.t) :: :ok
  def unlisten!(pid, ref, opts \\ []) do
    case unlisten(pid, ref, opts) do
      :ok           -> :ok
      {:error, err} -> raise err
    end
  end

  @doc """
  Returns a cached map of connection parameters.

  ## Options

    * `:timeout` - Call timeout (default: `#{@timeout}`)
  """
  @spec parameters(pid, Keyword.t) :: map
  def parameters(pid, opts \\ []) do
    Connection.call(pid, :parameters, opts[:timeout] || @timeout)
  end

  ### CONNECTION CALLBACKS ###

  @doc false
  def init(opts) do
    if opts[:sync_connect] do
      sync_connect(opts)
    else
      {:connect, :init, opts}
    end
  end

  @doc false
  def connect(_, opts) do
    case Protocol.init(opts) do
      {:ok, protocol, parameters, _} ->
        {:ok, %__MODULE__{protocol: protocol, parameters: parameters}}
      {:stop, reason} ->
        {:stop, reason, opts}
    end
  end

  @doc false
  def format_status(:terminate, [_pdict, opts]) when is_list(opts) do
    Keyword.put(opts, :password, :REDACTED)
  end
  def format_status(opt, [_pdict, s]) do
    s = %{s | types: :types_removed}
    if opt == :normal do
      [data: [{'State', s}]]
    else
      s
    end
  end

  @doc false
  def handle_call(:stop, _, s) do
    {:stop, :normal, :ok, s}
  end

  def handle_call(:parameters, _from, %{parameters: params} = s) do
    {:reply, params, s}
  end

  def handle_call({:query, statement, params}, from, s) do
    handle_query(statement, params, &reply(from, &1), s)
  end

  def handle_call({:listen, channel, pid}, from, s) do
    handle_listen(channel, pid, from, s)
  end

  def handle_call({:unlisten, ref}, from, s) do
    handle_unlisten(ref, from, s)
  end

  def handle_cast({{:query, statement, params}, from}, s) do
    handle_query(statement, params, &reply(from, &1), s)
  end

  @doc false
  def handle_info({:DOWN, ref, :process, _, _} = down, s) do
    case HashDict.fetch(s.listeners, ref) do
      {:ok, {channel, _pid}} ->
        s = update_in(s.listener_channels[channel], &HashSet.delete(&1, ref))
        s = update_in(s.listeners, &HashDict.delete(&1, ref))

        if HashSet.size(s.listener_channels[channel]) == 0 do
          s = update_in(s.listener_channels, &HashDict.delete(&1, channel))
          handle_query("UNLISTEN #{channel}", [], fn(_) -> :ok end, s)
        else
          {:noreply, s}
        end
        :error ->
          protocol_info(down, s)
    end
  end

  def handle_info(msg, s) do
    protocol_info(msg, s)
  end

  ### PRIVATE FUNCTIONS ###

  defp sync_connect(opts) do
    case connect(:init, opts) do
      {:ok, _} = ok      -> ok
      {:stop, reason, _} -> {:stop, reason}
    end
  end

  defp handle_query(statement, params, reply, s) do
    %__MODULE__{protocol: protocol, parameters: parameters} = s
    case Protocol.handle_query(statement, params, protocol) do
      {:ok, result, protocol, new_parameters, notifications} ->
        reply.(result)
        notify_listeners(notifications, s)
        parameters = Map.merge(parameters, new_parameters)
        {:noreply, %__MODULE__{s | protocol: protocol, parameters: parameters}}
      {:stop, reason, protocol} ->
        reply.(reason)
        {:stop, reason, %__MODULE__{s | protocol: protocol}}
    end
  end

  defp reply(from, reply) do
    Connection.reply(from, query_response(reply))
  end

  defp query_response(%Postgrex.Result{} = res), do: {:ok, res}
  defp query_response(%Postgrex.Error{} = err),  do: {:error, err}
  defp query_response({:exit, _} = exit),        do: exit

  defp notify_listeners(notifications, s) do
    %__MODULE__{listener_channels: channels, listeners: listeners} = s
    _ = for {channel, payload} <- notifications do
      _ = for ref <- HashDict.get(channels, channel) || [] do
        {_, pid} = HashDict.fetch!(listeners, ref)
        send(pid, {:notification, self(), ref, channel, payload})
        :ok
      end
    end
    :ok
  end

  defp handle_listen(channel, pid, from, s) do
    ref = Process.monitor(pid)
    s = update_in(s.listeners, &HashDict.put(&1, ref, {channel, pid}))
    s = update_in(s.listener_channels[channel], fn set ->
      (set || HashSet.new) |> HashSet.put(ref)
    end)

    if HashSet.size(s.listener_channels[channel]) == 1 do
      listener_query("LISTEN #{channel}", {:ok, ref}, from, s)
    else
      {:reply, {:ok, ref}, s}
    end
  end

  defp handle_unlisten(ref, from, s) do
    case HashDict.fetch(s.listeners, ref) do
      {:ok, {channel, _pid}} ->
        s = update_in(s.listener_channels[channel], &HashSet.delete(&1, ref))
        s = update_in(s.listeners, &HashDict.delete(&1, ref))

        if HashSet.size(s.listener_channels[channel]) == 0 do
          s = update_in(s.listener_channels, &HashDict.delete(&1, channel))
          listener_query("UNLISTEN #{channel}", :ok, from, s)
        else
          {:reply, :ok, s}
        end
      :error ->
        {:reply, {:error, %ArgumentError{}}, s}
    end
  end

  defp listener_query(statement, result, from, s) do
    reply = fn(%Postgrex.Result{}) ->
        Connection.reply(from, result)
      (other) ->
        reply(from, other)
    end
    handle_query(statement, [], reply, s)
  end

  defp protocol_info(info, s) do
    %__MODULE__{protocol: protocol, parameters: parameters} = s
    case Protocol.handle_info(info, protocol) do
      {:ok, protocol, new_parameters, notifications} ->
        notify_listeners(notifications, s)
        parameters = Map.merge(parameters, new_parameters)
        {:noreply, %__MODULE__{s | protocol: protocol, parameters: parameters}}
      {:stop, reason, protocol} ->
        {:stop, reason, %__MODULE__{s | protocol: protocol}}
    end
  end
end
