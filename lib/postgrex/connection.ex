defmodule Postgrex.Connection do
  @moduledoc """
  Main API for Postgrex. This module handles the connection to postgres.

  Note that the notifications API (pub/sub) supported by Postgres is handled by
  `Postgrex.Notifications.Connection` and not by this module. Hence, to use this
  feature, you need to start a separate (notifications) connection.
  """

  use Connection
  alias Postgrex.Protocol
  require Logger

  @timeout 5000

  defstruct [parameters: nil, protocol: nil, queue: :queue.new(), client: nil,
             timer: nil]

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
    Connection.start_link(__MODULE__, Postgrex.Utils.default_opts(opts))
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
    run(pid, &Protocol.query(&1, statement, params, &2), opts)
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
    run(pid, &Protocol.prepare(&1, name, statement, &2), opts)
  end

  @doc """
  Prepared an (extended) query and returns the prepared query or raises
  `Postgrex.Error` if there was an error. See `prepare/4`.
  """
  @spec prepare!(pid, iodata, iodata, Keyword.t) :: Postgrex.Query.t
  def prepare!(pid, name, statement, opts \\ []) do
    case prepare(pid, name, statement, opts) do
      {:ok, query}  -> query
      {:error, err} -> raise err
    end
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
  def execute(pid, query, opts \\ []) do
    query = Postgrex.Query.encode(query)
    run(pid, &Protocol.execute(&1, query, &2), opts)
  end

  @doc """
  Runs an (extended) prepared query and returns the result or raises
  `Postgrex.Error` if there was an error. See `execute/3`.
  """
  @spec execute!(pid, Postgrex.Query.t, Keyword.t) :: Postgrex.Result.t
  def execute!(pid, query, opts \\ []) do
    case execute(pid, query, opts) do
      {:ok, res}    -> res
      {:error, err} -> raise err
    end
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
    run(pid, &Protocol.close(&1, query, &2), opts)
  end

  @doc """
  Closes an (extended) prepared query and returns `:ok` or raises
  `Postgrex.Error` if there was an error. See `close/3`.
  """
  @spec close!(pid, Postgrex.Query.t, Keyword.t) :: :ok
  def close!(pid, query, opts \\ []) do
    case close(pid, query, opts) do
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
    case Protocol.connect(opts) do
      {:ok, protocol, parameters, _} ->
        {:ok, %__MODULE__{protocol: protocol, parameters: parameters}}
      {:error, reason} ->
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

  def handle_call({:checkout, ref, timeout}, {pid, _} = from, %{client: nil} = s) do
    monitor = Process.monitor(pid)
    handle_next({:checkout, timeout}, from, %{s | client: {ref, monitor}})
  end

  def handle_call({:checkout, ref, timeout}, {pid, _} = from, %{queue: queue} = s) do
    client = {ref, Process.monitor(pid)}
    {:noreply, %{s | queue: :queue.in({{:checkout, timeout}, client, from}, queue)}}
  end

  def handle_call({:listen, channel}, from, %{client: nil} = s) do
    handle_next({:listen, channel}, from, s)
  end
  def handle_call({:unlisten, ref}, from, %{client: nil} = s) do
    handle_next({:unlisten, ref}, from, s)
  end

  def handle_call(request, from, %{queue: queue} = s) do
    {:noreply, %{s | queue: :queue.in({request, nil, from}, queue)}}
  end

  @doc false
  def handle_cast({:done, ref, new_parameters, _notifications, buffer}, %{client: {ref, _}} = s) do
    %{parameters: parameters} = s
    parameters = Map.merge(parameters, new_parameters)
    handle_next(buffer, %__MODULE__{s | parameters: parameters})
  end

  def handle_cast({:update, ref, new_parameters, _notifications}, %{client: {ref, _}} = s) do
    %{parameters: parameters} = s
    parameters = Map.merge(parameters, new_parameters)
    {:noreply, %__MODULE__{s | parameters: parameters}}
  end

  def handle_cast({:stop, ref}, %{client: {ref, _}} = s) do
    {:stop, {:shutdown, :stop}, s}
  end

  def handle_cast({:cancel, ref}, %{client: {ref, _}} = s) do
    {:stop, {:shutdown, :cancel}, s}
  end
  def handle_cast({:cancel, ref}, %{queue: queue} = s) do
    cancel =
      fn({_, {ref2, monitor}, _}) when ref === ref2 ->
          Process.demonitor(monitor, [:flush])
          false
        (_) ->
          true
      end
    {:noreply, %{s | queue: :queue.filter(cancel, queue)}}
  end

  @doc false
  def handle_info({:DOWN, ref, :process, _, _}, %{client: {_, ref}} = s) do
    {:stop, {:shutdown, :DOWN}, s}
  end

  def handle_info({:DOWN, _, :process, _, _} = down, s) do
    filter_queue(down, s)
  end

  def handle_info({:timeout, timer, __MODULE__}, %{timer: timer} = s) when is_reference(timer) do
    {:stop, {:shutdown, :timeout}, s}
  end

  def handle_info(msg, s) do
    protocol_info(msg, s)
  end

  ### PRIVATE FUNCTIONS ###

  defp run(pid, fun, opts) do
    queue_timeout = opts[:queue_timeout] || @timeout
    timeout = opts[:timeout] || @timeout
    ref = make_ref()
    try do
      Connection.call(pid, {:checkout, ref, timeout}, queue_timeout)
    catch
      :exit, {_, {_, :call, [pid | _]}} = reason ->
        Connection.cast(pid, {:cancel, ref})
        exit(reason)
    else
      {:ok, protocol, pid, buffer} ->
        protocol = %{protocol | timeout: timeout}
        result = run(pid, ref, fun, protocol, buffer)
        run_result(result, opts)
      {:error, _} = error ->
        error
    end
  end

  defp run(pid, ref, fun, protocol, buffer) do
    try do
      fun.(protocol, buffer)
    else
      {:ok, result, parameters, notifications, buffer} ->
        Connection.cast(pid, {:done, ref, parameters, notifications, buffer})
        result
      {:ok, parameters, notifications, buffer} ->
        Connection.cast(pid, {:done, ref, parameters, notifications, buffer})
        :ok
      {:error, _} = error ->
        Connection.cast(pid, {:stop, ref})
        error
    catch
      kind, reason ->
        stack = System.stacktrace()
        Connection.cast(pid, {:stop, ref})
        :erlang.raise(kind, reason, stack)
    end
  end

  defp run_result(%Postgrex.Result{} = res, opts) do
    case Keyword.get(opts, :decode, :auto) do
      :auto   -> {:ok, Postgrex.Result.decode(res)}
      :manual -> {:ok, res}
    end
  end
  defp run_result({kind, reason, stack}, _) do
    :erlang.raise(kind, reason, stack)
  end
  defp run_result(%Postgrex.Query{} = query, _), do: {:ok, query}
  defp run_result(%Postgrex.Error{} = err, _), do: {:error, err}
  defp run_result(:ok, _), do: :ok
  defp run_result({:error, _} = err, _), do: err

  defp sync_connect(opts) do
    case connect(:init, opts) do
      {:ok, _} = ok      -> ok
      {:stop, reason, _} -> {:stop, reason}
    end
  end

  defp handle_next(buffer, %{client: client, timer: timer, queue: queue} = s) do
    _ = client && Process.demonitor(elem(client, 1), [:flush])
    cancel_timer(timer)
    {item, queue} = :queue.out(queue)
    s = %{s | client: nil, timer: nil, queue: queue}
    case item do
      {:value, {request, client, from}} ->
        handle_next(request, from, buffer, %{s | client: client})
      :empty ->
        checkin(buffer, s)
    end
  end

  defp handle_next(request, from, buffer \\ :active_once, s)

  defp handle_next({:checkout, timeout}, from, buffer, s) do
    handle_checkout(timeout, from, buffer, s)
  end

  defp checkin(buffer, %{protocol: protocol} = s) do
    case Protocol.checkin(protocol, buffer) do
      :ok              -> {:noreply, s}
      {:error, reason} -> {:stop, reason, s}
    end
  end

  defp handle_checkout(timeout, from, buffer, %{protocol: protocol} = s) do
    case Protocol.checkout(protocol, buffer) do
      {:ok, buffer} ->
        Connection.reply(from, {:ok, protocol, self(), buffer})
        timer = start_timer(timeout)
        {:noreply,  %{s | timer: timer}}
      {:error, reason} ->
        {:stop, reason, s}
    end
  end

  defp filter_queue({:DOWN, ref, :process, _, _} = msg, %{queue: queue} = s) do
    len = :queue.len(queue)

    down =
      fn({_, {_, monitor}, _}) when ref === monitor ->
          false
        (_) ->
          true
      end

    queue = :queue.filter(down, queue)

    case :queue.len(queue) do
      ^len ->
        protocol_info(msg, s)
      _ ->
        {:noreply, %{s | queue: queue}}
    end
  end

  defp protocol_info(info, s) do
    %__MODULE__{protocol: protocol, parameters: parameters} = s
    case Protocol.message(protocol, info) do
      {:ok, new_parameters, _notifications} ->
        parameters = Map.merge(parameters, new_parameters)
        {:noreply, %__MODULE__{s | parameters: parameters}}
      :unknown ->
        Logger.info fn() ->
          [inspect(__MODULE__), ?\s, inspect(self()), " received message: " |
            inspect(info)]
        end
        {:noreply, s}
      {:error, reason} ->
        {:stop, reason, s}
    end
  end

  defp start_timer(:infinity), do: nil
  defp start_timer(timeout) do
    :erlang.start_timer(timeout, self, __MODULE__)
  end

  defp cancel_timer(nil), do: :ok
  defp cancel_timer(timer) do
    case :erlang.cancel_timer(timer) do
      false -> flush_timer(timer)
      _     -> :ok
    end
  end

  defp flush_timer(timer) do
    receive do
      {:timeout, ^timer, __MODULE__} ->
        :ok
    after
      0 ->
        raise ArgumentError, "Timer #{inspect(timer)} does not exist"
    end
  end
end
