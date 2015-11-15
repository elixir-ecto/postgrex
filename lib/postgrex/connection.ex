defmodule Postgrex.Connection do
  @moduledoc """
  Main API for Postgrex. This module handles the connection to postgres.
  """

  use Connection
  alias Postgrex.Protocol
  require Logger

  @timeout 5000

  defstruct [parameters: nil, protocol: nil, queue: :queue.new(), client: nil,
             timer: nil, listeners: HashDict.new(),
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
    queue_timeout = opts[:queue_timeout] || @timeout
    timeout = opts[:timeout] || @timeout
    ref = make_ref()
    try do
      Connection.call(pid, {:query, ref, timeout}, queue_timeout)
    catch
      :exit, {_, {_, :call, [pid | _]}} = reason ->
        Connection.cast(pid, {:cancel, ref})
        exit(reason)
    else
      {:ok, protocol, pid, buffer} ->
        protocol = %{protocol | timeout: timeout}
        case query(pid, ref, protocol, statement, params, buffer) do
          %Postgrex.Result{} = res ->
            {:ok, decode(res, opts)}
          %Postgrex.Error{} = err ->
            {:error, err}
          {:error, _} = error ->
             error
          {kind, reason, stack} ->
            :erlang.raise(kind, reason, stack)
        end
      {:error, _} = error ->
        error
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
  @spec listen(pid, String.t, Keyword.t) :: {:ok, reference}
  def listen(pid, channel, opts \\ []) do
    message = {:listen, channel}
    timeout = opts[:timeout] || @timeout
    Connection.call(pid, message, timeout)
  end

  @doc """
  Listens to an asynchronous notification channel `channel`. See `listen/2`.
  """
  @spec listen!(pid, String.t, Keyword.t) :: reference
  def listen!(pid, channel, opts \\ []) do
    {:ok, ref} = listen(pid, channel, opts)
    ref
  end

  @doc """
  Stops listening on the given channel by passing the reference returned from
  `listen/2`.

  ## Options

    * `:timeout` - Call timeout (default: `#{@timeout}`)
  """
  @spec unlisten(pid, reference, Keyword.t) :: :ok
  def unlisten(pid, ref, opts \\ []) do
    message = {:unlisten, ref}
    timeout = opts[:timeout] || @timeout
    case Connection.call(pid, message, timeout) do
      :ok                              -> :ok
      {:error, %ArgumentError{} = err} -> raise err
    end
  end

  @doc """
  Stops listening on the given channel by passing the reference returned from
  `listen/2`.
  """
  @spec unlisten!(pid, reference, Keyword.t) :: :ok
  def unlisten!(pid, ref, opts \\ []) do
    unlisten(pid, ref, opts)
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

  def handle_call({:query, ref, timeout}, {pid, _} = from, %{client: nil} = s) do
    monitor = Process.monitor(pid)
    handle_next({:query, timeout}, from, %{s | client: {ref, monitor}})
  end

  def handle_call({:query, ref, timeout}, {pid, _} = from, %{queue: queue} = s) do
    client = {ref, Process.monitor(pid)}
    {:noreply, %{s | queue: :queue.in({{:query, timeout}, client, from}, queue)}}
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
  def handle_cast({:done, ref, new_parameters, notifications, buffer}, %{client: {ref, _}} = s) do
    %{parameters: parameters} = s
    notify_listeners(notifications, s)
    parameters = Map.merge(parameters, new_parameters)
    await_next(buffer, %__MODULE__{s | parameters: parameters})
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
  def handle_info({:DOWN, ref, :process, _, _} = down, s) do
    case HashDict.fetch(s.listeners, ref) do
      {:ok, {channel, _pid}} ->
        s = update_in(s.listener_channels[channel], &HashSet.delete(&1, ref))
        s = update_in(s.listeners, &HashDict.delete(&1, ref))

        if HashSet.size(s.listener_channels[channel]) == 0 do
          s = update_in(s.listener_channels, &HashDict.delete(&1, channel))
          down_unlisten(channel, s)
        else
          {:noreply, s}
        end
      :error ->
        filter_queue(down, s)
    end
  end

  def handle_info({:timeout, timer, __MODULE__}, %{timer: timer} = s) when is_reference(timer) do
    {:stop, {:shutdown, :timeout}, s}
  end

  def handle_info(msg, s) do
    protocol_info(msg, s)
  end

  ### PRIVATE FUNCTIONS ###

  defp query(pid, ref, protocol, statement, params, buffer) do
    try do
      Protocol.query(protocol, statement, params, buffer)
    else
      {:ok, result, parameters, notifications, buffer} ->
        Connection.cast(pid, {:done, ref, parameters, notifications, buffer})
        result
      {:error, _} = error ->
        Connection.cast(pid, {:stop, ref})
        error
    catch
      kind, reason ->
        stack = System.stacktrace()
        Connection.cast(pid, {:stop, ref})
        {kind, reason, stack}
    end
  end

  defp decode(res, opts) do
    case Keyword.get(opts, :decode, :auto) do
      :auto   -> Postgrex.Result.decode(res)
      :manual -> res
    end
  end

  defp sync_connect(opts) do
    case connect(:init, opts) do
      {:ok, _} = ok      -> ok
      {:stop, reason, _} -> {:stop, reason}
    end
  end

  defp await_next(buffer, %{client: client, timer: timer, queue: queue} = s) do
    _ = client && Process.demonitor(elem(client, 1), [:flush])
    cancel_timer(timer)
    {item, queue} = :queue.out(queue)
    s = %{s | client: nil, timer: nil, queue: queue}
    case item do
      {:value, {request, client, from}} ->
        await_next(request, from, buffer, %{s | client: client})
      :empty ->
        checkin(buffer, s)
    end
  end

  defp await_next(request, from, buffer, s) do
    %{protocol: protocol, parameters: parameters} = s
    case Protocol.await(protocol, buffer) do
      {:ok, new_parameters, notifications, buffer} ->
        notify_listeners(notifications, s)
        parameters = Map.merge(parameters, new_parameters)
        s = %__MODULE__{s | parameters: parameters}
        handle_next(request, from, buffer, s)
      {:error, reason} ->
        {:stop, reason, s}
    end
  end

  defp checkin(buffer, %{protocol: protocol, parameters: parameters} = s) do
    case Protocol.checkin(protocol, buffer) do
      {:ok, new_parameters, notifications} ->
        notify_listeners(notifications, s)
        parameters = Map.merge(parameters, new_parameters)
        {:noreply, %__MODULE__{s | parameters: parameters}}
      {:error, reason} ->
        {:stop, reason, s}
    end
  end

  defp handle_next(request, from, buffer \\ :active_once, s)

  defp handle_next({:query, timeout}, from, buffer, s) do
    handle_query(timeout, from, buffer, s)
  end
  defp handle_next({:listen, channel}, from, buffer, s) do
    handle_listen(channel, from, buffer, s)
  end
  defp handle_next({:unlisten, ref}, from, buffer, s) do
    handle_unlisten(ref, from, buffer, s)
  end
  defp handle_next({:down_unlisten, channel}, nil, buffer, s) do
    handle_down_unlisten(channel, buffer, s)
  end

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

  defp handle_query(timeout, from, buffer, %{protocol: protocol} = s) do
    case Protocol.checkout(protocol, buffer) do
      {:ok, buffer} ->
        Connection.reply(from, {:ok, protocol, self(), buffer})
        timer = start_timer(timeout)
        {:noreply,  %{s | timer: timer}}
      {:error, reason} ->
        {:stop, reason, s}
    end
  end

  defp handle_listen(channel, {pid, _} = from, buffer, s) do
    ref = Process.monitor(pid)
    s = update_in(s.listeners, &HashDict.put(&1, ref, {channel, pid}))
    s = update_in(s.listener_channels[channel], fn set ->
      (set || HashSet.new) |> HashSet.put(ref)
    end)

    if HashSet.size(s.listener_channels[channel]) == 1 do
      listener_query("LISTEN #{channel}", {:ok, ref}, from, buffer, s)
    else
      Connection.reply(from, {:ok, ref})
      await_next(buffer, s)
    end
  end

  defp handle_unlisten(ref, from, buffer, s) do
    case HashDict.fetch(s.listeners, ref) do
      {:ok, {channel, _pid}} ->
        Process.demonitor(ref, [:flush])
        s = update_in(s.listener_channels[channel], &HashSet.delete(&1, ref))
        s = update_in(s.listeners, &HashDict.delete(&1, ref))

        if HashSet.size(s.listener_channels[channel]) == 0 do
          s = update_in(s.listener_channels, &HashDict.delete(&1, channel))
          listener_query("UNLISTEN #{channel}", :ok, from, buffer, s)
        else
          Connection.reply(from, :ok)
          await_next(buffer, s)
        end
      :error ->
        Connection.reply(from, {:error, %ArgumentError{}})
        await_next(buffer, s)
    end
  end

  defp down_unlisten(channel, %{client: nil} = s) do
    listener_query("UNLISTEN #{channel}", :ok, nil, :active_once, s)
  end
  defp down_unlisten(channel, %{queue: queue} = s) do
    request = {:down_unlisten, channel}
    {:noreply, %{s | queue: :queue.in_r({request, nil, nil}, queue)}}
  end

  defp handle_down_unlisten(channel, buffer, s) do
   listener_query("UNLISTEN #{channel}", nil, nil, buffer, s)
  end

  defp listener_query(statement, result, from, buffer, s) do
    %{protocol: protocol, parameters: parameters} = s
    case Protocol.query(protocol, statement, [], buffer) do
      {:ok, %Postgrex.Result{}, new_parameters, notifications, buffer} ->
        _ = from && Connection.reply(from, result)
        notify_listeners(notifications, s)
        parameters = Map.merge(parameters, new_parameters)
        await_next(buffer, %__MODULE__{s | parameters: parameters})
      {:error, error} ->
        Connection.reply(from, error)
        {:stop, error, s}
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
      {:ok, new_parameters, notifications} ->
        notify_listeners(notifications, s)
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
