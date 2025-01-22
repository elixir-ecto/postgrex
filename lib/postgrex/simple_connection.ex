defmodule Postgrex.SimpleConnection do
  @moduledoc ~S"""
  A generic connection suitable for simple queries and pubsub functionality.

  On its own, a SimpleConnection server only maintains a connection. To execute
  queries, process results, or relay notices you must implement a callback module
  with the SimpleConnection behaviour.

  ## Example

  The SimpleConnection behaviour abstracts common client/server interactions,
  along with optional mechanisms for running queries or relaying notifications.

  Let's start with a minimal callback module that executes a query and relays
  the result back to the caller.

      defmodule MyConnection do
        @behaviour Postgrex.SimpleConnection

        @impl true
        def init(_args) do
          {:ok, %{from: nil}}
        end

        @impl true
        def handle_call({:query, query}, from, state) do
          {:query, query, %{state | from: from}}
        end

        @impl true
        def handle_result(results, state) when is_list(results) do
          SimpleConnection.reply(state.from, results)

          {:noreply, state}
        end

        @impl true
        def handle_result(%Postgrex.Error{} = error, state) do
          SimpleConnection.reply(state.from, error)

          {:noreply, state}
        end
      end

      # Start the connection
      {:ok, pid} = SimpleConnection.start_link(MyConnection, [], database: "demo")

      # Execute a literal query
      SimpleConnection.call(pid, {:query, "SELECT 1"})
      # => %Postgrex.Result{rows: [["1"]]}

  We start a connection by passing the callback module, callback options, and
  server options to `SimpleConnection.start_link/3`. The `init/1` function
  receives any callback options and returns the callback state.

  Queries are sent through `SimpleConnection.call/2`, executed on the server,
  and the result is handed off to `handle_result/2`. At that point the callback
  can process the result before replying back to the caller with
  `SimpleConnection.reply/2`.

  ## Building a PubSub Connection

  With the `notify/3` callback you can also build a pubsub server on top of
  `LISTEN/NOTIFY`. Here's a naive pubsub implementation:

      defmodule MyPubSub do
        @behaviour Postgrex.SimpleConnection

        defstruct [:from, listeners: %{}]

        @impl true
        def init(args) do
          {:ok, struct!(__MODULE__, args)}
        end

        @impl true
        def notify(channel, payload, state) do
          for pid <- state.listeners[channel] do
            send(pid, {:notice, channel, payload})
          end
        end

        @impl true
        def handle_call({:listen, channel}, {pid, _} = from, state) do
          listeners = Map.update(state.listeners, channel, [pid], &[pid | &1])

          {:query, ~s(LISTEN "#{channel}"), %{state | from: from, listeners: listeners}}
        end

        def handle_call({:query, query}, from, state) do
          {:query, query, %{state | from: from}}
        end

        @impl true
        def handle_result(_results, state) do
          SimpleConnection.reply(state.from, :ok)

          {:noreply, %{state | from: nil}}
        end
      end

      # Start the connection
      {:ok, pid} = SimpleConnection.start_link(MyPubSub, [], database: "demo")

      # Start listening to the "demo" channel
      SimpleConnection.call(pid, {:listen, "demo"})
      # => %Postgrex.Result{command: :listen}

      # Notify all listeners
      SimpleConnection.call(pid, {:query, ~s(NOTIFY "demo", 'hello')})
      # => %Postgrex.Result{command: :notify}

      # Check the inbox to see the notice message
      flush()
      # => {:notice, "demo", "hello"}

  See `Postgrex.Notifications` for a more complex implementation that can
  unlisten, handle process exits, and persist across reconnection.

  ## Name registration

  A `Postgrex.SimpleConnection` is bound to the same name registration rules as a
  `GenServer`. Read more about them in the `GenServer` docs.
  """

  @behaviour :gen_statem

  require Logger

  alias Postgrex.Protocol

  @doc false
  defstruct idle_interval: 5000,
            protocol: nil,
            auto_reconnect: false,
            reconnect_backoff: 500,
            state: nil

  ## PUBLIC API ##

  @type query :: iodata
  @type state :: term

  @typedoc since: "0.17.0"
  @type from :: {pid, term}

  @doc """
  Callback for process initialization.

  This is called once and before the Postgrex connection is established.
  """
  @callback init(term) :: {:ok, state}

  @doc """
  Callback for processing or relaying pubsub notifications.
  """
  @callback notify(binary, binary, state) :: :ok

  @doc """
  Invoked after connecting or reconnecting.

  This may be called multiple times if `:auto_reconnect` is true.
  """
  @callback handle_connect(state) :: {:noreply, state} | {:query, query, state}

  @doc """
  Invoked after disconnection.

  This is invoked regardless of the `:auto_reconnect` option.
  """
  @callback handle_disconnect(state) :: {:noreply, state}

  @doc """
  Callback for `SimpleConnection.call/3`.

  Replies must be sent with `SimpleConnection.reply/2`.
  """
  @callback handle_call(term, from, state) ::
              {:noreply, state} | {:query, query, state}

  @doc """
  Callback for `Kernel.send/2`.
  """
  @callback handle_info(term, state) :: {:noreply, state} | {:query, query, state}

  @doc """
  Callback for processing or relaying queries executed via `{:query, query, state}`.

  Either a list of successful query results or an error will be passed to this callback.
  A list is passed because the simple query protocol allows multiple commands to be
  issued in a single query.
  """
  @callback handle_result([Postgrex.Result.t()] | Postgrex.Error.t(), state) ::
              {:noreply, state}

  @optional_callbacks handle_call: 3,
                      handle_connect: 1,
                      handle_disconnect: 1,
                      handle_info: 2,
                      handle_result: 2

  @doc """
  Replies to the given client.

  Wrapper for `:gen_statem.reply/2`.
  """
  def reply({caller_pid, from} = _from, reply) when is_pid(caller_pid) do
    :gen_statem.reply(from, reply)
  end

  @doc """
  Calls the given server.

  Wrapper for `:gen_statem.call/3`.
  """
  def call(server, message, timeout \\ 5000) do
    with {__MODULE__, reason} <- :gen_statem.call(server, {message, self()}, timeout) do
      exit({reason, {__MODULE__, :call, [server, message, timeout]}})
    end
  end

  @doc false
  def child_spec(opts) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, opts}}
  end

  @doc """
  Start the connection process and connect to Postgres.

  The options that this function accepts are the same as those accepted by
  `Postgrex.start_link/1`, as well as the extra options `:sync_connect`,
  `:auto_reconnect`, `:reconnect_backoff`, and `:configure`.

  ## Options

    * `:auto_reconnect` - automatically attempt to reconnect to the database
      in event of a disconnection. Defaults to `false`, which means the process
      terminates. See the note in `Postgrex.Notifications` about [async connect
      and auto-reconnects][async-caveat].

    * `:configure` - A function to run before every connect attempt to dynamically
      configure the options as a `{module, function, args}`, where the current
      options will prepended to `args`. Defaults to `nil`.

    * `:idle_interval` - while also accepted on `Postgrex.start_link/1`, it has
      a default of `5000ms` in `Postgrex.SimpleConnection` (instead of 1000ms).

    * `:reconnect_backoff` - time (in ms) between reconnection attempts when
      `auto_reconnect` is enabled. Defaults to `500`.

    * `:sync_connect` - controls if the connection should be established on boot
      or asynchronously right after boot. Defaults to `true`.

  [async-caveat]: Postgrex.Notifications.html#module-async-connect-auto-reconnects-and-missed-notifications
  """
  @spec start_link(module, term, Keyword.t()) :: {:ok, pid} | {:error, Postgrex.Error.t() | term}
  def start_link(module, args, opts) do
    {name, opts} = Keyword.pop(opts, :name)

    opts = Keyword.put_new(opts, :sync_connect, true)
    connection_opts = Postgrex.Utils.default_opts(opts)
    start_args = {module, args, connection_opts}

    case name do
      nil ->
        :gen_statem.start_link(__MODULE__, start_args, [])

      atom when is_atom(atom) ->
        :gen_statem.start_link({:local, atom}, __MODULE__, start_args, [])

      {:global, _term} = tuple ->
        :gen_statem.start_link(tuple, __MODULE__, start_args, [])

      {:via, via_module, _term} = tuple when is_atom(via_module) ->
        :gen_statem.start_link(tuple, __MODULE__, start_args, [])

      other ->
        raise ArgumentError, """
        expected :name option to be one of the following:

          * nil
          * atom
          * {:global, term}
          * {:via, module, term}

        Got: #{inspect(other)}
        """
    end
  end

  ## CALLBACKS ##

  @state :no_state

  @doc false
  @impl :gen_statem
  def callback_mode, do: :handle_event_function

  @doc false
  @impl :gen_statem
  def init({mod, args, opts}) do
    case mod.init(args) do
      {:ok, mod_state} ->
        idle_timeout = opts[:idle_timeout]

        if idle_timeout do
          Logger.warning(
            ":idle_timeout in Postgrex.SimpleConnection is deprecated, " <>
              "please use :idle_interval instead"
          )
        end

        {idle_interval, opts} = Keyword.pop(opts, :idle_interval, idle_timeout || 5000)
        {auto_reconnect, opts} = Keyword.pop(opts, :auto_reconnect, false)
        {reconnect_backoff, opts} = Keyword.pop(opts, :reconnect_backoff, 500)

        state = %__MODULE__{
          idle_interval: idle_interval,
          auto_reconnect: auto_reconnect,
          reconnect_backoff: reconnect_backoff,
          state: {mod, mod_state}
        }

        put_opts(mod, opts)

        if opts[:sync_connect] do
          case handle_event(:internal, {:connect, :init}, @state, state) do
            {:keep_state, state} -> {:ok, @state, state}
            {:keep_state, state, actions} -> {:ok, @state, state, actions}
            {:stop, reason, _state} -> {:stop, reason}
          end
        else
          {:ok, @state, state, {:next_event, :internal, {:connect, :init}}}
        end
    end
  end

  @doc false
  @impl :gen_statem
  def handle_event(type, content, statem_state, state)

  def handle_event(:internal, {:connect, :reconnect}, @state, %{protocol: protocol} = state)
      when protocol != nil do
    Protocol.disconnect(:reconnect, protocol)
    {:keep_state, %{state | protocol: nil}, {:next_event, :internal, {:connect, :init}}}
  end

  def handle_event(:internal, {:connect, _}, @state, %{state: {mod, mod_state}} = state) do
    opts =
      case Keyword.get(opts(mod), :configure) do
        {module, fun, args} -> apply(module, fun, [opts(mod) | args])
        fun when is_function(fun, 1) -> fun.(opts(mod))
        nil -> opts(mod)
      end

    case Protocol.connect(opts) do
      {:ok, protocol} ->
        state = %{state | protocol: protocol}

        with {:keep_state, state, _} <- maybe_handle(mod, :handle_connect, [mod_state], state) do
          {:keep_state, state}
        end

      {:error, reason} ->
        Logger.error(
          "#{inspect(pid_or_name())} (#{inspect(mod)}) failed to connect to Postgres: #{Exception.format(:error, reason)}"
        )

        if state.auto_reconnect do
          {:keep_state, state, {{:timeout, :backoff}, state.reconnect_backoff, nil}}
        else
          {:stop, reason, state}
        end
    end
  end

  def handle_event({:timeout, :backoff}, nil, @state, state) do
    {:keep_state, state, {:next_event, :internal, {:connect, :reconnect}}}
  end

  def handle_event({:call, from}, {msg, caller_pid}, @state, %{state: {mod, mod_state}} = state) do
    # We have to do a hack here to carry the actual caller PID over to the handle_call/3
    # callback, because gen_statem uses a proxy process to do calls with timeout != :infinity.
    # This results in the caller PID not being the same as the PID in the "from" tuple,
    # so things like Postgrex.Notifications cannot use that "from"'s PID to register
    # notification handlers. This approach is paired with reconstructing the proper
    # "from" tuple in the reply/2 function in this module.
    callback_from = {caller_pid, from}
    handle(mod, :handle_call, [msg, callback_from, mod_state], from, state)
  end

  def handle_event(:timeout, nil, @state, %{protocol: protocol} = state) do
    case Protocol.ping(protocol) do
      {:ok, protocol} ->
        {:keep_state, %{state | protocol: protocol}, {:timeout, state.idle_interval, nil}}

      {error, reason, protocol} ->
        reconnect_or_stop(error, reason, protocol, state)
    end
  end

  def handle_event(:info, msg, @state, %{protocol: protocol, state: {mod, mod_state}} = state) do
    opts = [notify: &mod.notify(&1, &2, mod_state)]

    case Protocol.handle_info(msg, opts, protocol) do
      {:ok, protocol} ->
        {:keep_state, %{state | protocol: protocol}, {:timeout, state.idle_interval, nil}}

      {:unknown, protocol} ->
        maybe_handle(mod, :handle_info, [msg, mod_state], %{state | protocol: protocol})

      {error, reason, protocol} ->
        reconnect_or_stop(error, reason, protocol, state)
    end
  end

  def handle_event(:info, msg, @state, %{state: {mod, mod_state}} = state) do
    maybe_handle(mod, :handle_info, [msg, mod_state], state)
  end

  @doc false
  @impl :gen_statem
  def format_status(_opts, [_pdict, @state, state]) do
    state
  end

  ## Helpers

  defp maybe_handle(mod, fun, args, state) do
    if function_exported?(mod, fun, length(args)) do
      handle(mod, fun, args, nil, state)
    else
      {:keep_state, state, {:timeout, state.idle_interval, nil}}
    end
  end

  defp handle(mod, fun, args, from, state) do
    case apply(mod, fun, args) do
      {:noreply, mod_state} ->
        {:keep_state, %{state | state: {mod, mod_state}}, {:timeout, state.idle_interval, nil}}

      {:query, query, mod_state} ->
        opts = [notify: &mod.notify(&1, &2, mod_state)]

        state = %{state | state: {mod, mod_state}}

        with {:ok, results, protocol} <- Protocol.handle_simple(query, opts, state.protocol),
             {:ok, protocol} <- Protocol.checkin(protocol) do
          state = %{state | protocol: protocol}

          handle(mod, :handle_result, [results, mod_state], from, state)
        else
          {:error, %Postgrex.Error{} = error, protocol} ->
            handle(mod, :handle_result, [error, mod_state], from, %{state | protocol: protocol})

          {:disconnect, reason, protocol} ->
            reconnect_or_stop(:disconnect, reason, protocol, state)
        end
    end
  end

  defp reconnect_or_stop(error, reason, protocol, %{state: {mod, mod_state}} = state)
       when error in [:error, :disconnect] do
    {:keep_state, state, _actions} = maybe_handle(mod, :handle_disconnect, [mod_state], state)

    if state.auto_reconnect do
      {:keep_state, state, {:next_event, :internal, {:connect, :reconnect}}}
    else
      {:stop, reason, %{state | protocol: protocol}}
    end
  end

  defp pid_or_name do
    case Process.info(self(), :registered_name) do
      {:registered_name, atom} when is_atom(atom) -> atom
      _ -> self()
    end
  end

  defp opts(mod), do: Process.get(mod)

  defp put_opts(mod, opts), do: Process.put(mod, opts)
end
