defmodule Postgrex.Notifications do
  @moduledoc ~S"""
  API for notifications (pub/sub) in PostgreSQL.

  In order to use it, first you need to start the notification process.
  In your supervision tree:

      {Postgrex.Notifications, name: MyApp.Notifications}

  Then you can listen to certain channels:

      {:ok, listen_ref} = Postgrex.Notifications.listen(MyApp.Notifications, "channel")

  Now every time a message is broadcast on said channel, for example via
  PostgreSQL command line:

      NOTIFY "channel", "Oh hai!";

  You will receive a message in the format:

      {:notification, notification_pid, listen_ref, channel, message}

  ## Async connect and auto-reconnects

  By default, the notification system establishes a connection to the
  database on initialization, you can configure the connection to happen
  asynchronously. You can also configure the connection to automatically
  reconnect.

  Note however that when the notification system is waiting for a connection,
  any notifications that occur during the disconnection period are not queued
  and cannot be recovered. Similarly, any listen command will be queued until
  the connection is up.

  ## A note on casing

  While PostgreSQL seems to behave as case-insensitive, it actually has a very
  perculiar behaviour on casing. When you write:

      SELECT * FROM POSTS

  PostgreSQL actually converts `POSTS` into the lowercase `posts`. That's why
  both `SELECT * FROM POSTS` and `SELECT * FROM posts` feel equivalent.
  However, if you wrap the table name in quotes, then the casing in quotes
  will be preserved.

  These same rules apply to PostgreSQL notification channels. More importantly,
  whenever `Postgrex.Notifications` listens to a channel, it wraps the channel
  name in quotes. Therefore, if you listen to a channel named "fooBar" and
  you send a notification without quotes in the channel name, such as:

      NOTIFY fooBar, "Oh hai!";

  The notification will not be received by Postgrex.Notifications because the
  notification will be effectively sent to `"foobar"` and not `"fooBar"`. Therefore,
  you must guarantee one of the two following properties:

    1. If you can wrap the channel name in quotes when sending a notification,
       then make sure the channel name has the exact same casing when listening
       and sending notifications

    2. If you cannot wrap the channel name in quotes when sending a notification,
       then make sure to give the lowercased channel name when listening

  """

  use Connection
  require Logger

  alias Postgrex.Protocol

  @timeout 5000

  @doc false
  defstruct idle_interval: 5000,
            protocol: nil,
            auto_reconnect: false,
            reconnect_backoff: 500,
            state: nil

  ## PUBLIC API ##

  @type query :: iodata
  @type server :: GenServer.server()
  @type state :: term

  @doc """
  Callback for process initialization.

  This is called once and before the Postgrex notification connection is established.
  """
  @callback init(term) :: {:ok, state}

  @doc """
  Invoked after connecting.

  This may be called multiple times if `:auto_reconnect` is true.
  """
  @callback handle_connect(state) :: {:noreply, state} | {:query, query, state}

  @doc """
  Invoked after disconnection, regardless of `:auto_reconnect`.
  """
  @callback handle_disconnect(state) :: {:noreply, state}

  @doc """
  Callback for `call/3`.

  Replies must be sent with `reply/2`.
  """
  @callback handle_call(term, GenServer.from(), state) ::
              {:noreply, state}
              | {:query, query, state}

  @doc """
  Callback for `Kernel.send/2`.
  """
  @callback handle_info(term, state) :: {:noreply, state} | {:query, query, state}

  @doc """
  Callback for processing or relaying query results.
  """
  @callback handle_result(Postgrex.Result.t() | Postgrex.Error.t(), state) :: {:noreply, state}

  @doc """
  Callback for processing or relaying pubsub notifications.
  """
  @callback handle_notification(binary, binary, state) :: {:noreply, state}

  @optional_callbacks handle_info: 2,
                      handle_call: 3,
                      handle_result: 2,
                      handle_connect: 1,
                      handle_disconnect: 1

  @doc """
  Replies to the given client.

  Wrapper for `GenServer.reply/2`.
  """
  defdelegate reply(client, reply), to: GenServer

  @doc """
  Calls the given replication server.

  Wrapper for `GenServer.call/3`.
  """
  defdelegate call(server, message, timeout \\ 5000), to: GenServer

  @doc false
  def child_spec(opts) do
    opts =
      case opts do
        [mod, _args, _opts] when is_atom(mod) -> opts
        _ -> [opts]
      end

    %{id: __MODULE__, start: {__MODULE__, :start_link, opts}}
  end

  @doc """
  Start the notification connection process and connect to postgres.

  The options that this function accepts are the same as those accepted by
  `Postgrex.start_link/1`, as well as the extra options `:sync_connect`,
  `:auto_reconnect`, `:reconnect_backoff`, and `:configure`.

  ## Options

    * `:sync_connect` - controls if the connection should be established on boot
      or asynchronously right after boot. Defaults to `true`.

    * `:auto_reconnect` - automatically attempt to reconnect to the database
      in event of a disconnection. See the
      [note about async connect and auto-reconnects](#module-async-connect-and-auto-reconnects)
      above. Defaults to `false`, which means the process terminates.

    * `:reconnect_backoff` - time (in ms) between reconnection attempts when
      `auto_reconnect` is enabled. Defaults to `500`.

    * `:idle_interval` - while also accepted on `Postgrex.start_link/1`, it has
      a default of `5000ms` in `Postgrex.Notifications` (instead of 1000ms).

    * `:configure` - A function to run before every connect attempt to dynamically
      configure the options as a `{module, function, args}`, where the current
      options will prepended to `args`. Defaults to `nil`.
  """
  @spec start_link(module(), term(), Keyword.t()) ::
          {:ok, pid} | {:error, Postgrex.Error.t() | term}
  def start_link(module \\ Postgrex.Notifications.Default, args \\ [], opts) do
    {server_opts, opts} = Keyword.split(opts, [:name])
    opts = Keyword.put_new(opts, :sync_connect, true)
    connection_opts = Postgrex.Utils.default_opts(opts)
    Connection.start_link(__MODULE__, {module, args, connection_opts}, server_opts)
  end

  @doc """
  Listens to an asynchronous notification channel using the `LISTEN` command.

  A message `{:notification, connection_pid, ref, channel, payload}` will be
  sent to the calling process when a notification is received.

  It returns `{:ok, reference}`. It may also return `{:eventually, reference}`
  if the notification process is not currently connected to the database and
  it was started with `:sync_connect` set to false or `:auto_reconnect` set
  to true. The `reference` can be used to issue an `unlisten/3` command.

  ## Options

    * `:timeout` - Call timeout (default: `#{@timeout}`)
  """
  @spec listen(server, String.t(), Keyword.t()) :: {:ok, reference} | {:eventually, reference}
  def listen(pid, channel, opts \\ []) do
    call(pid, {:listen, channel}, Keyword.get(opts, :timeout, @timeout))
  end

  @doc """
  Listens to an asynchronous notification channel `channel`. See `listen/2`.
  """
  @spec listen!(server, String.t(), Keyword.t()) :: reference
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
  @spec unlisten(server, reference, Keyword.t()) :: :ok | :error
  def unlisten(pid, ref, opts \\ []) do
    call(pid, {:unlisten, ref}, Keyword.get(opts, :timeout, @timeout))
  end

  @doc """
  Stops listening on the given channel by passing the reference returned from
  `listen/2`.
  """
  @spec unlisten!(server, reference, Keyword.t()) :: :ok
  def unlisten!(pid, ref, opts \\ []) do
    case unlisten(pid, ref, opts) do
      :ok -> :ok
      :error -> raise ArgumentError, "unknown reference #{inspect(ref)}"
    end
  end

  ## CALLBACKS ##

  @doc false
  def init({mod, args, opts}) do
    case mod.init(args) do
      {:ok, mod_state} ->
        idle_timeout = opts[:idle_timeout]

        if idle_timeout do
          Logger.warn(
            ":idle_timeout in Postgrex.Notifications is deprecated, " <>
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

        put_opts(opts)

        if opts[:sync_connect] do
          case connect(:init, state) do
            {:ok, _} = ok -> ok
            {:backoff, _, _} = backoff -> backoff
            {:stop, reason, _} -> {:stop, reason}
          end
        else
          {:connect, :init, state}
        end
    end
  end

  @doc false
  def connect(_, %{state: {mod, mod_state}} = state) do
    opts =
      case Keyword.get(opts(), :configure) do
        {module, fun, args} -> apply(module, fun, [opts() | args])
        fun when is_function(fun, 1) -> fun.(opts())
        nil -> opts()
      end

    case Protocol.connect(opts) do
      {:ok, protocol} ->
        state = %{state | protocol: protocol}

        with {:noreply, state, _} <- maybe_handle(mod, :handle_connect, [mod_state], state) do
          {:ok, state}
        end

      {:error, reason} ->
        if state.auto_reconnect do
          {:backoff, state.reconnect_backoff, state}
        else
          {:stop, reason, state}
        end
    end
  end

  @doc false
  def handle_call(msg, from, %{state: {mod, mod_state}} = state) do
    maybe_handle(mod, :handle_call, [msg, from, mod_state], state)
  end

  @doc false
  def handle_info(:timeout, %{protocol: protocol} = state) do
    case Protocol.ping(protocol) do
      {:ok, protocol} ->
        {:noreply, %{state | protocol: protocol}, state.idle_interval}

      {error, reason, protocol} ->
        reconnect_or_stop(error, reason, protocol, state)
    end
  end

  def handle_info(msg, %{protocol: protocol, state: {mod, mod_state}} = state) do
    opts = [notify: &mod.handle_notification(&1, &2, mod_state)]

    case Protocol.handle_info(msg, opts, protocol) do
      {:ok, protocol} ->
        {:noreply, %{state | protocol: protocol}, state.idle_interval}

      {:unknown, protocol} ->
        maybe_handle(mod, :handle_info, [msg, mod_state], %{state | protocol: protocol})

      {error, reason, protocol} ->
        reconnect_or_stop(error, reason, protocol, state)
    end
  end

  def handle_info(msg, %{state: {mod, mod_state}} = state) do
    maybe_handle(mod, :handle_info, [msg, mod_state], state)
  end

  defp maybe_handle(mod, fun, args, state) do
    if function_exported?(mod, fun, length(args)) do
      handle(mod, fun, args, state)
    else
      {:noreply, state}
    end
  end

  defp handle(mod, fun, args, state) do
    case apply(mod, fun, args) do
      {:noreply, mod_state} ->
        {:noreply, %{state | state: {mod, mod_state}}, state.idle_interval}

      {:query, query, mod_state} ->
        opts = [notify: &mod.handle_notification(&1, &2, mod_state)]

        state = %{state | state: {mod, mod_state}}

        with {:ok, result, protocol} <- Protocol.handle_simple(query, opts, state.protocol),
             {:ok, protocol} <- Protocol.checkin(protocol) do
          maybe_handle(mod, :handle_result, [result, mod_state], %{state | protocol: protocol})
        else
          {error, reason, protocol} ->
            state = %{state | protocol: protocol}

            {:noreply, state, _} = maybe_handle(mod, :handle_disconnect, [mod_state], state)

            reconnect_or_stop(error, reason, protocol, state)
        end
    end
  end

  defp reconnect_or_stop(_error, reason, protocol, %{auto_reconnect: false} = state) do
    {:stop, reason, %{state | protocol: protocol}}
  end

  defp reconnect_or_stop(_error, _reason, _protocol, %{auto_reconnect: true} = state) do
    {:connect, :reconnect, state}
  end

  defp opts(), do: Process.get(__MODULE__)
  defp put_opts(opts), do: Process.put(__MODULE__, opts)
end

defmodule Postgrex.Notifications.Default do
  @moduledoc false

  @behaviour Postgrex.Notifications

  require Logger

  alias Postgrex.Notifications

  defstruct [:from, :ref, connected: false, listeners: %{}, listener_channels: %{}]

  @impl true
  def init(args) do
    {:ok, struct!(__MODULE__, args)}
  end

  @impl true
  def handle_connect(state) do
    state = %{state | connected: true}

    if map_size(state.listener_channels) > 0 do
      listen_statements =
        state.listener_channels
        |> Map.keys()
        |> Enum.map_join("\n", &~s(LISTEN "#{&1}";))

      query = "DO $$BEGIN #{listen_statements} END$$"

      {:query, query, state}
    else
      {:noreply, state}
    end
  end

  @impl true
  def handle_disconnect(state) do
    {:noreply, %{state | connected: false}}
  end

  @impl true
  def handle_call({:listen, channel}, {pid, _} = from, state) do
    ref = Process.monitor(pid)

    state = put_in(state.listeners[ref], {channel, pid})
    state = update_in(state.listener_channels[channel], &Map.put(&1 || %{}, ref, pid))

    cond do
      not state.connected ->
        Notifications.reply(from, {:eventually, ref})

        {:noreply, state}

      map_size(state.listener_channels[channel]) == 1 ->
        {:query, ~s(LISTEN "#{channel}"), %{state | from: from, ref: ref}}

      true ->
        Notifications.reply(from, {:ok, ref})

        {:noreply, state}
    end
  end

  def handle_call({:unlisten, ref}, from, state) do
    case state.listeners do
      %{^ref => {channel, _pid}} ->
        Process.demonitor(ref, [:flush])

        state = update_in(state.listeners, &Map.delete(&1, ref))
        state = update_in(state.listener_channels[channel], &Map.delete(&1, ref))

        if map_size(state.listener_channels[channel]) == 0 do
          state = update_in(state.listener_channels, &Map.delete(&1, channel))

          {:query, ~s(UNLISTEN "#{channel}"), %{state | from: from}}
        else
          from && Notifications.reply(from, :ok)

          {:noreply, state}
        end

      _ ->
        from && Notifications.reply(from, :error)

        {:noreply, state}
    end
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _, _}, state) do
    handle_call({:unlisten, ref}, nil, state)
  end

  def handle_info(msg, state) do
    Logger.info(fn ->
      context = " received unexpected message: "
      [inspect(__MODULE__), ?\s, inspect(self()), context | inspect(msg)]
    end)

    {:noreply, state}
  end

  @impl true
  def handle_result(_message, %{from: from, ref: ref} = state) do
    cond do
      from && ref ->
        Notifications.reply(from, {:ok, ref})

        {:noreply, %{state | from: nil, ref: nil}}

      from ->
        Notifications.reply(from, :ok)

        {:noreply, %{state | from: nil}}

      true ->
        {:noreply, state}
    end
  end

  @impl true
  def handle_notification(channel, payload, state) do
    for {ref, pid} <- Map.get(state.listener_channels, channel, []) do
      send(pid, {:notification, self(), ref, channel, payload})
    end
  end
end
