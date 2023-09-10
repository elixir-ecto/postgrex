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

  ## Async connect, auto-reconnects and missed notifications

  By default, the notification system establishes a connection to the
  database on initialization, you can configure the connection to happen
  asynchronously. You can also configure the connection to automatically
  reconnect.

  Note however that when the notification system is waiting for a connection,
  any notifications that occur during the disconnection period are not queued
  and cannot be recovered. Similarly, any listen command will be queued until
  the connection is up.

  There is a race condition between starting to listen and notifications being
  issued "at the same time", as explained [in the PostgreSQL documentation](https://www.postgresql.org/docs/current/sql-listen.html).
  If your application needs to keep a consistent representation of data, follow
  the three-step approach of first subscribing, then obtaining the current
  state of data, then handling the incoming notifications.

  Beware that the same
  race condition applies to auto-reconnects. A simple way of dealing with this
  issue is not using the auto-reconnect feature directly, but monitoring and
  re-starting the Notifications process, then subscribing to channel messages
  over again, using the same three-step approach.

  ## A note on casing

  While PostgreSQL seems to behave as case-insensitive, it actually has a very
  peculiar behaviour on casing. When you write:

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

  @typedoc since: "0.17.0"
  @type server :: :gen_statem.from()

  alias Postgrex.SimpleConnection

  @behaviour SimpleConnection

  require Logger

  defstruct [
    :from,
    :ref,
    auto_reconnect: false,
    connected: false,
    listeners: %{},
    listener_channels: %{}
  ]

  @timeout 5000

  @doc false
  def child_spec(opts) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}}
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
  @spec start_link(Keyword.t()) :: {:ok, pid} | {:error, Postgrex.Error.t() | term}
  def start_link(opts) do
    args = Keyword.take(opts, [:auto_reconnect])

    SimpleConnection.start_link(__MODULE__, args, opts)
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
  @spec listen(server, String.t(), Keyword.t()) ::
          {:ok, reference} | {:eventually, reference}
  def listen(pid, channel, opts \\ []) do
    SimpleConnection.call(pid, {:listen, channel}, Keyword.get(opts, :timeout, @timeout))
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
    SimpleConnection.call(pid, {:unlisten, ref}, Keyword.get(opts, :timeout, @timeout))
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

  @impl true
  def init(args) do
    {:ok, struct!(__MODULE__, args)}
  end

  @impl true
  def notify(channel, payload, state) do
    for {ref, pid} <- Map.get(state.listener_channels, channel, []) do
      send(pid, {:notification, self(), ref, channel, payload})
    end

    :ok
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
    state = %{state | connected: false}

    if state.auto_reconnect && state.from && state.ref do
      SimpleConnection.reply(state.from, {:eventually, state.ref})

      {:noreply, %{state | from: nil, ref: nil}}
    else
      {:noreply, state}
    end
  end

  @impl true
  def handle_call({:listen, channel}, {pid, _} = from, state) do
    ref = Process.monitor(pid)

    state = put_in(state.listeners[ref], {channel, pid})
    state = update_in(state.listener_channels[channel], &Map.put(&1 || %{}, ref, pid))

    cond do
      not state.connected ->
        SimpleConnection.reply(from, {:eventually, ref})

        {:noreply, state}

      map_size(state.listener_channels[channel]) == 1 ->
        {:query, ~s(LISTEN "#{channel}"), %{state | from: from, ref: ref}}

      true ->
        SimpleConnection.reply(from, {:ok, ref})

        {:noreply, state}
    end
  end

  def handle_call({:unlisten, ref}, from, state) do
    case state.listeners do
      %{^ref => {channel, _pid}} ->
        Process.demonitor(ref, [:flush])

        {_, state} = pop_in(state.listeners[ref])
        {_, state} = pop_in(state.listener_channels[channel][ref])

        if map_size(state.listener_channels[channel]) == 0 do
          {_, state} = pop_in(state.listener_channels[channel])

          {:query, ~s(UNLISTEN "#{channel}"), %{state | from: from}}
        else
          from && SimpleConnection.reply(from, :ok)

          {:noreply, state}
        end

      _ ->
        from && SimpleConnection.reply(from, :error)

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
        SimpleConnection.reply(from, {:ok, ref})

        {:noreply, %{state | from: nil, ref: nil}}

      from ->
        SimpleConnection.reply(from, :ok)

        {:noreply, %{state | from: nil}}

      true ->
        {:noreply, state}
    end
  end
end
