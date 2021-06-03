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

  defstruct idle_interval: 5000,
            protocol: nil,
            parameters: nil,
            listeners: %{},
            listener_channels: %{},
            auto_reconnect: false,
            reconnect_backoff: 500,
            connected: false

  ## PUBLIC API ##

  @type server :: GenServer.server()

  @doc false
  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]}
    }
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
      above. Defaults to `false`.

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
    {server_opts, opts} = Keyword.split(opts, [:name])
    opts = Keyword.put_new(opts, :sync_connect, true)
    connection_opts = Postgrex.Utils.default_opts(opts)
    Connection.start_link(__MODULE__, connection_opts, server_opts)
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
    message = {:listen, channel}
    timeout = opts[:timeout] || @timeout
    Connection.call(pid, message, timeout)
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
    message = {:unlisten, ref}
    timeout = opts[:timeout] || @timeout
    Connection.call(pid, message, timeout)
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

  def init(opts) do
    idle_timeout = opts[:idle_timeout]

    if idle_timeout do
      require Logger

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
      reconnect_backoff: reconnect_backoff
    }

    put_opts(opts)

    if opts[:sync_connect] do
      case connect(:init, state) do
        {:ok, _, _} = ok -> ok
        {:backoff, _, _} = backoff -> backoff
        {:stop, reason, _} -> {:stop, reason}
      end
    else
      {:connect, :init, state}
    end
  end

  def connect(_, s) do
    opts =
      case Keyword.get(opts(), :configure) do
        {module, fun, args} -> apply(module, fun, [opts() | args])
        fun when is_function(fun, 1) -> fun.(opts())
        nil -> opts()
      end

    case Protocol.connect([types: nil] ++ opts) do
      {:ok, protocol} ->
        s = %{s | listener_channels: %{}, connected: true, protocol: protocol}
        Enum.reduce_while(s.listeners, {:ok, s, s.idle_interval}, &reestablish_listener/2)

      {:error, reason} ->
        if s.auto_reconnect do
          {:backoff, s.reconnect_backoff, s}
        else
          {:stop, reason, s}
        end
    end
  end

  def handle_call({:listen, channel}, {pid, _} = from, s) do
    ref = Process.monitor(pid)
    s = put_in(s.listeners[ref], {channel, pid})
    do_listen(channel, pid, ref, from, s)
  end

  def handle_call({:unlisten, ref}, from, s) do
    case s.listeners do
      %{^ref => {channel, _pid}} ->
        Process.demonitor(ref, [:flush])
        do_unlisten(channel, ref, from, s)

      %{} ->
        {:reply, :error, s, s.idle_interval}
    end
  end

  def handle_info({:DOWN, ref, :process, _, _}, s) do
    case s.listeners do
      %{^ref => {channel, _pid}} ->
        do_unlisten(channel, ref, nil, s)

      %{} ->
        {:noreply, s, s.idle_interval}
    end
  end

  def handle_info(:timeout, %{protocol: protocol} = state) do
    case Protocol.ping(protocol) do
      {:ok, protocol} ->
        {:noreply, %{state | protocol: protocol}, state.idle_interval}

      {error, reason, protocol} ->
        reconnect_or_stop(error, reason, protocol, state)
    end
  end

  def handle_info(msg, s) do
    %{protocol: protocol, listener_channels: channels, listeners: listeners} = s
    opts = [notify: &notify_listeners(channels, listeners, &1, &2)]

    case Protocol.handle_info(msg, opts, protocol) do
      {:ok, protocol} ->
        {:noreply, %{s | protocol: protocol}, s.idle_interval}

      {error, reason, protocol} ->
        reconnect_or_stop(error, reason, protocol, s)
    end
  end

  defp reestablish_listener({ref, {channel, pid}}, {:ok, s, timeout}) do
    case do_listen(channel, pid, ref, nil, s) do
      {:noreply, s, _} -> {:cont, {:ok, s, timeout}}
      error -> {:halt, error}
    end
  end

  defp do_listen(channel, pid, ref, from, s) do
    s = update_in(s.listener_channels[channel], &((&1 || %{}) |> Map.put(ref, pid)))

    # If this is the first listener for the given channel,
    # we need to actually issue the LISTEN query.
    if map_size(s.listener_channels[channel]) == 1 do
      listener_query("LISTEN \"#{channel}\"", {:ok, ref}, {:eventually, ref}, from, s)
    else
      if from, do: Connection.reply(from, {:ok, ref})
      {:noreply, s, s.idle_interval}
    end
  end

  defp do_unlisten(channel, ref, from, s) do
    s = update_in(s.listeners, &Map.delete(&1, ref))
    s = update_in(s.listener_channels[channel], &Map.delete(&1, ref))

    # If this was the last listener for the given channel,
    # we need to issue the UNLISTEN query.
    if map_size(s.listener_channels[channel]) == 0 do
      s = update_in(s.listener_channels, &Map.delete(&1, channel))
      listener_query("UNLISTEN \"#{channel}\"", :ok, :ok, from, s)
    else
      if from, do: Connection.reply(from, :ok)
      {:noreply, s, s.idle_interval}
    end
  end

  defp listener_query(_statement, _ok_result, async_result, from, %{connected: false} = s) do
    if from, do: Connection.reply(from, async_result)
    {:noreply, s, s.idle_interval}
  end

  defp listener_query(statement, ok_result, async_result, from, s) do
    %{protocol: protocol, listener_channels: channels, listeners: listeners} = s
    opts = [notify: &notify_listeners(channels, listeners, &1, &2)]

    case Protocol.handle_listener(statement, opts, protocol) do
      {:ok, %Postgrex.Result{}, protocol} ->
        if from, do: Connection.reply(from, ok_result)
        checkin(protocol, s)

      {error, reason, protocol} ->
        case reconnect_or_stop(error, reason, protocol, s) do
          {:stop, _, _} = stop ->
            stop

          {:connect, _, _} = connect ->
            if from, do: Connection.reply(from, async_result)
            connect
        end
    end
  end

  defp notify_listeners(channels, listeners, channel, payload) do
    Enum.each(Map.get(channels, channel) || [], fn {ref, _pid} ->
      {_, pid} = Map.fetch!(listeners, ref)
      send(pid, {:notification, self(), ref, channel, payload})
    end)
  end

  defp checkin(protocol, s) do
    case Protocol.checkin(protocol) do
      {:ok, protocol} ->
        {:noreply, %{s | protocol: protocol}, s.idle_interval}

      {error, reason, protocol} ->
        reconnect_or_stop(error, reason, protocol, s)
    end
  end

  defp reconnect_or_stop(error, reason, protocol, %{auto_reconnect: false} = s)
       when error in [:error, :disconnect] do
    {:stop, reason, %{s | protocol: protocol}}
  end

  defp reconnect_or_stop(error, _reason, _protocol, %{auto_reconnect: true} = s)
       when error in [:error, :disconnect] do
    {:connect, :reconnect, %{s | connected: false}}
  end

  defp opts(), do: Process.get(__MODULE__)
  defp put_opts(opts), do: Process.put(__MODULE__, opts)
end
