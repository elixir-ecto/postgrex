defmodule Postgrex.Notifications do
  @moduledoc """
  API for notifications (pub/sub) in Postgres.
  """

  use Connection

  require Logger

  alias Postgrex.Protocol

  @timeout 5000

  defstruct protocol: nil, parameters: nil,
            listeners: Map.new(), listener_channels: Map.new()

  ## PUBLIC API ##

  @doc """
  Start the notification connection process and connect to postgres.

  The option that this function accepts are exactly the same accepted by
  `Postgrex.start_link/1`.
  """
  @spec start_link(Keyword.t) :: {:ok, pid} | {:error, Postgrex.Error.t | term}
  def start_link(opts) do
    Connection.start_link(__MODULE__, Postgrex.Utils.default_opts(opts), [name: opts[:name]])
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

  ## CALLBACKS ##

  def init(opts) do
    if opts[:sync_connect] do
      sync_connect(opts)
    else
      {:connect, :init, opts}
    end
  end

  def connect(_, opts) do
    case Protocol.connect([types: nil] ++ opts) do
      {:ok, protocol} ->
        {:ok, %__MODULE__{protocol: protocol}}
      {:error, reason} ->
        {:stop, reason, opts}
    end
  end

  def handle_call({:listen, channel}, {pid, _} = from, s) do
    ref = Process.monitor(pid)

    s = put_in(s.listeners[ref], {channel, pid})
    s = update_in(s.listener_channels[channel], &((&1 || Map.new()) |> Map.put(ref, pid)))
    # If this is the first listener for the given channel, we need to actually
    # issue the LISTEN query.
    if Map.size(s.listener_channels[channel]) == 1 do
      listener_query("LISTEN \"#{channel}\"", {:ok, ref}, from, s)
    else
      {:reply, {:ok, ref}, s}
    end
  end

  def handle_call({:unlisten, ref}, from, s) do
    case Map.fetch(s.listeners, ref) do
      :error ->
        {:reply, {:error, %ArgumentError{}}, s}
      {:ok, {channel, _pid}} ->
        Process.demonitor(ref, [:flush])

        s = remove_monitored_listener(s, ref, channel)

        # If no listeners remain for `channel`, then let's actually issue an
        # UNLISTEN query.
        if Map.size(s.listener_channels[channel]) == 0 do
          s = update_in(s.listener_channels, &Map.delete(&1, channel))
          listener_query("UNLISTEN \"#{channel}\"", :ok, from, s)
        else
          {:reply, :ok, s}
        end
    end
  end

  def handle_info({:DOWN, ref, :process, _, _}, s) do
    case Map.fetch(s.listeners, ref) do
      :error ->
        {:noreply, s}
      {:ok, {channel, _pid}} ->
        s = remove_monitored_listener(s, ref, channel)

        if Map.size(s.listener_channels[channel]) == 0 do
          s = update_in(s.listener_channels, &Map.delete(&1, channel))
          listener_query("UNLISTEN \"#{channel}\"", :ok, nil, s)
        else
          {:noreply, s}
        end
    end
  end

  def handle_info(msg, s) do
    %{protocol: protocol, listener_channels: channels, listeners: listeners} = s
    opts = [notify: &notify_listeners(channels, listeners, &1, &2)]

    case Protocol.handle_info(msg, opts, protocol) do
      {:ok, protocol} ->
        {:noreply, %{s | protocol: protocol}}
      {error, reason, protocol} when error in [:error, :disconnect] ->
        {:stop, reason, %{s | protocol: protocol}}
    end
  end

  defp listener_query(statement, result, from, s) do
    %{protocol: protocol, listener_channels: channels, listeners: listeners} = s
    opts = [notify: &notify_listeners(channels, listeners, &1, &2)]

    case Protocol.handle_listener(statement, opts, protocol) do
      {:ok, %Postgrex.Result{}, protocol} ->
        if from, do: Connection.reply(from, result)
        checkin(protocol, s)
      {error, reason, protocol} when error in [:error, :disconnect] ->
        {:stop, reason, %{s | protocol: protocol}}
    end
  end

  defp notify_listeners(channels, listeners, channel, payload) do
    Enum.each (Map.get(channels, channel) || []), fn {ref, _pid} ->
      {_, pid} = Map.fetch!(listeners, ref)
      send(pid, {:notification, self(), ref, channel, payload})
    end
  end

  defp checkin(protocol, s) do
    case Protocol.checkin(protocol) do
      {:ok, protocol} ->
        {:noreply, %{s | protocol: protocol}}
      {error, reason, protocol} when error in [:error, :disconnect] ->
        {:stop, reason, %{s | protocol: protocol}}
    end
  end

  defp sync_connect(opts) do
    case connect(:init, opts) do
      {:ok, _} = ok      -> ok
      {:stop, reason, _} -> {:stop, reason}
    end
  end

  defp remove_monitored_listener(s, ref, channel) do
    s = update_in(s.listeners, &Map.delete(&1, ref))
    update_in(s.listener_channels[channel], &Map.delete(&1, ref))
  end
end
