defmodule Postgrex.Notifications.Connection do
  @moduledoc """
  API for notifications (pub/sub) in Postgres.
  """

  use Connection

  require Logger

  alias Postgrex.Protocol

  @timeout 5000

  defstruct protocol: nil, parameters: nil,
            listeners: HashDict.new(), listener_channels: HashDict.new()

  ## PUBLIC API ##

  @doc """
  Start the notification connection process and connect to postgres.

  The option that this function accepts are exactly the same accepted by
  `Postgrex.Connection.start_link/1`.
  """
  @spec start_link(Keyword.t) :: {:ok, pid} | {:error, Postgrex.Error.t | term}
  def start_link(opts) do
    Connection.start_link(__MODULE__, Postgrex.Utils.default_opts(opts))
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
    case Protocol.connect(opts) do
      {:ok, protocol, parameters, _} ->
        {:ok, %__MODULE__{protocol: protocol, parameters: parameters}}
      {:error, reason} ->
        {:stop, reason, opts}
    end
  end

  def handle_call({:listen, channel}, {pid, _} = from, s) do
    ref = Process.monitor(pid)

    s = put_in(s.listeners[ref], {channel, pid})
    s = update_in(s.listener_channels[channel], &((&1 || HashSet.new()) |> HashSet.put(ref)))

    # If this is the first listener for the given channel, we need to actually
    # issue the LISTEN query.
    if HashSet.size(s.listener_channels[channel]) == 1 do
      listener_query("LISTEN #{channel}", {:ok, ref}, from, :active_once, s)
    else
      {:reply, {:ok, ref}, s}
    end
  end

  def handle_call({:unlisten, ref}, from, s) do
    case HashDict.fetch(s.listeners, ref) do
      :error ->
        {:reply, {:error, %ArgumentError{}}, s}
      {:ok, {channel, _pid}} ->
        Process.demonitor(ref, [:flush])

        s = update_in(s.listeners, &HashDict.delete(&1, ref))
        s = update_in(s.listener_channels[channel], &HashSet.delete(&1, ref))

        # If no listeners remain for `channel`, then let's actually issue an
        # UNLISTEN query.
        if HashSet.size(s.listener_channels[channel]) == 0 do
          s = update_in(s.listener_channels, &HashDict.delete(&1, channel))
          listener_query("UNLISTEN #{channel}", :ok, from, :active_once, s)
        else
          {:reply, :ok, s}
        end
    end
  end

  def handle_info({:DOWN, ref, :process, _, _}, s) do
    case HashDict.fetch(s.listeners, ref) do
      :error ->
        {:noreply, s}
      {:ok, {channel, _pid}} ->
        s = update_in(s.listener_channels[channel], &HashSet.delete(&1, ref))
        s = update_in(s.listeners, &HashDict.delete(&1, ref))

        if HashSet.size(s.listener_channels[channel]) == 0 do
          s = update_in(s.listener_channels, &HashDict.delete(&1, channel))
          listener_query("UNLISTEN #{channel}", :ok, nil, :active_once, s)
        else
          {:noreply, s}
        end
    end
  end

  def handle_info(msg, s) do
    protocol_info(msg, s)
  end

  defp listener_query(statement, result, from, buffer, s) do
    %{protocol: protocol, parameters: parameters} = s

    case Protocol.query(protocol, statement, [], buffer) do
      {:ok, %Postgrex.Result{}, new_parameters, notifications, buffer} ->
        _ = from && Connection.reply(from, result)
        notify_listeners(notifications, s)
        parameters = Map.merge(parameters, new_parameters)
        checkin(buffer, s)
        {:noreply, %{s | parameters: parameters}}
      {:error, error} ->
        Connection.reply(from, error)
        {:stop, error, s}
    end
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

  defp protocol_info(msg, s) do
    case Protocol.message(s.protocol, msg) do
      {:ok, new_parameters, notifications} ->
        notify_listeners(notifications, s)
        {:noreply, %{s | parameters: Map.merge(s.parameters, new_parameters)}}
      :unknown ->
        Logger.info fn() ->
          [inspect(__MODULE__), ?\s, inspect(self()), " received message: " |
            inspect(msg)]
        end
        {:noreply, s}
      {:error, reason} ->
        {:stop, reason, s}
    end
  end

  defp checkin(buffer, s) do
    case Protocol.checkin(s.protocol, buffer) do
      :ok              -> {:noreply, s}
      {:error, reason} -> {:stop, reason, s}
    end
  end

  defp sync_connect(opts) do
    case connect(:init, opts) do
      {:ok, _} = ok      -> ok
      {:stop, reason, _} -> {:stop, reason}
    end
  end
end
