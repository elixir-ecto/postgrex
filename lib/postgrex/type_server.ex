defmodule Postgrex.TypeServer do
  @moduledoc false

  use GenServer

  @name __MODULE__
  @type table :: :ets.tab

  @doc """
  Starts the type server.
  """
  @spec start_link :: GenServer.on_start
  def start_link do
    GenServer.start_link(__MODULE__, [], name: @name)
  end

  @doc """
  Fetches the cached type entry for the given pid.

  If one does not exist, we attempt to achieve a lock on key
  for fetching the entries. If another process got the lock,
  we wait until the entries are available or the other process
  crashes.
  """
  @spec fetch(key :: term) :: {:lock, reference, table} | {:go, table}
  def fetch(key) do
    GenServer.call(@name, {:fetch, key}, 60_000)
  end

  @doc """
  Unlocks the given reference for a given key after types are loaded.
  """
  @spec unlock(reference) :: :ok | :error
  def unlock(ref) do
    GenServer.call(@name, {:unlock, ref})
  end

  @doc """
  Unlocks the given reference for a given key after types failed to be loaded.
  """
  @spec fail(reference) :: :ok
  def fail(ref) do
    GenServer.cast(@name, {:fail, ref})
  end

  ## Callbacks

  def init([]) do
    _ = Process.flag(:trap_exit, true)
    {:ok, {%{}, %{}, Map.new}}
  end

  def handle_call({:fetch, key}, {pid, _} = from, {tables, monitors, conns}) do
    case Map.get(tables, key, :missing) do
      {:ready, counter, timer, table} ->
        cancel_timer(timer)
        ref = Process.monitor(pid)
        monitors = Map.put(monitors, ref, key)
        tables = Map.put(tables, key, {:waiting, ref, counter, [], table})
        {:reply, {:lock, ref, table}, {tables, monitors, conns}}

      {:waiting, owner_ref, counter, waiting, table} ->
        ref = Process.monitor(pid)
        monitors = Map.put(monitors, ref, key)
        tables = Map.put(tables, key, {:waiting, owner_ref, counter, [{pid, ref, from}|waiting], table})
        {:noreply, {tables, monitors, conns}}

      :missing ->
        table = types_table()
        ref = Process.monitor(pid)
        monitors = Map.put(monitors, ref, key)
        tables = Map.put(tables, key, {:waiting, ref, 0, [], table})
        {:reply, {:lock, ref, table}, {tables, monitors, conns}}
    end
  end

  def handle_call({:unlock, ref}, {pid, _}, {tables, monitors, conns}) do
    case Map.fetch(monitors, ref) do
      {:ok, key} ->
        {:waiting, ^ref, counter, waiting, table} = Map.fetch!(tables, key)
        conns_before = Map.size(conns)
        conns = add_connection(conns, pid, ref, key)
        go = fn({pid2, ref2, from2}, {monitors, conns}) ->
          GenServer.reply(from2, {:go, table})
          {Map.delete(monitors, ref2), add_connection(conns, pid2, ref2, key)}
        end
        {monitors, conns} = Enum.reduce(waiting, {monitors, conns}, go)
        new_conns = Map.size(conns) - conns_before
        counter = counter + new_conns
        tables = Map.put(tables, key, {:ready, counter, nil, table})
        {:reply, :ok, {tables, monitors, conns}}

      :error ->
        {:reply, :error, {tables, monitors, conns}}
    end
  end

  def handle_cast({:fail, ref}, state) do
    Process.demonitor(ref, [:flush])
    failure(ref, state)
  end

  def handle_info({:DOWN, ref, _, _, _}, state) do
    failure(ref, state)
  end

  def handle_info({:EXIT, pid, _}, {tables, monitors, conns}) do
    {key, conns} = Map.pop(conns, pid)

    tables =
      case Map.get(tables, key, :missing) do
        # There will be no one left using the table
        {:ready, 1, nil, table} ->
          stale_table(tables, key, table)
        # Others are using the table
        {:ready, counter, nil, table} ->
          Map.put(tables, key, {:ready, counter - 1, nil, table})
        # Others are using/updating/waiting on the table
        {:waiting, ref, counter, waiting, table} ->
          Map.put(tables, key, {:waiting, ref, counter-1, waiting, table})
        :missing ->
          tables
      end
    {:noreply, {tables, monitors, conns}}
  end

  def handle_info({:drop, ref, key}, {tables, monitors, conns}) do
    tables =
      case Map.fetch!(tables, key) do
        {:ready, 0, {^ref, _timer}, table} ->
          :ets.delete(table)
          Map.delete(tables, key)
        _ ->
          tables
      end

    {:noreply, {tables, monitors, conns}}
  end

  def handle_info(_other, state) do
    {:noreply, state}
  end

  defp types_table() do
    :ets.new(:postgrex_type_server, [:set, :public, read_concurrency: true])
  end

  defp failure(ref, {tables, monitors, conns}) do
    {key, monitors} = Map.pop(monitors, ref)

    tables =
      case Map.fetch!(tables, key) do
        # The process responsible for creating or updating the table crashed
        # and no one else is interested on it
        {:waiting, ^ref, 0, [], table} ->
          stale_table(tables, key, table)

        # The process responsible for updating the table crashed
        # and no one else is waiting
        {:waiting, ^ref, counter, [], table} ->
          Map.put(tables, key, {:ready, counter, nil, table})

        # The process responsible for creating or updating the table crashed
        # and others are waiting
        {:waiting, ^ref, counter, [{_, owner_ref, from}|waiting], table} ->
          GenServer.reply(from, {:lock, owner_ref, table})
          Map.put(tables, key, {:waiting, owner_ref, counter, waiting, table})

        # A process in the waiting list crashed
        {:waiting, owner_ref, counter, waiting, table} ->
          waiting = List.keydelete(waiting, ref, 1)
          Map.put(tables, key, {:waiting, owner_ref, counter, waiting, table})
      end

    {:noreply, {tables, monitors, conns}}
  end

  defp stale_table(tables, key, table) do
    ref     = make_ref()
    timeout = Application.get_env(:postgrex, :type_server_reap_after)
    timer   = Process.send_after(self(), {:drop, ref, key}, timeout)
    Map.put(tables, key, {:ready, 0, {ref, timer}, table})
  end

  defp add_connection(conns, pid, ref, key) do
    Process.demonitor(ref, [:flush])
    Process.link(pid)
    update =
      fn(key2) when key2 === key ->
          key;
        (key2) ->
          raise "#{inspect pid} key #{inspect key} does not match #{inspect key2}"
      end
    Map.update(conns, pid, key, update)
  end

  defp cancel_timer(nil), do: :ok
  defp cancel_timer({_, timer}), do: :erlang.cancel_timer(timer)
end