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
    {:ok, {%{}, %{}, HashDict.new}}
  end

  def handle_call({:fetch, key}, {pid, _} = from, {tables, monitors, conns}) do
    case Map.get(tables, key, :missing) do
      {:ready, counter, timer, table} ->
        cancel_timer(timer)
        conns = add_connection(pid, key, conns)
        tables = Map.put(tables, key, {:ready, counter + 1, nil, table})
        {:reply, {:go, table}, {tables, monitors, conns}}

      {:waiting, owner_ref, waiting, table} ->
        ref = Process.monitor(pid)
        monitors = Map.put(monitors, ref, key)
        tables = Map.put(tables, key, {:waiting, owner_ref, [{pid, ref, from}|waiting], table})
        {:noreply, {tables, monitors, conns}}

      :missing ->
        table = types_table()
        ref = Process.monitor(pid)
        monitors = Map.put(monitors, ref, key)
        tables = Map.put(tables, key, {:waiting, ref, [], table})
        {:reply, {:lock, ref, table}, {tables, monitors, conns}}
    end
  end

  def handle_call({:unlock, ref}, {pid, _}, {tables, monitors, conns}) do
    case Map.fetch(monitors, ref) do
      {:ok, key} ->
        {:waiting, ^ref, waiting, table} = Map.fetch!(tables, key)
        Process.demonitor(ref, [:flush])
        Process.link(pid)
        monitors = Map.delete(monitors, ref)
        conns = HashDict.put(conns, pid, key)
        go = fn({pid2, ref2, from2}, {monitors, conns}) ->
          Process.demonitor(ref2, [:flush])
          Process.link(pid2)
          GenServer.reply(from2, {:go, table})
          {Map.delete(monitors, ref2), HashDict.put(conns, pid2, key)}
        end
        {monitors, conns} = Enum.reduce(waiting, {monitors, conns}, go)
        tables = Map.put(tables, key, {:ready, 1 + length(waiting), nil, table})
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
    {key, conns} = HashDict.pop(conns, pid)

    tables =
      case Map.get(tables, key, :missing) do
        # There will be no one left using the table
        {:ready, 1, nil, table} ->
          ref     = make_ref()
          timeout = Application.get_env(:postgrex, :type_server_reap_after)
          timer   = Process.send_after(self(), {:drop, ref, key}, timeout)
          Map.put(tables, key, {:ready, 0, {ref, timer}, table})
        # Others are using the table
        {:ready, counter, nil, table} ->
          Map.put(tables, key, {:ready, counter - 1, nil, table})
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
        # The process responsible for creating the table crashed
        # and no one else is interested on it
        {:waiting, ^ref, [], table} ->
          :ets.delete(table)
          Map.delete(tables, key)

        # The process responsible for creating the table crashed
        # and others are waiting, use new table as might be writes
        # to table
        {:waiting, ^ref, [{_, owner_ref, from}|waiting], table} ->
          :ets.delete(table)
          new_table = types_table()
          GenServer.reply(from, {:lock, owner_ref, new_table})
          Map.put(tables, key, {:waiting, owner_ref, waiting, new_table})

        # Process is in the waiting list crashed
        {:waiting, owner_ref, waiting, table} ->
          waiting = List.keydelete(waiting, ref, 1)
          Map.put(tables, key, {:waiting, owner_ref, waiting, table})
      end

    {:noreply, {tables, monitors, conns}}
  end

  defp add_connection(pid, key, conns) do
    Process.link(pid)
    update =
      fn(key2) when key2 === key ->
          key;
        (key2) ->
          raise "#{inspect pid} key #{inspect key} does not match #{inspect key2}"
      end
    HashDict.update(conns, pid, key, update)
  end

  defp cancel_timer(nil), do: :ok
  defp cancel_timer({_, timer}), do: :erlang.cancel_timer(timer)
end