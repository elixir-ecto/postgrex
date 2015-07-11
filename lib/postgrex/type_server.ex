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
  @spec fetch(key :: term) :: {:ok, table} | {:lock, reference, table}
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

  ## Callbacks

  def init([]) do
    {:ok, {%{}, HashDict.new}}
  end

  def handle_call({:fetch, key}, {pid, _} = from, {tables, conns}) do
    {ref, conns} = add_connection(pid, key, conns)

    case Map.get(tables, key, :missing) do
      {:ready, counter, timer, table} ->
        cancel_timer(timer)
        tables = Map.put(tables, key, {:ready, counter + 1, nil, table})
        {:reply, {:ok, table}, {tables, conns}}

      {:waiting, owner_ref, waiting, table} ->
        tables = Map.put(tables, key, {:waiting, owner_ref, [{ref, from}|waiting], table})
        {:noreply, {tables, conns}}

      :missing ->
        table = :ets.new(:postgrex_type_server, [:set, :public, read_concurrency: true])
        tables = Map.put(tables, key, {:waiting, ref, [], table})
        {:reply, {:lock, ref, table}, {tables, conns}}
    end
  end

  def handle_call({:unlock, ref}, _from, {tables, conns}) do
    case HashDict.fetch(conns, ref) do
      {:ok, key} ->
        {:waiting, ^ref, waiting, table} = Map.fetch!(tables, key)
        for {_ref, from} <- waiting, do: GenServer.reply(from, {:ok, table})
        tables = Map.put(tables, key, {:ready, 1 + length(waiting), nil, table})
        {:reply, :ok, {tables, conns}}

      :error ->
        {:reply, :error, {tables, conns}}
    end
  end

  def handle_info({:DOWN, ref, _, _, _}, {tables, conns}) do
    {key, conns} = HashDict.pop(conns, ref)

    tables =
      case Map.fetch!(tables, key) do
        # There will be no one left using the table
        {:ready, 1, nil, table} ->
          ref     = make_ref()
          timeout = Application.get_env(:postgrex, :type_server_reap_after)
          timer   = Process.send_after(self(), {:drop, ref, key}, timeout)
          Map.put(tables, key, {:ready, 0, {ref, timer}, table})

        # Others are using the table
        {:ready, counter, nil, table} ->
          Map.put(tables, key, {:ready, counter - 1, nil, table})

        # The process responsible for creating the table crashed
        # and no one else is interested on it
        {:waiting, ^ref, [], table} ->
          :ets.delete(table)
          Map.delete(tables, key)

        # The process responsible for creating the table crashed
        # and others are waiting
        {:waiting, ^ref, [{owner_ref, from}|waiting], table} ->
          GenServer.reply(from, {:lock, owner_ref, table})
          Map.put(tables, key, {:waiting, owner_ref, waiting, table})

        # One process in the waiting list crashed
        {:waiting, owner_ref, waiting, table} ->
          waiting = List.keydelete(waiting, ref, 0)
          Map.put(tables, key, {:waiting, owner_ref, waiting, table})
      end

    {:noreply, {tables, conns}}
  end

  def handle_info({:drop, ref, key}, {tables, conns}) do
    tables =
      case Map.fetch!(tables, key) do
        {:ready, 0, {^ref, _timer}, table} ->
          :ets.delete(table)
          Map.delete(tables, key)
        _ ->
          tables
      end

    {:noreply, {tables, conns}}
  end

  def handle_info(_other, state) do
    {:noreply, state}
  end

  defp add_connection(pid, key, conns) do
    ref   = Process.monitor(pid)
    conns = HashDict.put(conns, ref, key)
    {ref, conns}
  end

  defp cancel_timer(nil), do: :ok
  defp cancel_timer({_, timer}), do: :erlang.cancel_timer(timer)
end