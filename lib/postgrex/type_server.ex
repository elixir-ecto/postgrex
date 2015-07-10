defmodule Postgrex.TypeServer do
  @moduledoc false

  use GenServer

  @name  __MODULE__
  @table :postgrex_type_server
  @type  table :: :ets.tab

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
  @spec fetch(pid, key :: term) :: {:ok, table} | {:lock, reference, table}
  def fetch(pid, key) do
    case :ets.lookup(@table, pid) do
      [table] -> {:ok, table}
      []      -> GenServer.call(@name, {:fetch, pid, key}, :infinity)
    end
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
    :ets.new(@table, [:set, :named_table, :protected, read_concurrency: true])
    {:ok, {%{}, HashDict.new}}
  end

  def handle_call({:fetch, pid, key}, from, {tables, conns}) do
    case Map.get(tables, key, :missing) do
      {:ready, refs, table} ->
        {ref, conns} = add_connection(pid, key, table, conns)
        tables = Map.put(tables, key, {:ready, [ref|refs], table})
        {:reply, {:ok, table}, {tables, conns}}

      {:waiting, owner_ref, waiting, table} ->
        {ref, conns} = add_connection(pid, key, table, conns)
        tables = Map.put(tables, key, {:waiting, owner_ref, [{ref, from}|waiting], table})
        {:noreply, {tables, conns}}

      :missing ->
        table = :ets.new(:postgrex_type_conn, [:set, :public, read_concurrency: true])
        {ref, conns} = add_connection(pid, key, table, conns)
        tables = Map.put(tables, key, {:waiting, ref, [], table})
        {:reply, {:lock, ref, table}, {tables, conns}}
    end
  end

  def handle_call({:unlock, ref}, _from, {tables, conns}) do
    case HashDict.get(conns, ref, :missing) do
      {_pid, key} ->
        {:waiting, ^ref, waiting, table} = Map.fetch!(tables, key)

        refs =
          for {waiting_ref, from} <- waiting do
            GenServer.reply(from, {:ok, table})
            waiting_ref
          end

        tables = Map.put(tables, key, {:ready, [ref|refs], table})
        {:reply, :ok, {tables, conns}}

      :missing ->
        {:reply, :error, {tables, conns}}
    end
  end

  def handle_info({:DOWN, ref, _, _, _}, {tables, conns}) do
    {{pid, key}, conns} = HashDict.pop(conns, ref)
    :ets.delete(@table, pid)

    tables =
      case Map.fetch!(tables, key) do
        # There will be no one left using the table
        {:ready, [^ref], table} ->
          :ets.delete(table)
          Map.delete(tables, key)

        # Others are using the table
        {:ready, refs, table} ->
          Map.put(tables, key, {:ready, List.delete(refs, ref), table})

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
          waiting = Keyword.delete(waiting, ref)
          Map.put(tables, key, {:waiting, owner_ref, waiting, table})
      end

    {:noreply, {tables, conns}}
  end

  def handle_info(_other, state) do
    {:noreply, state}
  end

  defp add_connection(pid, key, table, conns) do
    ref   = Process.monitor(pid)
    conns = HashDict.put(conns, ref, {pid, key})
    :ets.insert(@table, {pid, table})
    {ref, conns}
  end
end