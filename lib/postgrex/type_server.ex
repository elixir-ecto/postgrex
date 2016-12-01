defmodule Postgrex.TypeServer do
  @moduledoc false

  use GenServer

  defstruct [:types, :connections, :lock, :waiting]

  @timeout 60_000

  @doc """
  Starts a type server.
  """
  @spec start_link(module, pid) :: GenServer.on_start
  def start_link(module, starter) do
    GenServer.start_link(__MODULE__, {module, starter})
  end

  @doc """
  Fetches a lock for the given type server.

  We attempt to achieve a lock on the type server for updating the entries.
  If another process got the lock, we wait until the entries are available
  or the other process crashes. If the module is unknown returns `:not_found`.
  """
  @spec fetch(pid) ::
    {:lock, reference, Postgrex.Types.state} | {:go, Postgrex.Types.state} |
    :noproc | :error
  def fetch(server) do
    try do
      GenServer.call(server, :fetch, @timeout)
    catch
      # module timed out, pretend it did not exist.
      :exit, {:normal, _} -> :noproc
      :exit, {:noproc, _} -> :noproc
    end
  end

  @doc """
  Update the type server using the given reference and configuration.
  """
  @spec update(pid, reference, [Postgrex.TypeInfo.t]) :: :go
  def update(server, ref, type_infos) do
    GenServer.call(server, {:update, ref, type_infos}, @timeout)
  end

  @doc """
  Unlocks the given reference for a given module after types query failed.
  """
  @spec fail(pid, reference) :: :ok
  def fail(server, ref) do
    GenServer.cast(server, {:fail, ref})
  end

  ## Callbacks

  def init({module, starter}) do
    _ = Process.flag(:trap_exit, true)
    Process.link(starter)
    state = %__MODULE__{types: Postgrex.Types.new(module),
                        connections: MapSet.new([starter]), waiting: %{}}
    {:ok, state}
  end

  def handle_call(:fetch, from, %{lock: nil} = state) do
    lock(state, from)
  end
  def handle_call(:fetch, from, %{lock: ref} = state) when is_reference(ref) do
    wait(state, from)
  end

  def handle_call({:update, ref, type_infos}, _, %{lock: ref} = state)
      when is_reference(ref) do
    associate(state, type_infos)
  end

  def handle_cast({:fail, ref}, %{lock: ref} = state) when is_reference(ref) do
    Process.demonitor(ref, [:flush])
    failure(state)
  end


  def handle_info({:go, ref}, %{lock: ref} = state)
    when is_reference(ref) do
    go(state)
  end

  def handle_info({:DOWN, ref, _, _, _}, %{lock: ref} = state)
      when is_reference(ref) do
    failure(state)
  end
  def handle_info({:DOWN, ref, _, _, _}, state) do
    down(state, ref)
  end

  def handle_info({:EXIT, pid, _}, state) do
    exit(state, pid)
  end

  def handle_info(:timeout, state) do
    {:stop, :normal, state}
  end

  ## Helpers

  defp lock(%{connections: connections, types: types} = state, {pid, _}) do
    Process.link(pid)
    mref = Process.monitor(pid)
    state = %{state | lock: mref, connections: MapSet.put(connections, pid)}
    {:reply, {:lock, mref, types}, state}
  end

  defp wait(state, {pid, _} = from) do
    %{connections: connections, waiting: waiting} = state
    Process.link(pid)
    mref = Process.monitor(pid)
    state = %{state | connections: MapSet.put(connections, pid),
                      waiting: Map.put(waiting, mref, from)}
    {:noreply, state}
  end

  defp associate(%{types: types, lock: ref} = state, type_infos) do
    Postgrex.Types.associate_type_infos(type_infos, types)
    Process.demonitor(ref, [:flush])
    # flush message queue of waiters
    send(self(), {:go, ref})
    {:reply, :go, state}
  end

  defp go(%{types: types} = state) do
    %{state | lock: nil}
    |> reply({:go, types})
    |> check_processes()
  end

  defp failure(state) do
    %{state | lock: nil}
    |> reply(:error)
    |> check_processes()
  end

  defp reply(%{waiting: waiting} = state, resp) do
    _ = for {mref, from} <- waiting do
      Process.demonitor(mref, [:flush, :info]) && GenServer.reply(from, resp)
    end
    %{state | waiting: %{}}
  end

  defp down(%{waiting: waiting} = state, ref) do
    check_processes(%{state | waiting: Map.delete(waiting, ref)})
  end

  defp exit(%{connections: connections} = state, pid) do
    check_processes(%{state | connections: MapSet.delete(connections, pid)})
  end

  defp check_processes(%{lock: ref} = state) when is_reference(ref) do
    {:noreply, state}
  end
  defp check_processes(%{connections: connections} = state) do
    case MapSet.size(connections) do
      0 ->
        timeout = Application.fetch_env!(:postgrex, :type_server_reap_after)
        {:noreply, state, timeout}
      _ ->
        {:noreply, state}
    end
  end
end
