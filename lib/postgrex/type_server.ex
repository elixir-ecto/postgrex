defmodule Postgrex.TypeServer do
  @moduledoc false

  use GenServer, restart: :temporary

  defstruct [:types, :connections, :lock, :waiting]

  @timeout 60_000

  @doc """
  Starts a type server.
  """
  @spec start_link({module, pid, keyword}) :: GenServer.on_start()
  def start_link({module, starter, opts}) do
    GenServer.start_link(__MODULE__, {module, starter}, opts)
  end

  @doc """
  Fetches a lock for the given type server.

  We attempt to achieve a lock on the type server for updating the entries.
  If another process got the lock we wait for it to finish.
  """
  @spec fetch(pid) ::
          {:lock, reference, Postgrex.Types.state()} | :noproc | :error
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
  @spec update(pid, reference, [Postgrex.TypeInfo.t()]) :: :ok
  def update(server, ref, [_ | _] = type_infos) do
    GenServer.call(server, {:update, ref, type_infos}, @timeout)
  end

  def update(server, ref, []) do
    done(server, ref)
  end

  @doc """
  Unlocks the given reference for a given module if no update.
  """
  @spec done(pid, reference) :: :ok
  def done(server, ref) do
    GenServer.cast(server, {:done, ref})
  end

  ## Callbacks

  def init({module, starter}) do
    _ = Process.flag(:trap_exit, true)
    Process.link(starter)

    state = %__MODULE__{
      types: Postgrex.Types.new(module),
      connections: MapSet.new([starter]),
      waiting: :queue.new()
    }

    {:ok, state}
  end

  def handle_call(:fetch, from, %{lock: nil} = state) do
    lock(state, from)
  end

  def handle_call(:fetch, from, %{lock: ref} = state) when is_reference(ref) do
    wait(state, from)
  end

  def handle_call({:update, ref, type_infos}, from, %{lock: ref} = state)
      when is_reference(ref) do
    associate(state, type_infos, from)
  end

  def handle_cast({:done, ref}, %{lock: ref} = state) when is_reference(ref) do
    Process.demonitor(ref, [:flush])
    next(state)
  end

  def handle_info({:DOWN, ref, _, _, _}, %{lock: ref} = state)
      when is_reference(ref) do
    next(state)
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

    state = %{
      state
      | connections: MapSet.put(connections, pid),
        waiting: :queue.in({mref, from}, waiting)
    }

    {:noreply, state}
  end

  defp associate(%{types: types, lock: ref} = state, type_infos, from) do
    Postgrex.Types.associate_type_infos(type_infos, types)
    Process.demonitor(ref, [:flush])
    GenServer.reply(from, :go)
    next(state)
  end

  defp next(%{types: types, waiting: waiting} = state) do
    case :queue.out(waiting) do
      {{:value, {mref, from}}, waiting} ->
        GenServer.reply(from, {:lock, mref, types})
        {:noreply, %{state | lock: mref, waiting: waiting}}

      {:empty, waiting} ->
        check_processes(%{state | lock: nil, waiting: waiting})
    end
  end

  defp down(%{waiting: waiting} = state, ref) do
    check_processes(%{state | waiting: :queue.filter(fn {mref, _} -> mref != ref end, waiting)})
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
