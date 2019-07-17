defmodule Postgrex.Parameters do
  @moduledoc false

  use GenServer

  defstruct []
  @type t :: %__MODULE__{}

  def start_link(_) do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  @spec insert(%{binary => binary}) :: reference
  def insert(parameters) do
    GenServer.call(__MODULE__, {:insert, parameters})
  end

  @spec put(reference, binary, binary) :: boolean
  def put(ref, name, value) do
    try do
      :ets.lookup_element(__MODULE__, ref, 2)
    rescue
      ArgumentError ->
        false
    else
      parameters ->
        parameters = Map.put(parameters, name, value)
        :ets.update_element(__MODULE__, ref, {2, parameters})
    end
  end

  @spec delete(reference) :: :ok
  def delete(ref) do
    GenServer.cast(__MODULE__, {:delete, ref})
  end

  @spec fetch(reference) :: {:ok, %{binary => binary}} | :error
  def fetch(ref) do
    try do
      :ets.lookup_element(__MODULE__, ref, 2)
    rescue
      ArgumentError ->
        :error
    else
      parameters ->
        {:ok, parameters}
    end
  end

  def init(nil) do
    opts = [:public, :named_table, {:read_concurrency, true}, {:write_concurrency, true}]
    state = :ets.new(__MODULE__, opts)
    {:ok, state}
  end

  def handle_call({:insert, parameters}, {pid, _}, state) do
    ref = Process.monitor(pid)
    true = :ets.insert_new(state, {ref, parameters})
    {:reply, ref, state}
  end

  def handle_cast({:delete, ref}, state) do
    :ets.delete(state, ref)
    {:noreply, state}
  end

  def handle_info({:DOWN, ref, :process, _, _}, state) do
    :ets.delete(state, ref)
    {:noreply, state}
  end
end

defimpl DBConnection.Query, for: Postgrex.Parameters do
  def parse(query, _), do: query
  def describe(query, _), do: query
  def encode(_, nil, _), do: nil
  def decode(_, parameters, _), do: parameters
end
