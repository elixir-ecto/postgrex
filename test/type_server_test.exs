defmodule TypeServerTest do
  use ExUnit.Case, async: true
  alias Postgrex.TypeServer, as: TS

  test "does not unlock unknown references" do
    assert TS.unlock(make_ref) == :error
  end

  test "fetches and unlocks" do
    key = make_ref()
    assert {:lock, ref, table} = TS.fetch(key)
    assert :ets.info(table, :name) == :postgrex_type_server
    assert TS.unlock(ref) == :ok
  end

  test "fetches and exits" do
    key = make_ref()
    task = Task.async(fn() -> assert {:lock, _, _} = TS.fetch(key) end)
    {:lock, _, table} = Task.await(task)
    assert {:lock, _, table2} = TS.fetch(key)
    assert table != table2, "same table after lock/done"
  end

  test "blocks on fetch until lock is returned" do
    key = make_ref()
    {:lock, ref, table} = TS.fetch(key)

    task = Task.async fn -> TS.fetch(key) end
    TS.unlock(ref)
    assert {:go, ^table} = Task.await(task)
  end

  test "blocks on fetch until fail is returned" do
    key = make_ref()
    {:lock, ref, table} = TS.fetch(key)

    task = Task.async fn -> TS.fetch(key) end
    TS.fail(ref)
    assert {:lock, _, table2} = Task.await(task)
    assert table != table2, "same table after lock/done"
  end

  test "fetches existing table" do
    key = make_ref()
    {:lock, ref, table} = TS.fetch(key)
    TS.unlock(ref)

    task = Task.async(fn -> TS.fetch(key) end)
    assert {:go, ^table} = Task.await(task)
  end

  test "fetches existing table even if parent crashes" do
    key = make_ref()

    task = Task.async fn ->
      {:lock, ref, table} = TS.fetch(key)
      TS.unlock(ref)
      table
    end
    table = Task.await(task)
    wait_until_dead(task.pid)

    task = Task.async(fn -> TS.fetch(key) end)
    assert {:go, ^table} = Task.await(task)
  end

  test "fetches existing table even if other processes crashes" do
    key = make_ref()

    {:lock, ref, table} = TS.fetch(key)
    TS.unlock(ref)

    assert {:go, ^table} = Task.async(fn -> TS.fetch(key) end) |> Task.await()
    assert {:go, ^table} = Task.async(fn -> TS.fetch(key) end) |> Task.await()
    assert {:go, ^table} = Task.async(fn -> TS.fetch(key) end) |> Task.await()
  end

  test "does not fetch existing table if parent crashes and timeout passes" do
    Application.put_env(:postgrex, :type_server_reap_after, 0)
    key = make_ref()

    # Setup trace
    ts = Process.whereis(TS)
    :erlang.trace(ts, true, [:receive])

    task = Task.async fn ->
      {:lock, ref, table} = TS.fetch(key)
      TS.unlock(ref)
      table
    end
    table1 = Task.await(task)
    wait_until_dead(task.pid)

    # Wait until timeout kicks in
    assert_receive {:trace, ^ts, :receive, {:drop, _, ^key}}

    task = Task.async(fn -> TS.fetch(key) end)
    assert {:lock, _, table2} = Task.await(task)

    assert table1 != table2
    assert :ets.info(table1, :name) == :undefined
  after
    Application.put_env(:postgrex, :type_server_reap_after, 3 * 60_000)
  end

  test "gives lock to another process if original holder crashes before fetch" do
    key = make_ref()

    task = Task.async(fn -> TS.fetch(key) end)
    assert {:lock, _ref, table1} = Task.await(task)
    wait_until_dead(task.pid)

    assert {:lock, _ref, table2} =
           Task.async(fn -> TS.fetch(key) end) |> Task.await
    assert table1 != table2
  end

  test "gives lock to another process if original holder crashes after fetch" do
    key = make_ref()
    top = self()

    {:ok, pid} = Task.start fn ->
      send(top, TS.fetch(key))
      :timer.sleep(:infinity)
    end

    assert_receive {:lock, _, _}
    task = Task.async(fn -> TS.fetch(key) end)

    Process.exit(pid, :kill)
    wait_until_dead(pid)
    assert {:lock, _, _} = Task.await(task)
  end

  defp wait_until_dead(pid) do
    ref = Process.monitor(pid)
    receive do: ({:DOWN, ^ref, _, _, _} -> :ok)
  end
end
