defmodule TypeServerTest do
  use ExUnit.Case, async: true
  alias Postgrex.TypeSupervisor, as: TM
  alias Postgrex.TypeServer, as: TS

  @types Postgrex.DefaultTypes

  test "fetches and unlocks" do
    key = make_ref()
    server = TM.locate(@types, key)
    assert {:lock, ref, {@types, table}} = TS.fetch(server)
    assert :ets.info(table, :name) == Postgrex.Types
    assert TS.update(server, ref, []) == :ok
  end

  test "fetches and exits" do
    key = make_ref()
    server = TM.locate(@types, key)
    task = Task.async(fn -> assert {:lock, _, _} = TS.fetch(server) end)
    {:lock, _, types} = Task.await(task)
    assert ^server = TM.locate(@types, key)
    assert {:lock, _, ^types} = TS.fetch(server)
  end

  test "blocks on initial fetch until update returns lock" do
    key = make_ref()
    server = TM.locate(@types, key)
    {:lock, ref, types} = TS.fetch(server)

    task = Task.async(fn -> TS.fetch(server) end)
    :timer.sleep(100)
    TS.update(server, ref, [])
    assert {:lock, _, ^types} = Task.await(task)
  end

  test "blocks on later fetch until update returns lock" do
    key = make_ref()
    server = TM.locate(@types, key)
    {:lock, ref, types} = TS.fetch(server)
    TS.update(server, ref, [])

    assert {:lock, ref, ^types} = TS.fetch(server)

    task = Task.async(fn -> TS.fetch(server) end)
    :timer.sleep(100)
    assert Task.yield(task, 0) == nil
    TS.update(server, ref, [])
    assert {:lock, _, ^types} = Task.await(task)
  end

  test "blocks on initial fetch until done returns lock" do
    key = make_ref()
    server = TM.locate(@types, key)
    {:lock, ref, types} = TS.fetch(server)

    task = Task.async(fn -> TS.fetch(server) end)
    :timer.sleep(100)
    assert Task.yield(task, 0) == nil
    TS.done(server, ref)
    assert {:lock, _, ^types} = Task.await(task)
  end

  test "blocks on later fetch until done returns lock" do
    key = make_ref()
    server = TM.locate(@types, key)
    {:lock, ref, types} = TS.fetch(server)
    TS.update(server, ref, [])

    {:lock, ref, ^types} = TS.fetch(server)

    task = Task.async(fn -> TS.fetch(server) end)
    :timer.sleep(100)
    assert Task.yield(task, 0) == nil
    TS.done(server, ref)
    assert {:lock, _, ^types} = Task.await(task)
  end

  test "fetches existing table even if parent crashes" do
    key = make_ref()
    server = TM.locate(@types, key)

    task =
      Task.async(fn ->
        {:lock, ref, types} = TS.fetch(server)
        TS.update(server, ref, [])
        types
      end)

    types = Task.await(task)
    wait_until_dead(task.pid)

    task = Task.async(fn -> TS.fetch(server) end)
    assert {:lock, _, ^types} = Task.await(task)
  end

  test "the lock is granted to single process one by one" do
    key = make_ref()
    server = TM.locate(@types, key)

    {:lock, ref, types} = TS.fetch(server)
    TS.update(server, ref, [])

    parent = self()

    task = fn ->
      result = TS.fetch(server)
      send(parent, {self(), result})

      case result do
        {:lock, ref2, _} ->
          assert_receive {^parent, :go}
          TS.update(server, ref2, [])

        _ ->
          :ok
      end
    end

    {:ok, _} = Task.start_link(task)
    {:ok, _} = Task.start_link(task)
    {:ok, _} = Task.start_link(task)

    for _ <- 1..3 do
      assert_receive {pid, {:lock, _, ^types}}
      :timer.sleep(100)
      :sys.get_state(server)
      refute_received _
      send(pid, {parent, :go})
    end

    assert {:lock, _, ^types} = TS.fetch(server)
  end

  test "does not fetch existing table if parent crashes and timeout passes" do
    Application.put_env(:postgrex, :type_server_reap_after, 0)
    key = make_ref()

    task =
      Task.async(fn ->
        server = TM.locate(@types, key)
        {:lock, ref, types} = TS.fetch(server)
        TS.update(server, ref, [])
        {server, types}
      end)

    {server1, types1} = Task.await(task)
    mref = Process.monitor(server1)
    assert_receive {:DOWN, ^mref, _, _, _}

    server2 = TM.locate(@types, key)
    refute server1 == server2

    task = Task.async(fn -> TS.fetch(server2) end)
    assert {:lock, _, types2} = Task.await(task)

    assert types1 != types2
    assert {_, table1} = types1
    assert :ets.info(table1, :name) == :undefined
  after
    Application.put_env(:postgrex, :type_server_reap_after, 3 * 60_000)
  end

  test "gives lock to another process if original holder crashes before fetch" do
    key = make_ref()
    server = TM.locate(@types, key)

    task = Task.async(fn -> TS.fetch(server) end)
    assert {:lock, _ref, types} = Task.await(task)
    wait_until_dead(task.pid)

    assert {:lock, _ref, ^types} = Task.async(fn -> TS.fetch(server) end) |> Task.await()
  end

  test "error waiting process if original holder crashes after fetch" do
    key = make_ref()
    server = TM.locate(@types, key)
    top = self()

    {:ok, pid} =
      Task.start(fn ->
        send(top, TS.fetch(server))
        :timer.sleep(:infinity)
      end)

    assert_receive {:lock, _, types}
    task = Task.async(fn -> TS.fetch(server) end)

    Process.exit(pid, :kill)
    wait_until_dead(pid)
    assert {:lock, _, ^types} = Task.await(task)
  end

  defp wait_until_dead(pid) do
    ref = Process.monitor(pid)
    receive do: ({:DOWN, ^ref, _, _, _} -> :ok)
  end
end
