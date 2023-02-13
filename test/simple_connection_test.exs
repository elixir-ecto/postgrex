defmodule SimpleConnectionTest do
  use ExUnit.Case, async: true

  alias Postgrex.SimpleConnection, as: SC

  defmodule Conn do
    @behaviour Postgrex.SimpleConnection

    @impl true
    def init(pid) do
      {:ok, %{from: nil, pid: pid}}
    end

    @impl true
    def notify(channel, payload, state) do
      send(state.pid, {channel, payload})

      :ok
    end

    @impl true
    def handle_connect(state) do
      state.from && Postgrex.SimpleConnection.reply(state.from, :reconnecting)

      send(state.pid, {:connect, System.unique_integer([:monotonic])})

      {:noreply, state}
    end

    @impl true
    def handle_disconnect(state) do
      send(state.pid, {:disconnect, System.unique_integer([:monotonic])})

      {:noreply, state}
    end

    @impl true
    def handle_call(:ping, from, state) do
      Postgrex.SimpleConnection.reply(from, :pong)

      {:noreply, state}
    end

    def handle_call({:query, query}, from, state) do
      {:query, query, %{state | from: from}}
    end

    @impl true
    def handle_info(:ping, state) do
      send(state.pid, :pong)

      {:noreply, state}
    end

    @impl true
    def handle_result(result, %{from: from} = state) do
      Postgrex.SimpleConnection.reply(from, {:ok, result})

      {:noreply, state}
    end
  end

  @opts [database: "postgrex_test", sync_connect: true, auto_reconnect: false]

  setup context do
    opts = Keyword.merge(@opts, context[:opts] || [])
    conn = start_supervised!({SC, [Conn, self(), opts]})

    {:ok, conn: conn}
  end

  describe "handle_call/3" do
    test "forwarding calls to the callback module", context do
      assert :pong == SC.call(context.conn, :ping)
    end
  end

  describe "handle_info/2" do
    test "forwarding unknown messages to the callback module", context do
      send(context.conn, :ping)

      assert_receive :pong
    end
  end

  describe "handle_result/2" do
    test "relaying query results", context do
      assert {:ok, [%Postgrex.Result{}]} = SC.call(context.conn, {:query, "SELECT 1"})
    end

    test "relaying multi-statement query results", context do
      assert {:ok, [%Postgrex.Result{} = result1, %Postgrex.Result{} = result2]} =
               SC.call(context.conn, {:query, "SELECT 1; SELECT 2;"})

      assert result1.rows == [["1"]]
      assert result1.num_rows == 1
      assert result2.rows == [["2"]]
      assert result2.num_rows == 1
    end

    test "relaying query errors", context do
      assert {:ok, %Postgrex.Error{}} = SC.call(context.conn, {:query, "SELCT"})
    end
  end

  describe "notify/3" do
    test "relaying pubsub notifications", context do
      assert {:ok, _result} = SC.call(context.conn, {:query, ~s(LISTEN "channel")})
      assert {:ok, _result} = SC.call(context.conn, {:query, ~s(NOTIFY "channel", 'hello')})

      assert_receive {"channel", "hello"}
    end
  end

  describe "auto-reconnect" do
    @tag opts: [auto_reconnect: true]
    test "disconnect and connect handlers are invoked on reconnection", context do
      assert_receive {:connect, i1}

      :sys.suspend(context.conn)

      :erlang.trace(:all, true, [:call])
      match_spec = [{:_, [], [{:return_trace}]}]
      :erlang.trace_pattern({SC, :_, :_}, match_spec, [:local])

      task =
        Task.async(fn ->
          SC.call(context.conn, {:query, "SELECT 1"})
        end)

      # Make sure that the task "launched" the call before disconnecting and resuming
      # the process.
      assert_receive {:trace, _pid, :call, {SC, :call, [_, {:query, "SELECT 1"}]}}

      disconnect(context.conn)
      :sys.resume(context.conn)

      assert {:ok, [%Postgrex.Result{}]} = SC.call(context.conn, {:query, "SELECT 2"})
      assert :reconnecting == Task.await(task)
      assert_receive {:disconnect, i2} when i1 < i2
      assert_receive {:connect, i3} when i2 < i3
    after
      :erlang.trace_pattern({SC, :_, :_}, false, [])
      :erlang.trace(:all, false, [:call])
    end

    @tag capture_log: true
    test "disconnect handler is invoked and the process stays down", context do
      assert_receive {:connect, _}

      Process.flag(:trap_exit, true)

      :sys.suspend(context.conn)
      {_pid, ref} = spawn_monitor(fn -> SC.call(context.conn, {:query, "SELECT 1"}) end)
      disconnect(context.conn)
      :sys.resume(context.conn)

      assert_receive {:DOWN, ^ref, _, _, _}
      assert_received {:disconnect, _}

      ref = Process.monitor(context.conn)
      assert_receive {:DOWN, ^ref, _, _, _}

      refute_received {:connect, _}
    end
  end

  defp disconnect(conn) do
    {_, state} = :sys.get_state(conn)
    {:gen_tcp, sock} = state.protocol.sock
    :gen_tcp.shutdown(sock, :read_write)
  end
end
