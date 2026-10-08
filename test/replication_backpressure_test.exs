defmodule Postgrex.ReplicationBackpressureTest do
  use ExUnit.Case, async: false

  alias Postgrex.ReplicationConnection, as: Connection

  @moduletag :logical_replication

  defmodule Receiver do
    use Postgrex.ReplicationConnection

    def start_link(opts) do
      {owner, opts} = Keyword.pop!(opts, :owner)
      Connection.start_link(__MODULE__, owner, opts)
    end

    @impl true
    def init(owner), do: {:ok, owner}

    @impl true
    def handle_connect(owner) do
      send(owner, {:connected, self()})
      {:noreply, owner}
    end

    @impl true
    def handle_call({:query, sql}, from, owner), do: {:query, sql, {from, owner}}

    def handle_call({:stream, sql, batch_size}, from, owner) do
      Connection.reply(from, :ok)
      {:stream, sql, [max_messages: batch_size], owner}
    end

    def handle_call(:ping, from, owner) do
      Connection.reply(from, :pong)
      {:noreply, owner}
    end

    @impl true
    def handle_result(result, {from, owner}) do
      Connection.reply(from, result)
      {:noreply, owner}
    end

    @impl true
    def handle_data(:done, owner) do
      send(owner, :done)
      {:noreply, owner}
    end

    def handle_data(<<?k, _::binary>>, owner), do: {:noreply, owner}

    def handle_data(data, owner) do
      send(owner, {:data, data})
      {:pause, owner}
    end

    @impl true
    def handle_info(:resume, owner), do: {:resume, owner}
    def handle_info(:disconnect, _owner), do: {:disconnect, :test_disconnect}

    def handle_info(:resume_with_feedback, owner) do
      {:noreply, replies, owner} = handle_info(:heartbeat, owner)
      {:resume, replies, owner}
    end

    def handle_info(:heartbeat, owner) do
      clock = System.os_time(:microsecond) - 946_684_800_000_000
      {:noreply, [<<?r, 0::64, 0::64, 0::64, clock::64, 0>>], owner}
    end
  end

  for transport <- [:tcp, :ssl] do
    @tag transport: transport
    if transport == :ssl, do: @tag(:ssl)

    test "pauses and resumes ordered COPY data over #{transport}", %{transport: transport} do
      opts = if transport == :ssl, do: [ssl: [verify: :verify_none]], else: []
      receiver = start_supervised!({Receiver, [owner: self(), database: "postgrex_test"] ++ opts})

      assert :ok =
               Connection.call(
                 receiver,
                 {:stream, "COPY (SELECT generate_series(1, 20)) TO STDOUT", 3}
               )

      for expected <- 1..20 do
        assert_receive {:data, data}
        assert data == "#{expected}\n"
        assert Connection.call(receiver, :ping) == :pong
        {:no_state, state} = :sys.get_state(receiver)
        assert state.paused
        assert length(state.pending) <= 2
        assert is_binary(state.protocol.buffer)
        refute_receive {:data, _}, 10
        send(receiver, :resume)
      end

      assert_receive :done
      assert [%Postgrex.Result{rows: [["1"]]}] = Connection.call(receiver, {:query, "SELECT 1"})
    end
  end

  test "paused logical replication keeps slot ownership and can send feedback" do
    receiver = start_supervised!({Receiver, owner: self(), database: "postgrex_test"})
    slot = "postgrex_pause_#{System.unique_integer([:positive])}"

    assert [%Postgrex.Result{}] =
             Connection.call(
               receiver,
               {:query,
                "CREATE_REPLICATION_SLOT #{slot} TEMPORARY LOGICAL pgoutput NOEXPORT_SNAPSHOT"}
             )

    assert :ok =
             Connection.call(
               receiver,
               {:stream,
                "START_REPLICATION SLOT #{slot} LOGICAL 0/0 (proto_version '1', publication_names 'postgrex_example', messages 'true')",
                2}
             )

    writer = start_supervised!({Postgrex, database: "postgrex_test"})
    Postgrex.query!(writer, "SELECT pg_logical_emit_message(false, 'pause_test', 'first')", [])
    assert_receive {:data, <<?w, _::binary>>}

    [[backend_pid]] =
      Postgrex.query!(
        writer,
        "SELECT active_pid FROM pg_replication_slots WHERE slot_name = $1",
        [slot]
      ).rows

    assert is_integer(backend_pid)

    send(receiver, :heartbeat)
    assert Connection.call(receiver, :ping) == :pong
    Postgrex.query!(writer, "SELECT pg_logical_emit_message(false, 'pause_test', 'second')", [])
    refute_receive {:data, _}, 100

    assert [[^backend_pid]] =
             Postgrex.query!(
               writer,
               "SELECT active_pid FROM pg_replication_slots WHERE slot_name = $1",
               [slot]
             ).rows

    send(receiver, :resume_with_feedback)
    assert_receive {:data, <<?w, _::binary>>}
  end

  @tag :capture_log
  test "reconnection discards paused messages from the old connection" do
    receiver =
      start_supervised!(
        {Receiver, owner: self(), database: "postgrex_test", auto_reconnect: true}
      )

    assert_receive {:connected, ^receiver}

    assert :ok =
             Connection.call(
               receiver,
               {:stream, "COPY (SELECT generate_series(1, 20)) TO STDOUT", 3}
             )

    assert_receive {:data, "1\n"}
    {:no_state, state} = :sys.get_state(receiver)
    assert state.paused

    send(receiver, :disconnect)
    assert_receive {:connected, ^receiver}
    {:no_state, state} = :sys.get_state(receiver)
    refute state.paused
    assert state.pending == []
    assert state.streaming == nil
    assert [%Postgrex.Result{rows: [["1"]]}] = Connection.call(receiver, {:query, "SELECT 1"})
  end
end
