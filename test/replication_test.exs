defmodule ReplicationTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureLog
  alias Postgrex, as: P
  alias Postgrex.Replication, as: PR

  @timeout 2000
  @moduletag :logical_replication
  @moduletag {:min_pg_version, "10.0"}

  defmodule Repl do
    use Postgrex.Replication

    def start_link({pid, opts}) do
      Postgrex.Replication.start_link(__MODULE__, pid, opts)
    end

    @impl true
    def init(pid) do
      {:ok, pid}
    end

    @impl true
    def handle_data(<<?k, wal_end::64, _clock::64, _reply>> = msg, pid) do
      send(pid, msg)
      reply = <<?r, wal_end + 1::64, wal_end + 1::64, wal_end + 1::64, current_time()::64, 0>>
      {:noreply, [reply], pid}
    end

    def handle_data(msg, pid) do
      send(pid, msg)
      {:noreply, [], pid}
    end

    @impl true
    def handle_info(:ping, pid) do
      send(pid, :pong)
      {:noreply, [], pid}
    end

    @impl true
    def handle_call(:ping, from, pid) do
      Postgrex.Replication.reply(from, :pong)
      {:noreply, [], pid}
    end

    @epoch DateTime.to_unix(~U[2000-01-01 00:00:00Z], :microsecond)
    defp current_time(), do: System.os_time(:microsecond) - @epoch
  end

  @opts [
    database: "postgrex_test",
    backoff_type: :stop,
    max_restarts: 0
  ]

  @repl_opts [
    slot: "postgrex_example",
    logical: {:pgoutput, proto_version: 1, publication_names: "postgrex_example"},
    temporary: true,
    snapshot: :noexport
  ]

  setup do
    repl = start_supervised!({Repl, {self(), @repl_opts ++ @opts}})
    {:ok, repl: repl}
  end

  test "handle_call", context do
    assert PR.call(context.repl, :ping) == :pong
  end

  test "handle_info", context do
    send(context.repl, :ping)
    assert_receive :pong
  end

  test "handle_data" do
    assert_receive <<?k, _::64, _::64, _>>, @timeout
  end

  test "receives pgoutput" do
    pid = start_supervised!({P, @opts})
    P.query!(pid, "CREATE TABLE repl_test (id int, text text)", [])
    P.query!(pid, "INSERT INTO repl_test VALUES ($1, $2)", [42, "fortytwo"])

    assert_receive <<?w, _ws::64, _we::64, _ts1::64, ?B, _ls::64, _ts2::64, _xid::32>>,
                   @timeout

    assert_receive <<?w, _ws::64, _we::64, _ts1::64, ?I, _rid::32, ?N, _nc::16, _::binary>>,
                   @timeout

    assert_receive <<?w, _ws::64, _we::64, _ts1::64, ?C, _f, _ls::64, _le::64, _ts2::64>>,
                   @timeout
  end

  test "raises on bad config with sync_connect: true" do
    Process.flag(:trap_exit, true)
    {:error, {%Postgrex.Error{} = error, _}} = Repl.start_link({self(), @repl_opts ++ @opts})
    assert Exception.message(error) =~ "replication slot \"postgrex_example\" already exists"
  end

  test "raises on bad config with sync_connect: false" do
    Process.flag(:trap_exit, true)
    opts = [sync_connect: false] ++ @repl_opts ++ @opts
    msg = "replication slot \"postgrex_example\" already exists"

    assert capture_log(fn ->
             {:ok, pid} = Repl.start_link({self(), opts})
             ref = Process.monitor(pid)
             assert_receive {:DOWN, ^ref, _, _, {%Postgrex.Error{} = error, _}}
             assert Exception.message(error) =~ msg
           end) =~ msg
  end
end
