defmodule ReplicationTest do
  use ExUnit.Case, async: true
  alias Postgrex, as: P
  alias Postgrex.Replication, as: PR

  @timeout 2000
  @moduletag :logical_replication
  @moduletag min_pg_version: "10.0"

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

  @repl_opts %{
    slot: "postgrex_example",
    plugin: :pgoutput,
    plugin_opts: [proto_version: 1, publication_names: "postgrex_example"]
  }

  setup do
    repl = start_supervised!({Repl, {self(), @opts}})
    {:ok, repl: repl}
  end

  test "handle_call", context do
    assert PR.call(context.repl, :ping) == :pong
  end

  test "handle_info", context do
    send(context.repl, :ping)
    assert_receive :pong
  end

  test "handle_data", context do
    start_replication(context.repl)
    assert_receive <<?k, _::64, _::64, _>>, @timeout
  end

  test "receives pgoutput", context do
    start_replication(context.repl)
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

  test "create slot returns results", context do
    %{slot: slot, plugin: plugin} = @repl_opts
    {:ok, %Postgrex.Result{} = result} = PR.create_slot(context.repl, slot, plugin)
    assert result.num_rows == 1
    assert result.columns == ["slot_name", "consistent_point", "snapshot_name", "output_plugin"]
    assert [[_, _, _, _]] = result.rows
  end

  test "can't create same slot twice", context do
    %{slot: slot, plugin: plugin} = @repl_opts
    {:ok, %Postgrex.Result{}} = PR.create_slot(context.repl, slot, plugin)
    {:error, %Postgrex.Error{} = error} = PR.create_slot(context.repl, slot, plugin)
    assert Exception.message(error) =~ "replication slot \"postgrex_example\" already exists"
  end

  test "can't drop a slot that doesn't exist", context do
    %{slot: slot} = @repl_opts
    {:error, %Postgrex.Error{} = error} = PR.drop_slot(context.repl, slot)
    assert Exception.message(error) =~ "replication slot \"postgrex_example\" does not exist"
  end

  test "can't run other commands after replication has started", context do
    start_replication(context.repl)
    assert {:error, :replication_started} == PR.create_slot(context.repl, "slot", :plugin)
    assert {:error, :replication_started} == PR.drop_slot(context.repl, "slot")
    assert {:error, :replication_started} == PR.start_replication(context.repl, "slot")
  end

  test "drop_slot with wait = false returns an error when being used by a connection", context do
    %{slot: slot} = @repl_opts
    start_replication(context.repl)
    repl1 = start_supervised!({Repl, {self(), @opts}}, id: :repl1)
    {:error, %Postgrex.Error{} = error} = PR.drop_slot(repl1, slot, wait: false)
    assert Exception.message(error) =~ "replication slot \"postgrex_example\" is active for PID"
  end

  test "identify system returns values", context do
    {:ok, %Postgrex.Result{} = result} = PR.identify_system(context.repl)
    assert result.num_rows == 1
    assert result.columns == ["systemid", "timeline", "xlogpos", "dbname"]
    assert [[_, _, _, _]] = result.rows
  end

  test "show returns values", context do
    {:ok, %Postgrex.Result{} = result} = PR.show(context.repl, "SERVER_VERSION")
    assert result.num_rows == 1
    assert result.columns == ["server_version"]
    assert [[_]] = result.rows
  end

  defp start_replication(repl) do
    %{slot: slot, plugin: plugin, plugin_opts: plugin_opts} = @repl_opts
    {:ok, %Postgrex.Result{}} = PR.create_slot(repl, slot, plugin)
    :ok = PR.start_replication(repl, slot, plugin_opts: plugin_opts)
  end
end
