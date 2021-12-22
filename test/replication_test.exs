defmodule ReplicationTest do
  use ExUnit.Case, async: true
  alias Postgrex, as: P
  alias Postgrex.Replication, as: PR

  @timeout 2000
  @max_uint64 18_446_744_073_709_551_615
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
    def handle_copy(msg, pid) do
      send(pid, msg)
      {:noreply, [], pid}
    end

    @impl true
    def handle_info(:ping, pid) do
      send(pid, :pong)
      {:noreply, [], pid}
    end

    @impl true
    def handle_info(_, pid) do
      {:noreply, [], pid}
    end

    @impl true
    def handle_call(:ping, from, pid) do
      Postgrex.Replication.reply(from, :pong)
      {:noreply, [], pid}
    end

    @impl true
    def handle_call({:query, query}, from, pid) do
      {:query, query, {from, pid}}
    end

    @impl true
    def handle_result(%Postgrex.Result{} = result, {from, pid}) do
      Postgrex.Replication.reply(from, {:ok, result})
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
    table_name: "repl_test",
    slot_name: "postgrex_example",
    plugin: :pgoutput,
    plugin_opts: [proto_version: 1, publication_names: "postgrex_example"],
    create_temporary_slot: [slot_name: "postgrex_example", plugin: :pgoutput, snapshot: :export]
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

  test "encodes LSN" do
    lsn_int = Enum.reduce(1..15, 0, &(&1 * pow(16, &1) + &2))
    {:ok, lsn_str} = PR.encode_lsn(lsn_int)
    {:ok, lsn_min} = PR.encode_lsn(0)
    {:ok, lsn_max} = PR.encode_lsn(@max_uint64)
    assert lsn_str == "FEDCBA98/76543210"
    assert lsn_min == "0/0"
    assert lsn_max == "FFFFFFFF/FFFFFFFF"
  end

  test "decodes LSN" do
    lsn_str = "FEDCBA98/76543210"
    {:ok, lsn_int} = PR.decode_lsn(lsn_str)
    {:ok, lsn_min} = PR.decode_lsn("0/0")
    {:ok, lsn_max} = PR.decode_lsn("FFFFFFFF/FFFFFFFF")
    assert lsn_int == Enum.reduce(1..15, 0, &(&1 * pow(16, &1) + &2))
    assert lsn_min == 0
    assert lsn_max == @max_uint64
  end

  test "decode then encode LSN returns original" do
    lsn_str = "FEDCBA98/76543210"
    lsn_str_computed = lsn_str |> PR.decode_lsn() |> elem(1) |> PR.encode_lsn() |> elem(1)
    assert lsn_str == lsn_str_computed
  end

  test "encode then decode LSN returns original" do
    lsn_int = Enum.reduce(1..15, 0, &(&1 * pow(16, &1) + &2))
    lsn_int_computed = lsn_int |> PR.encode_lsn() |> elem(1) |> PR.decode_lsn() |> elem(1)
    assert lsn_int == lsn_int_computed
  end

  test "decode invalid LSN returns :error" do
    assert PR.decode_lsn("0123ABC") == :error
    assert PR.decode_lsn("/0123ABC") == :error
    assert PR.decode_lsn("0123ABC/") == :error
    assert PR.decode_lsn("123G/0123ABC") == :error
    assert PR.decode_lsn("0/012345678") == :error
    assert PR.decode_lsn("012345678/0") == :error
    assert PR.decode_lsn("-0FA23/08FACD1") == :error
    assert PR.decode_lsn("0FA23/-08FACD1") == :error
  end

  test "encode invalid LSN returns :error" do
    assert PR.encode_lsn(-1) == :error
    assert PR.encode_lsn(@max_uint64 + 1) == :error
  end

  describe "replication" do
    setup do
      %{table_name: table} = @repl_opts
      repl = start_supervised!({Repl, {self(), @opts}}, id: :repl_repl)

      pid = start_supervised!({P, @opts}, id: :repl_conn)
      P.query!(pid, "CREATE TABLE IF NOT EXISTS #{table} (id int, text text)", [])

      on_exit(fn ->
        {:ok, pid} = P.start_link(@opts)
        P.query!(pid, "DROP TABLE IF EXISTS #{table}", [])
      end)

      {:ok, pid: pid, repl: repl}
    end

    test "handle_data", context do
      start_replication(context.repl)
      assert_receive <<?k, _::64, _::64, _>>, @timeout
    end

    test "receives pgoutput with slot_name", context do
      %{table_name: table} = @repl_opts
      start_replication(context.repl)
      P.query!(context.pid, "INSERT INTO #{table} VALUES ($1, $2)", [42, "fortytwo"])

      assert_receive <<?w, _ws::64, _we::64, _ts1::64, ?B, _ls::64, _ts2::64, _xid::32>>,
                     @timeout

      assert_receive <<?w, _ws::64, _we::64, _ts1::64, ?I, _rid::32, ?N, _nc::16, _::binary>>,
                     @timeout

      assert_receive <<?w, _ws::64, _we::64, _ts1::64, ?C, _f, _ls::64, _le::64, _ts2::64>>,
                     @timeout
    end

    test "receives pgoutput with create_temporary_slot", context do
      %{table_name: table} = @repl_opts
      start_replication(context.repl, :create_temporary_slot)
      P.query!(context.pid, "INSERT INTO #{table} VALUES ($1, $2)", [42, "fortytwo"])

      assert_receive <<?w, _ws::64, _we::64, _ts1::64, ?B, _ls::64, _ts2::64, _xid::32>>,
                     @timeout

      assert_receive <<?w, _ws::64, _we::64, _ts1::64, ?I, _rid::32, ?N, _nc::16, _::binary>>,
                     @timeout

      assert_receive <<?w, _ws::64, _we::64, _ts1::64, ?C, _f, _ls::64, _le::64, _ts2::64>>,
                     @timeout
    end

    test "cannot start replication on a slot_name that doesn't exist", context do
      {:error, %Postgrex.Error{} = error} =
        PR.start_replication(context.repl, slot_name: "not_a_slot")

      assert Exception.message(error) =~ "replication slot \"not_a_slot\" does not exist"
    end

    test "must provide slot_name or create_temporary_slot", context do
      message = "expected one of :slot_name or :create_temporary slot"
      assert_raise ArgumentError, message, fn -> PR.start_replication(context.repl) end
    end

    test "must provide all create_temporary_slot keys", context do
      message =
        "expected :slot_name, :plugin and :snapshot to be provided in :create_temporary_slot"

      temp_slot = []

      assert_raise ArgumentError, message, fn ->
        PR.start_replication(context.repl, create_temporary_slot: temp_slot)
      end

      temp_slot = [slot_name: "slot_name"]

      assert_raise ArgumentError, message, fn ->
        PR.start_replication(context.repl, create_temporary_slot: temp_slot)
      end

      temp_slot = [plugin: "plugin"]

      assert_raise ArgumentError, message, fn ->
        PR.start_replication(context.repl, create_temporary_slot: temp_slot)
      end

      temp_slot = [snapshot: "snapshot"]

      assert_raise ArgumentError, message, fn ->
        PR.start_replication(context.repl, create_temporary_slot: temp_slot)
      end

      temp_slot = [slot_name: "slot_name", plugin: "plugin"]

      assert_raise ArgumentError, message, fn ->
        PR.start_replication(context.repl, create_temporary_slot: temp_slot)
      end

      temp_slot = [slot_name: "slot_name", snapshot: "snapshot"]

      assert_raise ArgumentError, message, fn ->
        PR.start_replication(context.repl, create_temporary_slot: temp_slot)
      end

      temp_slot = [plugin: "plugin", snapshot: "snapshot"]

      assert_raise ArgumentError, message, fn ->
        PR.start_replication(context.repl, create_temporary_slot: temp_slot)
      end
    end

    test "can't run other commands after replication has started", context do
      start_replication(context.repl)

      assert {:error, :stream_in_progress} ==
               PR.start_replication(context.repl, slot_name: "slot")

      assert {:error, :stream_in_progress} ==
               PR.copy_table(context.repl, "table", "slot", :plugin)

      assert {:error, :stream_in_progress} ==
               PR.call(context.repl, {:query, "SHOW server_version_num"})
    end
  end

  describe "copy_table" do
    setup do
      repl = start_supervised!({Repl, {self(), @opts}}, id: :copy_repl)

      pid = start_supervised!({P, @opts}, id: :copy_conn)
      P.query!(pid, "CREATE TABLE IF NOT EXISTS #{@repl_opts.table_name} (id int, text text)", [])

      on_exit(fn ->
        {:ok, pid} = P.start_link(@opts)
        P.query!(pid, "DROP TABLE IF EXISTS #{@repl_opts.table_name}", [])
      end)

      {:ok, pid: pid, repl: repl}
    end

    test "copy table", context do
      %{table_name: table, slot_name: slot, plugin: plugin} = @repl_opts
      P.query!(context.pid, "INSERT INTO #{table} VALUES ($1, $2), ($3, $4)", [42, "42", 1, "1"])
      {:ok, _} = PR.copy_table(context.repl, table, slot, plugin)
      assert_receive %Postgrex.Result{columns: ["id", "text"], rows: [["42", "42"]]}, @timeout
      assert_receive %Postgrex.Result{columns: ["id", "text"], rows: [["1", "1"]]}, @timeout
      assert_receive {:copy_done, ^table}, @timeout
    end

    test "can issue commands after copying is finished", context do
      %{table_name: table, slot_name: slot, plugin: plugin} = @repl_opts
      {:ok, _} = PR.copy_table(context.repl, table, slot, plugin)
      assert_receive {:copy_done, ^table}, @timeout
      assert {:ok, _} = PR.call(context.repl, {:query, "SHOW server_version_num"})
    end

    test "can start replication after copying is finished", context do
      %{table_name: table, plugin: plugin} = @repl_opts
      {:ok, _} = PR.copy_table(context.repl, table, "repl_slot", plugin)
      assert_receive {:copy_done, ^table}, @timeout
      start_replication(context.repl)
      P.query!(context.pid, "INSERT INTO #{table} VALUES ($1, $2)", [42, "fortytwo"])

      assert_receive <<?w, _ws::64, _we::64, _ts1::64, ?B, _ls::64, _ts2::64, _xid::32>>,
                     @timeout

      assert_receive <<?w, _ws::64, _we::64, _ts1::64, ?I, _rid::32, ?N, _nc::16, _::binary>>,
                     @timeout

      assert_receive <<?w, _ws::64, _we::64, _ts1::64, ?C, _f, _ls::64, _le::64, _ts2::64>>,
                     @timeout
    end
  end

  describe "auto-reconnect" do
    setup do
      opts = Keyword.put(@opts, :auto_reconnect, true)
      repl = start_supervised!({Repl, {self(), opts}}, id: :reconnect_repl)

      pid = start_supervised!({P, @opts}, id: :reconnect_conn)
      P.query!(pid, "CREATE TABLE IF NOT EXISTS repl_test (id int, text text)", [])

      on_exit(fn ->
        {:ok, pid} = P.start_link(@opts)
        P.query!(pid, "DROP TABLE IF EXISTS repl_test", [])

        {:ok, repl} = PR.start_link(Repl, self(), @opts)
        drop_slot(repl, @repl_opts.slot_name)
      end)

      {:ok, pid: pid, repl: repl}
    end

    test "replication with permanent slot auto-reconnects", context do
      %{slot_name: slot, plugin_opts: plugin_opts} = @repl_opts
      {:ok, %Postgrex.Result{}} = create_slot(context.repl, slot, "LOGICAL")
      {:ok, nil} = PR.start_replication(context.repl, slot_name: slot, plugin_opts: plugin_opts)
      disconnect(context.repl)

      # allow time for the process to reconnect
      :timer.sleep(500)
      P.query!(context.pid, "INSERT INTO repl_test VALUES ($1, $2)", [42, "fortytwo"])

      assert_receive <<?w, _ws::64, _we::64, _ts1::64, ?B, _ls::64, _ts2::64, _xid::32>>,
                     @timeout
    end

    test "replication with temporary slot auto-reconnects", context do
      %{create_temporary_slot: temp_slot, plugin_opts: plugin_opts} = @repl_opts

      {:ok, %Postgrex.Result{}} =
        PR.start_replication(context.repl,
          create_temporary_slot: temp_slot,
          plugin_opts: plugin_opts
        )

      disconnect(context.repl)

      # allow time for the process to reconnect
      :timer.sleep(500)
      P.query!(context.pid, "INSERT INTO repl_test VALUES ($1, $2)", [42, "fortytwo"])

      assert_receive <<?w, _ws::64, _we::64, _ts1::64, ?B, _ls::64, _ts2::64, _xid::32>>,
                     @timeout
    end
  end

  defp start_replication(repl, create_slot_method \\ :create_temporary_slot)

  defp start_replication(repl, :slot_name) do
    %{slot_name: slot, plugin_opts: plugin_opts} = @repl_opts
    {:ok, %Postgrex.Result{}} = create_slot(repl, slot, "TEMPORARY LOGICAL")
    {:ok, nil} = PR.start_replication(repl, slot_name: slot, plugin_opts: plugin_opts)
  end

  defp start_replication(repl, :create_temporary_slot) do
    %{create_temporary_slot: temp_slot, plugin_opts: plugin_opts} = @repl_opts

    {:ok, %Postgrex.Result{}} =
      PR.start_replication(repl, create_temporary_slot: temp_slot, plugin_opts: plugin_opts)
  end

  defp create_slot(repl, slot, type) do
    PR.call(repl, {:query, "CREATE_REPLICATION_SLOT #{slot} #{type} pgoutput NOEXPORT_SNAPSHOT"})
  end

  defp drop_slot(repl, slot) do
    PR.call(repl, {:query, "DROP_REPLICATION_SLOT #{slot}"})
  end

  defp pow(base, exp) do
    :math.pow(base, exp) |> round()
  end

  defp disconnect(repl) do
    {:gen_tcp, sock} = :sys.get_state(repl).mod_state.protocol.sock
    :gen_tcp.shutdown(sock, :read_write)
  end
end
