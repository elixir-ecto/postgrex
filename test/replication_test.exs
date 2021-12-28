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
      send(pid, {msg, System.unique_integer()})
      {:noreply, [], pid}
    end

    @impl true
    def handle_info(:ping, pid) do
      send(pid, :pong)
      {:noreply, pid}
    end

    def handle_info({:stream, query}, pid) do
      {:stream, query, [], pid}
    end

    def handle_info(_, pid) do
      {:noreply, pid}
    end

    @impl true
    def handle_call(:ping, from, pid) do
      Postgrex.Replication.reply(from, :pong)
      {:noreply, pid}
    end

    @impl true
    def handle_call({:query, query}, from, pid) do
      {:query, query, {from, pid}}
    end

    @impl true
    def handle_result(result, {from, pid}) do
      Postgrex.Replication.reply(from, {:ok, result})
      {:noreply, pid}
    end

    @epoch DateTime.to_unix(~U[2000-01-01 00:00:00Z], :microsecond)
    defp current_time(), do: System.os_time(:microsecond) - @epoch
  end

  @opts [
    database: "postgrex_test",
    backoff_type: :stop,
    max_restarts: 0
  ]

  setup context do
    repl = start_supervised!({Repl, {self(), Keyword.merge(@opts, context[:opts] || [])}})
    {:ok, repl: repl}
  end

  test "handle_call", context do
    assert PR.call(context.repl, :ping) == :pong
  end

  test "handle_info", context do
    send(context.repl, :ping)
    assert_receive :pong
  end

  describe "handle_result" do
    test "on result", context do
      assert {:ok, %Postgrex.Result{}} = PR.call(context.repl, {:query, "SELECT 1"})
    end

    test "on error", context do
      assert {:ok, %Postgrex.Error{}} = PR.call(context.repl, {:query, "SELCT"})
    end

    @tag :capture_log
    test "on disconnect", context do
      Process.flag(:trap_exit, true)
      :sys.suspend(context.repl)
      {_pid, ref} = spawn_monitor(fn -> PR.call(context.repl, {:query, "SELECT 1"}) end)
      disconnect(context.repl)
      :sys.resume(context.repl)
      assert_receive {:DOWN, ^ref, _, _, {%DBConnection.ConnectionError{}, _}}

      ref = Process.monitor(context.repl)
      assert_receive {:DOWN, ^ref, _, _, _}
    end
  end

  describe "auto-reconnect" do
    @tag opts: [auto_reconnect: true]
    test "on disconnect", context do
      :sys.suspend(context.repl)
      {_pid, ref} = spawn_monitor(fn -> PR.call(context.repl, {:query, "SELECT 1"}) end)
      disconnect(context.repl)
      :sys.resume(context.repl)
      assert_receive {:DOWN, ^ref, _, _, {%DBConnection.ConnectionError{}, _}}
      assert {:ok, %Postgrex.Result{}} = PR.call(context.repl, {:query, "SELECT 1"})
    end
  end

  describe "LSN" do
    test "encoding" do
      lsn_int = Enum.reduce(1..15, 0, &(&1 * pow(16, &1) + &2))
      {:ok, lsn_str} = PR.encode_lsn(lsn_int)
      {:ok, lsn_min} = PR.encode_lsn(0)
      {:ok, lsn_max} = PR.encode_lsn(@max_uint64)
      assert lsn_str == "FEDCBA98/76543210"
      assert lsn_min == "0/0"
      assert lsn_max == "FFFFFFFF/FFFFFFFF"
    end

    test "decoding" do
      lsn_str = "FEDCBA98/76543210"
      {:ok, lsn_int} = PR.decode_lsn(lsn_str)
      {:ok, lsn_min} = PR.decode_lsn("0/0")
      {:ok, lsn_max} = PR.decode_lsn("FFFFFFFF/FFFFFFFF")
      assert lsn_int == Enum.reduce(1..15, 0, &(&1 * pow(16, &1) + &2))
      assert lsn_min == 0
      assert lsn_max == @max_uint64
    end

    test "decoding then encoding" do
      lsn_str = "FEDCBA98/76543210"
      lsn_str_computed = lsn_str |> PR.decode_lsn() |> elem(1) |> PR.encode_lsn() |> elem(1)
      assert lsn_str == lsn_str_computed
    end

    test "encoding then decoding" do
      lsn_int = Enum.reduce(1..15, 0, &(&1 * pow(16, &1) + &2))
      lsn_int_computed = lsn_int |> PR.encode_lsn() |> elem(1) |> PR.decode_lsn() |> elem(1)
      assert lsn_int == lsn_int_computed
    end

    test "decode :error" do
      assert PR.decode_lsn("0123ABC") == :error
      assert PR.decode_lsn("/0123ABC") == :error
      assert PR.decode_lsn("0123ABC/") == :error
      assert PR.decode_lsn("123G/0123ABC") == :error
      assert PR.decode_lsn("0/012345678") == :error
      assert PR.decode_lsn("012345678/0") == :error
      assert PR.decode_lsn("-0FA23/08FACD1") == :error
      assert PR.decode_lsn("0FA23/-08FACD1") == :error
    end

    test "encode :error" do
      assert PR.encode_lsn(-1) == :error
      assert PR.encode_lsn(@max_uint64 + 1) == :error
    end
  end

  describe "handle_data" do
    setup do
      pid = start_supervised!({P, @opts}, id: :repl_conn)
      P.query!(pid, "CREATE TABLE IF NOT EXISTS repl_test (id int, text text)", [])

      on_exit(fn ->
        {:ok, pid} = P.start_link(@opts)
        P.query!(pid, "DROP TABLE IF EXISTS repl_test", [])
      end)

      {:ok, pid: pid}
    end

    test "on replication", context do
      start_replication(context.repl)
      assert_receive <<?k, _::64, _::64, _>>, @timeout
    end

    test "on replication with pgoutput", context do
      start_replication(context.repl)
      P.query!(context.pid, "INSERT INTO repl_test VALUES ($1, $2)", [42, "fortytwo"])

      assert_receive {<<?w, _ws::64, _we::64, _ts1::64, ?B, _ls::64, _ts2::64, _xid::32>>, _},
                     @timeout

      assert_receive {<<?w, _ws::64, _we::64, _ts1::64, ?I, _rid::32, ?N, _nc::16, _::binary>>,
                      _},
                     @timeout

      assert_receive {<<?w, _ws::64, _we::64, _ts1::64, ?C, _f, _ls::64, _le::64, _ts2::64>>, _},
                     @timeout
    end

    test "on copy", context do
      P.query!(context.pid, "INSERT INTO repl_test VALUES ($1, $2), ($3, $4)", [42, "42", 1, "1"])
      send(context.repl, {:stream, "COPY repl_test TO STDOUT"})
      assert_receive {"42\t42\n", i1}, @timeout
      assert_receive {"1\t1\n", i2}, @timeout
      assert_receive {:done, i3}, @timeout

      assert i1 < i2
      assert i2 < i3

      # Can query after copy is done
      {:ok, %Postgrex.Result{}} = PR.call(context.repl, {:query, "SELECT 1"})
    end
  end

  defp start_replication(repl) do
    PR.call(
      repl,
      {:query,
       "CREATE_REPLICATION_SLOT postgrex_test TEMPORARY LOGICAL pgoutput NOEXPORT_SNAPSHOT"}
    )

    send(
      repl,
      {:stream,
       "START_REPLICATION SLOT postgrex_test LOGICAL 0/0 (proto_version '1', publication_names 'postgrex_example')"}
    )
  end

  defp pow(base, exp) do
    :math.pow(base, exp) |> round()
  end

  defp disconnect(repl) do
    {:gen_tcp, sock} = :sys.get_state(repl).mod_state.protocol.sock
    :gen_tcp.shutdown(sock, :read_write)
  end
end
