defmodule NotificationTest do
  use ExUnit.Case, async: true
  import Postgrex.TestHelper
  alias Postgrex, as: P
  alias Postgrex.Notifications, as: PN

  @opts [database: "postgrex_test", sync_connect: true]

  setup do
    {:ok, pid} = P.start_link(@opts)
    {:ok, pid_ps} = PN.start_link(@opts)
    {:ok, [pid: pid, pid_ps: pid_ps]}
  end

  test "listening", context do
    assert {:ok, _} = PN.listen(context.pid_ps, "channel")
  end

  test "notifying", context do
    assert :ok = query("NOTIFY channel", [])
  end

  @tag requires_notify_payload: true
  test "listening, notify, then receive (with payload)", context do
    assert {:ok, ref} = PN.listen(context.pid_ps, "channel")

    assert {:ok, %Postgrex.Result{command: :notify}} = P.query(context.pid, "NOTIFY channel, 'hello'", [])
    receiver_pid = context.pid_ps
    assert_receive {:notification, ^receiver_pid, ^ref, "channel", "hello"}
  end

  test "listening, notify, then receive (without payload)", context do
    assert {:ok, ref} = PN.listen(context.pid_ps, "channel")

    assert {:ok, %Postgrex.Result{command: :notify}} = P.query(context.pid, "NOTIFY channel", [])
    receiver_pid = context.pid_ps
    assert_receive {:notification, ^receiver_pid, ^ref, "channel", ""}
  end

  test "listening, notify, then receive (using registered names)", _context do
    {:ok, _} = P.start_link(Keyword.put(@opts, :name, :client))
    {:ok, _} = PN.start_link(Keyword.put(@opts, :name, :notifications))
    assert {:ok, ref} = PN.listen(:notifications, "channel")

    assert {:ok, %Postgrex.Result{command: :notify}} = P.query(:client, "NOTIFY channel", [])
    receiver_pid = Process.whereis(:notifications)
    assert_receive {:notification, ^receiver_pid, ^ref, "channel", ""}
  end

  test "listening, unlistening, notify, don't receive", context do
    assert {:ok, ref} = PN.listen(context.pid_ps, "channel")
    assert :ok = PN.unlisten(context.pid_ps, ref)

    assert {:ok, %Postgrex.Result{command: :notify}} = P.query(context.pid, "NOTIFY channel", [])
    pid = context.pid_ps
    refute_receive {:notification, ^pid, ^ref, "channel", ""}
  end

  test "listening x2, unlistening, notify, receive", context do
    {:ok, other_pid_ps} = PN.start_link(@opts)

    assert {:ok, ref1} = PN.listen(context.pid_ps, "channel")
    assert {:ok, ref2} = PN.listen(other_pid_ps, "channel")

    assert :ok = PN.unlisten(other_pid_ps, ref2)
    assert {:ok, %Postgrex.Result{command: :notify}} = P.query(context.pid, "NOTIFY channel", [])

    pid = context.pid_ps
    assert_receive {:notification, ^pid, ^ref1, "channel", ""}, 1_000
  end

  test "listen, go away", context do
    spawn fn ->
      assert {:ok, _} = PN.listen(context.pid_ps, "channel")
    end

    assert {:ok, %Postgrex.Result{command: :notify}} = P.query(context.pid, "NOTIFY channel", [])
    :timer.sleep(300)
  end
end
