defmodule NotificationTest do
  use ExUnit.Case
  import Postgrex.TestHelper
  alias Postgrex.Connection, as: P

  setup do
    opts = [ database: "postgrex_test" ]
    {:ok, pid} = P.start_link(opts)
    {:ok, pid2} = P.start_link(opts)

    {:ok, [pid: pid, pid2: pid2]}
  end

  test "listening", context do
    assert {:ok, _} = P.listen(context.pid, "channel")
  end

  test "notifying", context do
    assert :ok = query("NOTIFY channel", [])
  end

  @tag requires_notify_payload: true
  test "listening, notify, then receive (with payload)", context do
    assert {:ok, ref} = P.listen(context.pid, "channel")

    assert {:ok, %Postgrex.Result{command: :notify}} = P.query(context.pid2, "NOTIFY channel, 'hello'", [])
    pid = context.pid
    assert_receive {:notification, ^pid, ^ref, "channel", "hello"}, 1_000
  end

  test "listening, notify, then receive (without payload)", context do
    assert {:ok, ref} = P.listen(context.pid, "channel")

    assert {:ok, %Postgrex.Result{command: :notify}} = P.query(context.pid2, "NOTIFY channel", [])
    pid = context.pid
    assert_receive {:notification, ^pid, ^ref, "channel", ""}, 1_000
  end

  test "listening, unlistening, notify, don't receive", context do
    assert {:ok, ref} = P.listen(context.pid, "channel")
    assert :ok = P.unlisten(context.pid, ref)

    assert {:ok, %Postgrex.Result{command: :notify}} = P.query(context.pid2, "NOTIFY channel", [])
    pid = context.pid
    refute_receive {:notification, ^pid, ^ref, "channel", ""}, 1_000
  end

  test "listening x2, unlistening, notify, receive", context do
    assert {:ok, ref1} = P.listen(context.pid, "channel")
    assert {:ok, ref2} = P.listen(context.pid2, "channel")

    assert :ok = P.unlisten(context.pid2, ref2)

    assert {:ok, %Postgrex.Result{command: :notify}} = P.query(context.pid2, "NOTIFY channel", [])
    pid = context.pid
    assert_receive {:notification, ^pid, ^ref1, "channel", ""}, 1_000
  end

  test "listen, go away", context do
    spawn fn ->
      assert {:ok, _} = P.listen(context.pid, "channel")
    end

    assert {:ok, %Postgrex.Result{command: :notify}} = P.query(context.pid2, "NOTIFY channel", [])
    :timer.sleep(300)
  end
end
