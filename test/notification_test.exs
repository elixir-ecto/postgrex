defmodule NotificationTest do
  use ExUnit.Case, async: true
  import Postgrex.TestHelper
  alias Postgrex.Connection, as: P

  setup do
    opts = [ database: "postgrex_test" ]
    {:ok, pid} = P.start_link(opts)
    {:ok, pid2} = P.start_link(opts)

    {:ok, [pid: pid, pid2: pid2]}
  end

  test "listening", context do
    assert :ok = P.listen(context[:pid], "channel")
  end

  test "notifying", context do
    assert :ok = query("NOTIFY channel", [])
  end

  @tag requires_notify_payload: true
  test "listening, notify, then receive (with payload)", context do
    assert :ok = P.listen(context[:pid], "channel")

    assert {:ok, %Postgrex.Result{command: :notify}} = P.query(context[:pid2], "NOTIFY channel, 'hello'", [])
    pid = context[:pid]
    assert_receive {:notification, ^pid, {:msg_notify, _, "channel", "hello"}}, 1_000
  end

  test "listening, notify, then receive (without payload)", context do
    assert :ok = P.listen(context[:pid], "channel")

    assert {:ok, %Postgrex.Result{command: :notify}} = P.query(context[:pid2], "NOTIFY channel", [])
    pid = context[:pid]
    assert_receive {:notification, ^pid, {:msg_notify, _, "channel", ""}}, 1_000
  end

  test "listening, unlistening, notify, don't receive", context do
    assert :ok = P.listen(context[:pid], "channel")
    assert :ok = P.unlisten(context[:pid], "channel")

    assert {:ok, %Postgrex.Result{command: :notify}} = P.query(context[:pid2], "NOTIFY channel", [])
    pid = context[:pid]
    refute_receive {:notification, ^pid, {:msg_notify, _, "channel", ""}}, 1_000
  end

  test "listen, go away", context do
    spawn fn ->
      assert :ok = P.listen(context[:pid], "channel")
    end

    assert {:ok, %Postgrex.Result{command: :notify}} = P.query(context[:pid2], "NOTIFY channel", [])
    :timer.sleep(300)
  end
end
