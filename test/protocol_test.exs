defmodule Postgrex.ProtocolTest do
  use ExUnit.Case, async: true

  alias Postgrex.Protocol

  defmodule Socket do
    def send(pid, data) do
      Kernel.send(pid, {:sent, IO.iodata_to_binary(data)})
      :ok
    end
  end

  test "streaming startup handles asynchronous messages before the copy response" do
    responses =
      IO.iodata_to_binary([
        backend_message(?S, ["application_name", 0, "postgrex", 0]),
        backend_message(?N, [?S, "NOTICE", 0, ?M, "replication starting", 0, 0]),
        backend_message(?A, [<<123::32>>, "events", 0, "ready", 0]),
        backend_message(?W, <<0, 0::16>>)
      ])

    state = %Protocol{
      sock: {Socket, self()},
      buffer: responses,
      parameters: %{},
      messages: []
    }

    assert {:ok, state} = Protocol.handle_streaming("START_REPLICATION", state)
    assert state.parameters["application_name"] == "postgrex"
    assert [%{message: "replication starting", severity: "NOTICE"}] = state.messages
    assert state.buffer == ""

    assert_receive {:sent, <<?Q, _size::32, "START_REPLICATION", 0>>}
  end

  test "ping handles notifications received before ready" do
    responses =
      IO.iodata_to_binary([
        backend_message(?A, [<<123::32>>, "events", 0, "ready", 0]),
        backend_message(?Z, [?I])
      ])

    state = %Protocol{
      sock: {Socket, self()},
      buffer: responses,
      postgres: :idle,
      transactions: :naive,
      messages: []
    }

    notify = fn channel, payload -> send(self(), {:notification, channel, payload}) end

    assert {:ok, state} = Protocol.ping(state, notify: notify)
    assert state.buffer == ""
    assert_receive {:notification, "events", "ready"}
    assert_receive {:sent, <<?S, 4::32>>}
  end

  test "connection errors after authentication do not include connection options" do
    {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false])
    {:ok, port} = :inet.port(listen)

    server =
      Task.async(fn ->
        {:ok, sock} = :gen_tcp.accept(listen)
        {:ok, <<len::32>>} = :gen_tcp.recv(sock, 4)
        {:ok, _startup} = :gen_tcp.recv(sock, len - 4)

        :ok =
          :gen_tcp.send(sock, [
            backend_message(?R, <<0::32>>),
            backend_message(?A, [<<123::32>>, "events", 0, "ready", 0])
          ])

        :gen_tcp.close(sock)
      end)

    opts =
      Postgrex.Utils.default_opts(
        hostname: "localhost",
        port: port,
        database: "postgrex_test",
        username: "postgres",
        password: "do-not-log-me",
        types: Postgrex.DefaultTypes
      )

    error = assert_raise MatchError, fn -> Protocol.connect(opts) end
    refute inspect(error, limit: :infinity) =~ "do-not-log-me"
    refute Map.has_key?(error.term, :ssl)

    Task.await(server)
    :gen_tcp.close(listen)
  end

  defp backend_message(type, data) do
    [type, <<IO.iodata_length(data) + 4::32>>, data]
  end
end
