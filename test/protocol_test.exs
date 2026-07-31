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

  defp backend_message(type, data) do
    [type, <<IO.iodata_length(data) + 4::32>>, data]
  end
end
