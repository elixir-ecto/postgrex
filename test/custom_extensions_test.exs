defmodule CustomExtensionsTest do
  use ExUnit.Case, async: true
  import Postgrex.TestHelper
  import ExUnit.CaptureLog
  alias Postgrex, as: P
  alias Postgrex.Result

  @types CustomExtensionsTypes

  defmodule BinaryExtension do
    @behaviour Postgrex.Extension

    def init([]),
      do: []

    def matching([]),
      do: [send: "int4send"]

    def format([]),
      do: :binary

    def encode([]) do
      quote do
        int ->
          <<4::int32(), int + 1::int32()>>
      end
    end

    def decode([]) do
      quote do
        <<4::int32(), int::int32()>> -> int + 1
      end
    end
  end

  defmodule TextExtension do
    @behaviour Postgrex.Extension

    def init([]),
      do: {}

    def matching({}),
      do: [send: "float8send", send: "oidsend"]

    def format({}),
      do: :text

    def encode({}) do
      quote do
        value ->
          [<<byte_size(value)::int32()>> | value]
      end
    end

    def decode({}) do
      quote do
        <<len::int32(), binary::binary-size(len)>> ->
          binary
      end
    end
  end

  defmodule BadExtension do
    @behaviour Postgrex.Extension

    def init([]),
      do: []

    def matching([]),
      do: [send: "boolsend"]

    def format([]),
      do: :binary

    def prelude([]) do
      quote do
        @encode_msg "encode"
        @decode_msg "decode"
      end
    end

    def encode([]) do
      quote do
        _ ->
          raise @encode_msg
      end
    end

    def decode([]) do
      quote do
        <<1::int32(), _>> ->
          raise @decode_msg
      end
    end
  end

  setup_all do
    on_exit(fn ->
      :code.delete(@types)
      :code.purge(@types)
    end)

    extensions = [BinaryExtension, TextExtension, BadExtension]
    opts = [decode_binary: :reference, null: :custom]
    Postgrex.TypeModule.define(@types, extensions, opts)
    :ok
  end

  setup do
    opts = [database: "postgrex_test", backoff_type: :stop, max_restarts: 0, types: @types]
    {:ok, pid} = P.start_link(opts)
    {:ok, [pid: pid, options: opts]}
  end

  test "encode and decode", context do
    assert [[44]] = query("SELECT $1::int4", [42])
    assert [[[44]]] = query("SELECT $1::int4[]", [[42]])
  end

  test "encode and decode unknown type", context do
    assert [["23"]] = query("SELECT $1::oid", ["23"])
  end

  test "encode and decode pushes error to client", context do
    Process.flag(:trap_exit, true)

    assert_raise RuntimeError, "encode", fn ->
      query("SELECT $1::boolean", [true])
    end

    assert capture_log(fn ->
             assert_raise RuntimeError, "decode", fn ->
               query("SELECT true", [])
             end

             pid = context[:pid]
             assert_receive {:EXIT, ^pid, :killed}
           end) =~ "(RuntimeError) decode"
  end

  test "execute prepared query on connection with different types", context do
    query = prepare("S42", "SELECT 42")

    opts = [types: Postgrex.DefaultTypes] ++ context[:options]
    {:ok, pid2} = Postgrex.start_link(opts)

    {:ok, %Postgrex.Query{}, %Result{rows: [[42]]}} = Postgrex.execute(pid2, query, [])
  end

  test "stream prepared query on connection with different types", context do
    query = prepare("S42", "SELECT 42")

    opts = [types: Postgrex.DefaultTypes] ++ context[:options]
    {:ok, pid2} = Postgrex.start_link(opts)

    Postgrex.transaction(pid2, fn conn ->
      assert [%Result{rows: [[42]]}] = stream(query, []) |> Enum.take(1)
    end)
  end

  test "stream prepared COPY FROM on connection with different types", context do
    query = prepare("copy", "COPY uniques FROM STDIN")

    opts = [types: Postgrex.DefaultTypes] ++ context[:options]
    {:ok, pid2} = Postgrex.start_link(opts)

    Postgrex.transaction(pid2, fn conn ->
      stream = stream(query, [])
      assert Enum.into(["1\n"], stream) == stream
      Postgrex.rollback(conn, :done)
    end)
  end

  test "dont decode text format", context do
    assert [["123.45"]] = query("SELECT 123.45::float8", [])
  end
end
