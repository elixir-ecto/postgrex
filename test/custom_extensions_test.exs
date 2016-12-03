defmodule CustomExtensionsTest do
  use ExUnit.Case, async: true
  import Postgrex.TestHelper
  alias Postgrex, as: P

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
          <<4::int32, int+1::int32>>
      end
    end

    def decode([]) do
      quote do
        <<4::int32, int::int32>> -> int + 1
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
          [<<byte_size(value)::int32>> | value]
       end
    end

    def decode({}) do
      quote do
        <<len::int32, binary::binary-size(len)>> ->
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

    def encode([]) do
      quote do
        _ ->
          raise "encode"
      end
    end

    def decode([]) do
      quote do
        <<1::int32, _>> ->
          raise "decode"
      end
    end
  end

  setup_all do
    on_exit(fn ->
      :code.delete(@types)
      :code.purge(@types)
    end)
    extensions = [BinaryExtension, TextExtension, BadExtension]
    opts       = [decode_binary: :reference, null: :custom]
    Postgrex.TypeModule.define(@types, extensions, opts)
    :ok
  end

  setup do
    opts = [database: "postgrex_test", backoff_type: :stop, types: @types]
    {:ok, pid} = P.start_link(opts)
    {:ok, [pid: pid, options: opts]}
  end

  test "encode and decode", context do
    assert [[44]] = query("SELECT $1::int4", [42])
    assert [[[44]]] = query("SELECT $1::int4[]", [[42]])
  end

  test "encode and decode unknown type", context do
    assert [["23"]] =
           query("SELECT $1::oid", ["23"])
  end

  test "encode and decode pushes error to client", context do
    assert_raise RuntimeError, "encode", fn ->
      query("SELECT $1::boolean", [true])
    end

    assert Process.alive? context[:pid]

    assert_raise RuntimeError, "decode", fn ->
      query("SELECT true", [])
    end

    assert Process.alive? context[:pid]
  end

  test "raise when executing prepared query on connection with different types", context do
    query = prepare("S42", "SELECT 42")

    opts = [types: Postgrex.DefaultTypes] ++ context[:options]
    {:ok, pid2} = Postgrex.start_link(opts)

    assert_raise ArgumentError, ~r"invalid types for the connection",
      fn() -> Postgrex.execute(pid2, query, []) end
  end

  test "dont decode text format", context do
    assert [["123.45"]] = query("SELECT 123.45::float8", [])
  end
end
