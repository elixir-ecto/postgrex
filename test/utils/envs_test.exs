defmodule Utils.EnvsTest do
  use ExUnit.Case, async: false

  ExUnit.Case.register_attribute(__MODULE__, :env)

  setup ctx do
    envs =
      for {key, value} <- ctx.registered.env do
        key = key |> to_string() |> String.upcase()
        old = System.get_env(key)

        if value do
          System.put_env(key, value)
        else
          System.delete_env(key)
        end

        {key, old}
      end

    on_exit(fn ->
      for {key, value} <- envs do
        if value do
          System.put_env(key, value)
        else
          System.delete_env(key)
        end
      end
    end)

    {:ok, opts: Postgrex.Utils.default_opts([])}
  end

  describe "PGHOST" do
    @env PGHOST: nil
    test "by default it is set to `localhost`", ctx do
      assert ctx.opts[:hostname] == "localhost"
    end

    @env PGHOST: "foobar"
    test "if the host is 'regular' hostname, then it sets hostname", ctx do
      assert ctx.opts[:hostname] == "foobar"
    end

    @env PGHOST: "127.0.0.1"
    test "if the host is IPv4 address then it sets hostname", ctx do
      assert ctx.opts[:hostname] == "127.0.0.1"
    end

    @env PGHOST: "[::1]"
    test "if the host is IPv6 address then it sets hostname", ctx do
      assert ctx.opts[:hostname] == "[::1]"
    end

    @env PGHOST: "/tmp/example"
    test "if the host is path-like (UNIX) then it sets socket_dir", ctx do
      assert ctx.opts[:socket_dir] == "/tmp/example"
    end

    @env PGHOST: ~S"C:\\example"
    test "if the host is path-like (Windows) then it sets socket_dir", ctx do
      assert ctx.opts[:socket_dir] == ~S"C:\\example"
    end

    @env PGHOST: ~S"C://example"
    test "if the host is path-like (Windows alt) then it sets socket_dir", ctx do
      assert ctx.opts[:socket_dir] == ~S"C://example"
    end

    @env PGHOST: "@foo"
    test "if the host is abstract socket address it sets socket", ctx do
      assert ctx.opts[:socket] == <<0, "foo">>
    end
  end
end
