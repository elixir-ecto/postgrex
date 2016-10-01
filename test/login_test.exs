defmodule LoginTest do
  use ExUnit.Case
  alias Postgrex, as: P
  import Postgrex.TestHelper

  test "login cleartext password" do
    Process.flag(:trap_exit, true)

    opts = [ hostname: "localhost", username: "postgrex_cleartext_pw",
             password: "postgrex_cleartext_pw", database: "postgres" ]
    assert {:ok, pid} = P.start_link(opts)
    assert {:ok, %Postgrex.Result{}} = P.query(pid, "SELECT 123", [])
  end

  test "login cleartext password failure" do
    Process.flag(:trap_exit, true)

    opts = [ hostname: "localhost", username: "postgrex_cleartext_pw",
             password: "wrong_password", database: "postgres",
             backoff_type: :stop]

    capture_log fn ->
      assert {:ok, pid} = P.start_link(opts)
      assert_receive {:EXIT, ^pid, {%Postgrex.Error{postgres: %{code: code}}, [_|_]}}
      assert code in [:invalid_authorization_specification, :invalid_password]
    end
  end

  test "login md5 password" do
    Process.flag(:trap_exit, true)

    opts = [ hostname: "localhost", username: "postgrex_md5_pw",
             password: "postgrex_md5_pw", database: "postgres" ]
    assert {:ok, pid} = P.start_link(opts)
    assert {:ok, %Postgrex.Result{}} = P.query(pid, "SELECT 123", [])
  end

  test "login md5 password failure" do
    Process.flag(:trap_exit, true)

    opts = [ hostname: "localhost", username: "postgrex_md5_pw",
             password: "wrong_password", database: "postgres",
             backoff_type: :stop]

    capture_log fn ->
      assert {:ok, pid} = P.start_link(opts)
      assert_receive {:EXIT, ^pid, {%Postgrex.Error{postgres: %{code: code}}, [_|_]}}
      assert code in [:invalid_authorization_specification, :invalid_password]
    end
  end

  test "parameters" do
    opts = [ hostname: "localhost", username: "postgres",
             password: "postgres", database: "postgrex_test" ]

    assert {:ok, pid} = P.start_link(opts)
    assert {:ok, %Postgrex.Result{}} = P.query(pid, "SELECT 123", [])

    assert String.match? P.parameters(pid)["server_version"], ~R"^\d+\.\d+"
  end

  @tag min_pg_version: "9.0"
  test "setting parameters" do
    opts = [ hostname: "localhost", username: "postgres",
             password: "postgres", database: "postgrex_test" ]

    assert {:ok, pid} = P.start_link(opts)
    assert "" == P.parameters(pid)["application_name"]

    opts = opts ++ [parameters: [application_name: "postgrex"]]
    assert {:ok, pid} = P.start_link(opts)
    assert {:ok, %Postgrex.Result{}} = P.query(pid, "SELECT 123", [])
    assert "postgrex" == P.parameters(pid)["application_name"]
  end

  test "infinity timeout" do
    opts = [ hostname: "localhost", username: "postgres",
             password: "postgres", database: "postgrex_test",
             timeout: :infinity ]

    assert {:ok, pid} = P.start_link(opts)
    assert {:ok, %Postgrex.Result{}} = P.query(pid, "SELECT 123", [])
  end

  @tag :ssl
  test "ssl" do
    opts = [ hostname: "localhost", username: "postgres",
             password: "postgres", database: "postgrex_test",
             ssl: true ]
    assert {:ok, pid} = P.start_link(opts)
    assert {:ok, %Postgrex.Result{}} = P.query(pid, "SELECT 123", [])
  end

  test "env var defaults" do
    opts = [ database: "postgrex_test" ]
    assert {:ok, pid} = P.start_link(opts)
    assert {:ok, %Postgrex.Result{}} = P.query(pid, "SELECT 123", [])
  end

  test "env var default db name" do
    previous_db_name = System.get_env("PGDATABASE")
    try do
      set_db_name("postgrex_test")
      opts = []
      assert {:ok, pid} = P.start_link(opts)
      assert {:ok, %Postgrex.Result{}} = P.query(pid, "SELECT 123", [])
    after
      set_db_name(previous_db_name)
    end
  end

  test "env var default db name (no such database)" do
    previous_db_name = System.get_env("PGDATABASE")
    try do
      set_db_name("doesntexist")
      Process.flag(:trap_exit, true)

      capture_log fn ->
        opts = [ sync_connect: true, backoff_type: :stop ]
        assert {:error, {%Postgrex.Error{postgres: %{code: :invalid_catalog_name}}, [_|_]}} =
               P.start_link(opts)
      end
    after
      set_db_name(previous_db_name)
    end
  end

  test "sync connect" do
    opts = [ database: "postgres", sync_connect: true ]
    assert {:ok, pid} = P.start_link(opts)
    assert {:ok, %Postgrex.Result{}} = P.query(pid, "SELECT 123", [])
  end

  test "non existant database" do
    Process.flag(:trap_exit, true)

    capture_log fn ->
      opts = [ database: "doesntexist", sync_connect: true ,
               backoff_type: :stop ]
      assert {:error, {%Postgrex.Error{postgres: %{code: :invalid_catalog_name}}, [_|_]}} =
             P.start_link(opts)

      assert_receive {:EXIT, _, {%Postgrex.Error{postgres: %{code: :invalid_catalog_name}}, [_|_]}}
    end

    capture_log fn ->
      opts = [ database: "doesntexist", backoff_type: :stop ]
      {:ok, pid} = P.start_link(opts)
      assert_receive {:EXIT, ^pid, {%Postgrex.Error{postgres: %{code: :invalid_catalog_name}}, [_|_]}}
    end
  end

  test "non-existent domain" do
    Process.flag(:trap_exit, true)
    opts = [ hostname: "doesntexist", username: "postgrex_cleartext_pw",
             password: "password", database: "postgres", backoff_type: :stop ]

    capture_log fn ->
      assert {:ok, pid} = P.start_link(opts)
      assert_receive {:EXIT, ^pid, {%DBConnection.ConnectionError{message: message}, [_|_]}}
      assert message == "tcp connect: non-existing domain - :nxdomain"
    end
  end

  test "after connect function run" do
    parent = self()
    after_connect = fn(conn) -> send(parent, P.query(conn, "SELECT 42", [])) end
    opts = [ hostname: "localhost", username: "postgres",
             password: "postgres", database: "postgrex_test",
             after_connect: after_connect ]

    {:ok, _} = P.start_link(opts)

    assert_receive {:ok, %Postgrex.Result{}}
  end

  test "translates provided port number to integer" do
    assert 123 == P.Utils.default_opts(port: "123")[:port]
  end

  test "defaults to PGPORT if no port number is provided" do
    previous_port = System.get_env("PGPORT")
    try do
      set_port_number("12345")
      assert 12345 == P.Utils.default_opts([])[:port]
    after
      set_port_number(previous_port)
    end
  end

  test "ignores PGPORT if non existent" do
    opts = []
    previous_port = System.get_env("PGPORT")
    try do
      System.delete_env("PGPORT")
      assert nil == P.Utils.default_opts(opts)[:port]
    after
      set_port_number(previous_port)
    end
  end

  defp set_port_number(nil) do
    System.delete_env("PGPORT")
  end

  defp set_port_number(port) when is_binary(port) do
    System.put_env("PGPORT", port)
  end

  defp set_db_name(nil) do
    System.delete_env("PGDATABASE")
  end

  defp set_db_name(db_name) when is_binary(db_name) do
    System.put_env("PGDATABASE", db_name)
  end
end
