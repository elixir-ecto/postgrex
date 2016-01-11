defmodule LoginTest do
  use ExUnit.Case
  alias Postgrex.Connection, as: P
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
      assert_receive {:EXIT, ^pid, {%Postgrex.Error{message: message}, [_|_]}}
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
end
