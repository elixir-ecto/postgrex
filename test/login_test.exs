defmodule LoginTest do
  use ExUnit.Case
  alias Postgrex, as: P
  import ExUnit.CaptureLog

  setup do
    {:ok, [options: [database: "postgrex_test", backoff_type: :stop, max_restarts: 0]]}
  end

  test "login cleartext password", context do
    Process.flag(:trap_exit, true)
    opts = [username: "postgrex_cleartext_pw", password: "postgrex_cleartext_pw"]
    assert {:ok, pid} = P.start_link(opts ++ context[:options])
    assert {:ok, %Postgrex.Result{}} = P.query(pid, "SELECT 123", [])
  end

  test "login cleartext password failure", context do
    assert capture_log(fn ->
             opts = [username: "postgrex_cleartext_pw", password: "wrong_password"]
             assert_start_and_killed(opts ++ context[:options])
           end) =~
             ~r"\*\* \(Postgrex.Error\) FATAL (28P01 \(invalid_password\)|28000 \(invalid_authorization_specification\))"
  end

  test "login md5 password", context do
    opts = [username: "postgrex_md5_pw", password: "postgrex_md5_pw"]
    assert {:ok, pid} = P.start_link(opts ++ context[:options])
    assert {:ok, %Postgrex.Result{}} = P.query(pid, "SELECT 123", [])
  end

  test "login md5 password failure", context do
    assert capture_log(fn ->
             opts = [username: "postgrex_md5_pw", password: "wrong_password"]
             assert_start_and_killed(opts ++ context[:options])
           end) =~
             ~r"\*\* \(Postgrex.Error\) FATAL (28P01 \(invalid_password\)|28000 \(invalid_authorization_specification\))"
  end

  @tag min_pg_version: "10.0"
  test "login scram password", context do
    opts = [username: "postgrex_scram_pw", password: "postgrex_scram_pw"]
    assert {:ok, pid} = P.start_link(opts ++ context[:options])
    assert {:ok, %Postgrex.Result{}} = P.query(pid, "SELECT 123", [])
  end

  @tag min_pg_version: "10.0"
  test "login scram password failure", context do
    assert capture_log(fn ->
             opts = [username: "postgrex_scram_pw", password: "wrong_password"]
             assert_start_and_killed(opts ++ context[:options])
           end) =~
             ~r"\*\* \(Postgrex.Error\) FATAL (28P01 \(invalid_password\)|28000 \(invalid_authorization_specification\))"
  end

  test "parameters", context do
    assert {:ok, pid} = P.start_link(context[:options])
    assert {:ok, %Postgrex.Result{}} = P.query(pid, "SELECT 123", [])

    assert String.match?(P.parameters(pid)["server_version"], ~R"^\d+\.\d+")
  end

  @tag min_pg_version: "9.0"
  test "setting parameters", context do
    assert {:ok, pid} = P.start_link(context[:options])
    assert "" == P.parameters(pid)["application_name"]
    opts = [parameters: [application_name: "postgrex"]]
    assert {:ok, pid} = P.start_link(opts ++ context[:options])
    assert {:ok, %Postgrex.Result{}} = P.query(pid, "SELECT 123", [])
    assert "postgrex" == P.parameters(pid)["application_name"]
  end

  test "infinity timeout", context do
    opts = [timeout: :infinity]
    assert {:ok, pid} = P.start_link(opts ++ context[:options])
    assert {:ok, %Postgrex.Result{}} = P.query(pid, "SELECT 123", [])
  end

  @tag :ssl
  test "ssl", context do
    opts = [ssl: true]
    assert {:ok, pid} = P.start_link(opts ++ context[:options])
    assert {:ok, %Postgrex.Result{}} = P.query(pid, "SELECT 123", [])
  end

  test "env var defaults", context do
    assert {:ok, pid} = P.start_link(context[:options])
    assert {:ok, %Postgrex.Result{}} = P.query(pid, "SELECT 123", [])
  end

  test "env var default db name", context do
    previous_db_name = System.get_env("PGDATABASE")

    try do
      set_db_name("postgrex_test")
      opts = [] ++ Keyword.delete(context[:options], :database)
      assert {:ok, pid} = P.start_link(opts)
      assert {:ok, %Postgrex.Result{}} = P.query(pid, "SELECT 123", [])
    after
      set_db_name(previous_db_name)
    end
  end

  test "env var default db name (no such database)", context do
    previous_db_name = System.get_env("PGDATABASE")

    try do
      set_db_name("doesntexist")

      assert capture_log(fn ->
               opts = Keyword.delete(context[:options], :database)
               assert_start_and_killed(opts)
             end)
    after
      set_db_name(previous_db_name)
    end
  end

  test "non-existent database", context do
    assert capture_log(fn ->
             opts = [database: "doesntexist"]
             assert_start_and_killed(opts ++ context[:options])
           end) =~ "** (Postgrex.Error) FATAL 3D000 (invalid_catalog_name)"
  end

  test "non-existent domain", context do
    assert capture_log(fn ->
             opts = [hostname: "doesntexist", connect_timeout: 100]
             assert_start_and_killed(opts ++ context[:options])
           end) =~ ~r"tcp connect \(doesntexist:\d+\): non-existing domain - :nxdomain"
  end

  @tag :unix
  test "unix domain socket connection", context do
    socket = System.get_env("PG_SOCKET_DIR") || "/tmp"

    opts = [socket_dir: socket]

    capture_log(fn ->
      assert {:ok, pid} = P.start_link(opts ++ context[:options])
      assert {:ok, %Postgrex.Result{}} = P.query(pid, "SELECT 123", [])
    end)
  end

  @tag :unix
  test "non-existent unix domain socket", context do
    assert capture_log(fn ->
             opts = [socket_dir: "/doesntexist"]
             assert_start_and_killed(opts ++ context[:options])
           end) =~
             ~r"tcp connect \(/doesntexist/.s.PGSQL.\d+\): no such file or directory - :enoent"
  end

  @tag :unix
  test "socket precedes socket_dir", context do
    assert capture_log(fn ->
             opts = [socket: "/socketfile", socket_dir: "/socketdir"]
             assert_start_and_killed(opts ++ context[:options])
           end) =~ ~r"tcp connect \(/socketfile\): no such file or directory - :enoent"
  end

  test "after connect function run", context do
    parent = self()
    after_connect = fn conn -> send(parent, P.query(conn, "SELECT 42", [])) end
    opts = [after_connect: after_connect]

    {:ok, _} = P.start_link(opts ++ context[:options])
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

  defp assert_start_and_killed(opts) do
    Process.flag(:trap_exit, true)

    case P.start_link(opts) do
      {:ok, pid} -> assert_receive {:EXIT, ^pid, :killed}
      {:error, :killed} -> :ok
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
