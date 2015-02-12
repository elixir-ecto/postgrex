defmodule LoginTest do
  use ExUnit.Case
  alias Postgrex.Connection, as: P

  defp capture_log(fun) do
    Logger.remove_backend(:console)
    fun.()
    Logger.add_backend(:console, flush: true)
  end

  test "login cleartext password" do
    Process.flag(:trap_exit, true)

    opts = [ hostname: "localhost", username: "postgrex_cleartext_pw",
             password: "postgrex_cleartext_pw", database: "postgres" ]
    assert {:ok, pid} = P.start_link(opts)
    assert :ok = P.stop(pid)

    opts = [ hostname: "localhost", username: "postgrex_cleartext_pw",
             password: "wrong_password", database: "postgres" ]

    capture_log fn ->
      assert {:error, %Postgrex.Error{postgres: %{code: "28P01"}}} = P.start_link(opts)
    end
  end

  test "login md5 password" do
    Process.flag(:trap_exit, true)

    opts = [ hostname: "localhost", username: "postgrex_md5_pw",
             password: "postgrex_md5_pw", database: "postgres" ]
    assert {:ok, pid} = P.start_link(opts)
    assert :ok = P.stop(pid)

    opts = [ hostname: "localhost", username: "postgrex_md5_pw",
             password: "wrong_password", database: "postgres" ]

    capture_log fn ->
      assert {:error, %Postgrex.Error{postgres: %{code: "28P01"}}} = P.start_link(opts)
    end
  end

  test "parameters" do
    opts = [ hostname: "localhost", username: "postgres",
             password: "postgres", database: "postgrex_test" ]

    assert {:ok, pid} = P.start_link(opts)
    assert String.match? P.parameters(pid)["server_version"], ~R"\d+\.\d+\.\d+"

    if String.match? P.parameters(pid)["server_version"], ~R"9\.\d+\.\d+" do
      assert "" == P.parameters(pid)["application_name"]
      assert :ok = P.stop(pid)

      opts = opts ++ [parameters: [application_name: "postgrex"]]
      assert {:ok, pid} = P.start_link(opts)
      assert "postgrex" == P.parameters(pid)["application_name"]
      assert :ok = P.stop(pid)
    else
      assert :ok = P.stop(pid)
    end
  end

  @tag :ssl
  test "ssl" do
    opts = [ hostname: "localhost", username: "postgres",
             password: "postgres", database: "postgrex_test",
             ssl: true ]
    assert {:ok, pid} = P.start_link(opts)
    assert :ok = P.stop(pid)
  end

  test "env var defaults" do
    opts = [ database: "postgrex_test" ]
    assert {:ok, pid} = P.start_link(opts)
    assert :ok = P.stop(pid)
  end
end
