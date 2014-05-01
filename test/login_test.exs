defmodule LoginTest do
  use ExUnit.Case, async: true
  alias Postgrex.Connection, as: P

  test "login cleartext password" do
    opts = [ hostname: "localhost", username: "postgrex_cleartext_pw",
             password: "postgrex_cleartext_pw", database: "postgres" ]
    assert {:ok, pid} = P.start_link(opts)
    assert :ok = P.stop(pid)
  end

  test "login md5 password" do
    opts = [ hostname: "localhost", username: "postgrex_md5_pw",
             password: "postgrex_md5_pw", database: "postgres" ]
    assert {:ok, pid} = P.start_link(opts)
    assert :ok = P.stop(pid)
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
