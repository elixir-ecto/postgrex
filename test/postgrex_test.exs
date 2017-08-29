defmodule PostgrexTest do
  use ExUnit.Case, async: false

  test "start_link/2 raises when :ssl app is required but not started" do
    on_exit(fn ->
      Application.start(:ssl)
    end)

    Application.stop(:ssl)
    assert_raise RuntimeError, ~r"SSL connection can not be established", fn ->
      Postgrex.start_link(ssl: true, database: "postgrex_test")
    end
  end
end
