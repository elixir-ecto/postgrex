defmodule PostgrexTest do
  use ExUnit.Case, async: false
  import ExUnit.CaptureLog

  test "start_link/2 raises when :ssl app is required but not started" do
    on_exit(fn ->
      Application.start(:ssl)
    end)

    Application.stop(:ssl)

    assert_raise RuntimeError, ~r"SSL connection can not be established", fn ->
      Postgrex.start_link(ssl: true, database: "postgrex_test")
    end
  end

  test "start_link/2 sets search path" do
    # valid argument
    search_path = ["public", "extension"]
    {:ok, conn} = Postgrex.start_link(database: "postgrex_test", search_path: search_path)
    %{rows: [[result]]} = Postgrex.query!(conn, "show search_path", [])
    assert result == Enum.join(search_path, ", ")

    # invalid argument
    Process.flag(:trap_exit, true)
    search_path = "public, extension"

    opts = [
      database: "postgrex_test",
      search_path: search_path,
      show_sensitive_data_on_connection_error: true
    ]

    error_log =
      capture_log(fn ->
        Postgrex.start_link(opts)
        assert_receive {:EXIT, _, :killed}
      end)

    assert error_log =~ "expected :search_path to be a list of strings, got: \"#{search_path}\""
  end
end
