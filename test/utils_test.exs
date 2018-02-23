defmodule UtilsTest do
  use ExUnit.Case, async: true

  describe "parse_version/1" do
    test "parses simple 3-part version strings" do
      segments = Postgrex.Utils.parse_version("10.1.0")
      assert segments == {10, 1, 0}
    end

    test "parses complex version strings" do
      segments = Postgrex.Utils.parse_version("10.2 (Debian 10.2-1.pgdg90+1)")
      assert segments == {10, 2, 0}
    end
  end
end
