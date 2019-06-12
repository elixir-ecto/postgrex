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

    test "parses 3 part version strings" do
      segments = Postgrex.Utils.parse_version("10.2.1 (Debian 10.2-1.pgdg90+1)")
      assert segments == {10, 2, 1}
    end

    test "parses 4 part version strings" do
      segments = Postgrex.Utils.parse_version("9.5.5.10 ___")
      assert segments == {9, 5, 5}
    end

    test "parses beta version strings" do
      segments = Postgrex.Utils.parse_version("12beta1 (Debian 10.2-1.pgdg90+1)")

      assert segments == {12, 0, 0}

      assert Postgrex.Utils.parse_version("12alpha1 (Debian 10.2-1.pgdg90+1)") ==
               Postgrex.Utils.parse_version("12beta1 (Debian 10.2-1.pgdg90+1)")
    end
  end
end
