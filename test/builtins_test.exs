defmodule BuiltinsTest do
  use ExUnit.Case, async: true

  describe "Postgrex.Interval" do
    alias Postgrex.Interval

    test "compare" do
      assert Interval.compare(%Interval{}, %Interval{}) == :eq

      assert Interval.compare(
               %Interval{microsecs: 1},
               %Interval{microsecs: 2}
             ) == :lt

      assert Interval.compare(
               %Interval{microsecs: 2},
               %Interval{microsecs: 1}
             ) == :gt

      assert Interval.compare(
               %Interval{secs: 1, microsecs: 9},
               %Interval{secs: 2, microsecs: 9}
             ) == :lt

      assert Interval.compare(
               %Interval{secs: 2, microsecs: 9},
               %Interval{secs: 1, microsecs: 9}
             ) == :gt

      assert Interval.compare(
               %Interval{days: 1, secs: 8, microsecs: 9},
               %Interval{days: 2, secs: 8, microsecs: 9}
             ) == :lt

      assert Interval.compare(
               %Interval{days: 2, secs: 8, microsecs: 9},
               %Interval{days: 1, secs: 8, microsecs: 9}
             ) == :gt

      assert Interval.compare(
               %Interval{months: 1, days: 7, secs: 8, microsecs: 9},
               %Interval{months: 2, days: 7, secs: 8, microsecs: 9}
             ) == :lt

      assert Interval.compare(
               %Interval{months: 2, days: 7, secs: 8, microsecs: 9},
               %Interval{months: 1, days: 7, secs: 8, microsecs: 9}
             ) == :gt
    end

    test "to_string" do
      assert Interval.to_string(%Interval{secs: 0}) ==
               "0 seconds"

      assert Interval.to_string(%Interval{microsecs: 123}) ==
               "0.000123 seconds"

      assert Interval.to_string(%Interval{secs: 123}) ==
               "123 seconds"

      assert Interval.to_string(%Interval{secs: 1, microsecs: 123}) ==
               "1.000123 seconds"

      assert Interval.to_string(%Interval{secs: 1, microsecs: 654_321}) ==
               "1.654321 seconds"

      assert Interval.to_string(%Interval{days: 1, secs: 1, microsecs: 654_321}) ==
               "1 day, 1.654321 seconds"

      assert Interval.to_string(%Interval{days: 2, secs: 1, microsecs: 654_321}) ==
               "2 days, 1.654321 seconds"

      assert Interval.to_string(%Interval{months: 1, days: 1, secs: 1, microsecs: 654_321}) ==
               "1 month, 1 day, 1.654321 seconds"

      assert Interval.to_string(%Interval{months: 2, days: 2, secs: 1, microsecs: 654_321}) ==
               "2 months, 2 days, 1.654321 seconds"
    end
  end
end
