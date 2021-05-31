defmodule CalendarTest do
  use ExUnit.Case, async: true
  import Postgrex.TestHelper
  alias Postgrex, as: P

  Postgrex.Types.define(Postgrex.InfiniteCalendarTypes, [], allow_infinite_timestamps: true)

  setup do
    opts = [database: "postgrex_test", backoff_type: :stop]
    {:ok, pid} = P.start_link(opts)
    {:ok, [pid: pid]}
  end

  test "decode time", context do
    assert [[~T[00:00:00.000000]]] = query("SELECT time '00:00:00'", [])
    assert [[~T[01:02:03.000000]]] = query("SELECT time '01:02:03'", [])
    assert [[~T[23:59:59.000000]]] = query("SELECT time '23:59:59'", [])
    assert [[~T[04:05:06.000000]]] = query("SELECT time '04:05:06 PST'", [])
    assert [[~T[04:05:06.000000]]] = query("SELECT time '04:05:06-8'", [])
    assert [[~T[04:05:06.000000]]] = query("SELECT time '04:05:06-8'", [])

    assert [[~T[00:00:00.123000]]] = query("SELECT time '00:00:00.123'", [])
    assert [[~T[00:00:00.123456]]] = query("SELECT time '00:00:00.123456'", [])
    assert [[~T[01:02:03.123456]]] = query("SELECT time '01:02:03.123456'", [])
  end

  test "decode timetz", context do
    assert [[~T[00:00:00.000000]]] = query("SELECT time with time zone '00:00:00 UTC'", [])
    assert [[~T[01:02:03.000000]]] = query("SELECT time with time zone '01:02:03 UTC'", [])
    assert [[~T[23:00:00.000000]]] = query("SELECT time with time zone '00:00:00+1'", [])
    assert [[~T[01:00:00.000000]]] = query("SELECT time with time zone '23:00:00-2'", [])

    assert :ok = query("SET SESSION TIME ZONE UTC", [])

    assert [[~T[23:59:59.000000]]] = query("SELECT time with time zone '23:59:59'", [])
    assert [[~T[12:05:06.000000]]] = query("SELECT time with time zone '04:05:06 PST'", [])
    assert [[~T[12:05:06.000000]]] = query("SELECT time with time zone '04:05:06-8'", [])

    assert [[~T[00:00:00.123000]]] = query("SELECT time with time zone '00:00:00.123'", [])
    assert [[~T[00:00:00.123456]]] = query("SELECT time with time zone '00:00:00.123456 UTC'", [])
    assert [[~T[01:02:03.123456]]] = query("SELECT time with time zone '01:02:03.123456'", [])

    assert [[~T[01:02:03.123456]]] =
             query("SELECT time with time zone '16:01:03.123456+1459'", [])

    assert [[~T[16:01:03.123456]]] =
             query("SELECT time with time zone '01:02:03.123456-1459'", [])

    assert :ok = query("SET SESSION TIME ZONE +1", [])

    assert [[~T[12:05:06.000000]]] = query("SELECT time with time zone '04:05:06 PST'", [])
    assert [[~T[12:05:06.000000]]] = query("SELECT time with time zone '04:05:06-8'", [])
    assert [[~T[01:02:03.123456]]] = query("SELECT time with time zone '01:02:03.123456 UTC'", [])
  end

  test "decode date", context do
    assert [[~D[0001-01-01]]] = query("SELECT date '0001-01-01'", [])
    assert [[~D[0001-02-03]]] = query("SELECT date '0001-02-03'", [])
    assert [[~D[2013-09-23]]] = query("SELECT date '2013-09-23'", [])

    assert [[~D[0000-01-01]]] = query("SELECT date 'January 1, 1 BC'", [])
    assert [[~D[9999-12-31]]] = query("SELECT date '9999-12-31'", [])

    assert [[~D[-0001-01-01]]] = query("SELECT date 'January 1, 2 BC'", [])
  end

  test "decode date upper bound error", context do
    assert_raise ArgumentError, fn ->
      query("SELECT date '10000-01-01'", [])
    end
  end

  test "decode timestamp", context do
    assert [[~N[2001-01-01 00:00:00.000000]]] =
             query("SELECT timestamp '2001-01-01 00:00:00'", [])

    assert :ok = query("SET SESSION TIME ZONE UTC", [])

    assert [[~N[2013-09-23 14:04:37.123000]]] =
             query("SELECT timestamp '2013-09-23 14:04:37.123'", [])

    assert [[~N[2013-09-23 14:04:37.000000]]] =
             query("SELECT timestamp '2013-09-23 14:04:37 PST'", [])

    assert [[~N[2013-09-23 14:04:37.000000]]] =
             query("SELECT timestamp '2013-09-23 14:04:37-8'", [])

    assert :ok = query("SET SESSION TIME ZONE +1", [])

    assert [[~N[2013-09-23 14:04:37.000000]]] =
             query("SELECT timestamp '2013-09-23 14:04:37 PST'", [])

    assert [[~N[1980-01-01 00:00:00.123456]]] =
             query("SELECT timestamp '1980-01-01 00:00:00.123456'", [])

    assert [[~N[-1980-01-01 00:00:00.123456]]] =
             query("SELECT timestamp '1981-01-01BC 00:00:00.123456'", [])

    assert [[~N[-4712-01-01 00:00:00.123456]]] =
             query("SELECT timestamp '4713-01-01BC 00:00:00.123456'", [])
  end

  test "decode timestamptz", context do
    assert [
             [
               %DateTime{
                 year: 2001,
                 month: 1,
                 day: 1,
                 hour: 0,
                 minute: 0,
                 second: 0,
                 microsecond: {0, 6},
                 time_zone: "Etc/UTC",
                 utc_offset: 0
               }
             ]
           ] = query("SELECT timestamp with time zone '2001-01-01 00:00:00 UTC'", [])

    assert :ok = query("SET SESSION TIME ZONE UTC", [])

    assert [
             [
               %DateTime{
                 year: 2013,
                 month: 9,
                 day: 23,
                 hour: 14,
                 minute: 4,
                 second: 37,
                 microsecond: {123_000, 6},
                 time_zone: "Etc/UTC",
                 utc_offset: 0
               }
             ]
           ] = query("SELECT timestamp with time zone '2013-09-23 14:04:37.123'", [])

    assert [
             [
               %DateTime{
                 year: 2013,
                 month: 9,
                 day: 23,
                 hour: 22,
                 minute: 4,
                 second: 37,
                 microsecond: {0, 6},
                 time_zone: "Etc/UTC",
                 utc_offset: 0
               }
             ]
           ] = query("SELECT timestamp with time zone '2013-09-23 14:04:37 PST'", [])

    assert [
             [
               %DateTime{
                 year: 2013,
                 month: 9,
                 day: 23,
                 hour: 22,
                 minute: 4,
                 second: 37,
                 microsecond: {0, 6},
                 time_zone: "Etc/UTC",
                 utc_offset: 0
               }
             ]
           ] = query("SELECT timestamp with time zone '2013-09-23 14:04:37-8'", [])

    assert :ok = query("SET SESSION TIME ZONE +1", [])

    assert [
             [
               %DateTime{
                 year: 2013,
                 month: 9,
                 day: 23,
                 hour: 22,
                 minute: 4,
                 second: 37,
                 microsecond: {0, 6},
                 time_zone: "Etc/UTC",
                 utc_offset: 0
               }
             ]
           ] = query("SELECT timestamp with time zone '2013-09-23 14:04:37 PST'", [])

    assert [
             [
               %DateTime{
                 year: 2013,
                 month: 9,
                 day: 23,
                 hour: 22,
                 minute: 4,
                 second: 37,
                 microsecond: {0, 6},
                 time_zone: "Etc/UTC",
                 utc_offset: 0
               }
             ]
           ] = query("SELECT timestamp with time zone '2013-09-23 14:04:37-8'", [])

    assert [
             [
               %DateTime{
                 year: 1980,
                 month: 1,
                 day: 1,
                 hour: 0,
                 minute: 0,
                 second: 0,
                 microsecond: {123_456, 6},
                 time_zone: "Etc/UTC",
                 utc_offset: 0
               }
             ]
           ] = query("SELECT timestamp with time zone '1980-01-01 01:00:00.123456'", [])
  end

  test "decode negative timestampz", context do
    assert :ok = query("SET SESSION TIME ZONE UTC", [])

    assert [
             [
               %DateTime{
                 year: -1980,
                 month: 1,
                 day: 1,
                 hour: 0,
                 minute: 0,
                 second: 0,
                 microsecond: {123_456, 6},
                 time_zone: "Etc/UTC",
                 utc_offset: 0
               }
             ]
           ] = query("SELECT timestamp with time zone '1981-01-01BC 00:00:00.123456'", [])
  end

  @tag :capture_log
  test "decode infinity", context do
    assert_raise ArgumentError, fn -> query("SELECT 'infinity'::timestamp", []) end
    assert_raise ArgumentError, fn -> query("SELECT '-infinity'::timestamptz", []) end

    opts = [database: "postgrex_test", backoff_type: :stop, types: Postgrex.InfiniteCalendarTypes]
    {:ok, pid} = P.start_link(opts)

    assert {:ok, %{rows: [[:inf]]}} = P.query(pid, "SELECT 'infinity'::timestamp", [])
    assert {:ok, %{rows: [[:"-inf"]]}} = P.query(pid, "SELECT '-infinity'::timestamp", [])
    assert {:ok, %{rows: [[:inf]]}} = P.query(pid, "SELECT 'infinity'::timestamptz", [])
    assert {:ok, %{rows: [[:"-inf"]]}} = P.query(pid, "SELECT '-infinity'::timestamptz", [])
  end

  test "encode time", context do
    assert [["00:00:00"]] = query("SELECT $1::time::text", [~T[00:00:00.0000]])
    assert [["01:02:03"]] = query("SELECT $1::time::text", [~T[01:02:03]])
    assert [["23:59:59"]] = query("SELECT $1::time::text", [~T[23:59:59]])
    assert [["04:05:06.123456"]] = query("SELECT $1::time::text", [~T[04:05:06.123456]])
  end

  test "encode timetz", context do
    assert :ok = query("SET SESSION TIME ZONE UTC", [])

    assert [["00:00:00+00"]] = query("SELECT $1::timetz::text", [~T[00:00:00.0000]])
    assert [["01:02:03+00"]] = query("SELECT $1::timetz::text", [~T[01:02:03]])
    assert [["23:59:59+00"]] = query("SELECT $1::timetz::text", [~T[23:59:59]])
    assert [["04:05:06.123456+00"]] = query("SELECT $1::timetz::text", [~T[04:05:06.123456]])

    assert :ok = query("SET SESSION TIME ZONE +1", [])

    assert [["04:05:06.123456+00"]] = query("SELECT $1::timetz::text", [~T[04:05:06.123456]])
  end

  test "encode date", context do
    assert [["0001-01-01"]] = query("SELECT $1::date::text", [~D[0001-01-01]])
    assert [["0001-02-03"]] = query("SELECT $1::date::text", [~D[0001-02-03]])
    assert [["2013-09-23"]] = query("SELECT $1::date::text", [~D[2013-09-23]])
    assert [["1999-12-31"]] = query("SELECT $1::date::text", [~D[1999-12-31]])
  end

  test "encode non Calendar.ISO data types", context do
    defmodule OtherCalendar do
    end

    assert_raise DBConnection.EncodeError,
                 ~r/Postgrex expected a %Date{} in the `Calendar.ISO` calendar/,
                 fn ->
                   assert [["1999-12-31"]] =
                            query("SELECT $1::date::text", [
                              %{~D[1999-12-31] | calendar: OtherCalendar}
                            ])
                 end

    # Timestamp
    assert_raise DBConnection.EncodeError,
                 ~r/Postgrex expected a %NaiveDateTime{} in the `Calendar.ISO` calendar/,
                 fn ->
                   assert [["1999-12-31"]] =
                            query(
                              "SELECT $1::timestamp::text",
                              [%{~N[1999-12-31 11:00:00Z] | calendar: OtherCalendar}]
                            )
                 end

    # Timestampz
    assert_raise DBConnection.EncodeError,
                 ~r/Postgrex expected a %NaiveDateTime{} in the `Calendar.ISO` calendar/,
                 fn ->
                   assert [["1999-12-31"]] =
                            query(
                              "SELECT $1::timestamp with time zone::text",
                              [%{~N[1999-12-31 11:00:00Z] | calendar: OtherCalendar}]
                            )
                 end

    # Time
    assert_raise DBConnection.EncodeError,
                 ~r/Postgrex expected a %Time{} in the `Calendar.ISO` calendar/,
                 fn ->
                   assert [["1999-12-31"]] =
                            query(
                              "SELECT $1::time::text",
                              [%{~T[10:10:10] | calendar: OtherCalendar}]
                            )
                 end

    # Time with zone
    assert_raise DBConnection.EncodeError,
                 ~r/Postgrex expected a %Time{} in the `Calendar.ISO` calendar/,
                 fn ->
                   assert [["1999-12-31"]] =
                            query(
                              "SELECT $1::timetz::text",
                              [%{~T[10:10:10] | calendar: OtherCalendar}]
                            )
                 end
  end

  test "encode timestamp", context do
    assert [["2001-01-01 00:00:00"]] =
             query("SELECT $1::timestamp::text", [~N[2001-01-01 00:00:00.000000]])

    assert :ok = query("SET SESSION TIME ZONE UTC", [])

    assert [["2013-09-23 14:04:37.123"]] =
             query("SELECT $1::timestamp::text", [~N[2013-09-23 14:04:37.123000]])

    assert [["2013-09-23 14:04:37"]] =
             query("SELECT $1::timestamp::text", [~N[2013-09-23 14:04:37.000000]])

    assert :ok = query("SET SESSION TIME ZONE +1", [])

    assert [["2013-09-23 14:04:37"]] =
             query("SELECT $1::timestamp::text", [~N[2013-09-23 14:04:37.000000]])

    assert [["1980-01-01 00:00:00.123456"]] =
             query("SELECT $1::timestamp::text", [~N[1980-01-01 00:00:00.123456]])

    assert [["1980-01-01 00:00:00.123456"]] =
             query("SELECT $1::timestamp::text", [
               DateTime.from_naive!(~N[1980-01-01 00:00:00.123456], "Etc/UTC")
             ])
  end

  test "encode timestamptz", context do
    assert :ok = query("SET SESSION TIME ZONE UTC", [])

    assert [["2001-01-01 00:00:00+00"]] =
             query(
               "SELECT $1::timestamp with time zone::text",
               [
                 %DateTime{
                   year: 2001,
                   month: 1,
                   day: 1,
                   hour: 0,
                   minute: 0,
                   second: 0,
                   microsecond: {0, 6},
                   time_zone: "Etc/UTC",
                   zone_abbr: "UTC",
                   utc_offset: 0,
                   std_offset: 0
                 }
               ]
             )

    assert [["2013-09-23 14:04:37.123+00"]] =
             query(
               "SELECT $1::timestamp with time zone::text",
               [
                 %DateTime{
                   year: 2013,
                   month: 9,
                   day: 23,
                   hour: 14,
                   minute: 4,
                   second: 37,
                   microsecond: {123_000, 6},
                   time_zone: "Etc/UTC",
                   zone_abbr: "UTC",
                   utc_offset: 0,
                   std_offset: 0
                 }
               ]
             )

    assert_raise ArgumentError, ~r"is not in UTC", fn ->
      query(
        "SELECT $1::timestamp with time zone::text",
        [
          %DateTime{
            year: 2013,
            month: 9,
            day: 23,
            hour: 14,
            minute: 4,
            second: 37,
            microsecond: {123_000, 6},
            time_zone: "PST",
            zone_abbr: "PST",
            utc_offset: -8,
            std_offset: 0
          }
        ]
      )
    end

    assert :ok = query("SET SESSION TIME ZONE +1", [])

    assert [["2013-09-23 15:04:37.123+01"]] =
             query(
               "SELECT $1::timestamp with time zone::text",
               [
                 %DateTime{
                   year: 2013,
                   month: 9,
                   day: 23,
                   hour: 14,
                   minute: 4,
                   second: 37,
                   microsecond: {123_000, 6},
                   time_zone: "Etc/UTC",
                   zone_abbr: "UTC",
                   utc_offset: 0,
                   std_offset: 0
                 }
               ]
             )

    assert [["1980-01-01 01:00:00.123456+01"]] =
             query(
               "SELECT $1::timestamp with time zone::text",
               [
                 %DateTime{
                   year: 1980,
                   month: 1,
                   day: 1,
                   hour: 0,
                   minute: 0,
                   second: 0,
                   microsecond: {123_456, 6},
                   time_zone: "Etc/UTC",
                   zone_abbr: "UTC",
                   utc_offset: 0,
                   std_offset: 0
                 }
               ]
             )
  end

  test "persist timestamp and timestamptz", context do
    assert :ok = query("SET SESSION TIME ZONE +1", [])

    assert :ok =
             query(
               "INSERT INTO calendar VALUES (timestamp without time zone '2001-01-01 00:00:00', timestamp with time zone '2001-01-01 00:00:00 UTC')",
               []
             )

    assert [
             [
               ~N[2001-01-01 00:00:00.000000],
               %DateTime{year: 2001, month: 1, day: 1, hour: 0, minute: 0, second: 0}
             ]
           ] = query("SELECT a, b FROM calendar", [])

    assert :ok = query("SET SESSION TIME ZONE +2", [])

    assert [
             [
               ~N[2001-01-01 00:00:00.000000],
               %DateTime{year: 2001, month: 1, day: 1, hour: 0, minute: 0, second: 0}
             ]
           ] = query("SELECT a, b FROM calendar", [])
  end
end
