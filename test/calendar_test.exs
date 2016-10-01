if Code.ensure_loaded?(Calendar) do
  defmodule CalendarTest do
    use ExUnit.Case, async: true
    import Postgrex.TestHelper
    alias Postgrex, as: P

    setup do
      opts = [database: "postgrex_test", backoff_type: :stop,
              extensions: [{Postgrex.Extensions.Calendar, []}]]
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

      assert [[~T[01:02:03.123456]]] = query("SELECT time with time zone '16:01:03.123456+1459'", [])

      assert [[~T[16:01:03.123456]]] = query("SELECT time with time zone '01:02:03.123456-1459'", [])

      assert :ok = query("SET SESSION TIME ZONE +1", [])

      assert [[~T[12:05:06.000000]]] = query("SELECT time with time zone '04:05:06 PST'", [])
      assert [[~T[12:05:06.000000]]] = query("SELECT time with time zone '04:05:06-8'", [])
      assert [[~T[01:02:03.123456]]] = query("SELECT time with time zone '01:02:03.123456 UTC'", [])
    end

    test "decode date", context do
      assert [[~D[0001-01-01]]] = query("SELECT date '0001-01-01'", [])
      assert [[~D[0001-02-03]]] = query("SELECT date '0001-02-03'", [])
      assert [[~D[2013-09-23]]] = query("SELECT date '2013-09-23'", [])
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
    end

    test "decode timestamptz", context do
      assert [[%DateTime{year: 2001, month: 1, day: 1, hour: 0, minute: 0,
                         second: 0, microsecond: {0, 6},
                         time_zone: "Etc/UTC", utc_offset: 0}]] =
       query("SELECT timestamp with time zone '2001-01-01 00:00:00 UTC'", [])

      assert :ok = query("SET SESSION TIME ZONE UTC", [])
      assert [[%DateTime{year: 2013, month: 9, day: 23, hour: 14, minute: 4,
                         second: 37, microsecond: {123000, 6},
                         time_zone: "Etc/UTC", utc_offset: 0}]] =
      query("SELECT timestamp with time zone '2013-09-23 14:04:37.123'", [])

      assert [[%DateTime{year: 2013, month: 9, day: 23, hour: 22, minute: 4,
                         second: 37, microsecond: {0, 6},
                         time_zone: "Etc/UTC", utc_offset: 0}]] =
      query("SELECT timestamp with time zone '2013-09-23 14:04:37 PST'", [])
      assert [[%DateTime{year: 2013, month: 9, day: 23, hour: 22, minute: 4,
                         second: 37, microsecond: {0, 6},
                         time_zone: "Etc/UTC", utc_offset: 0}]] =
      query("SELECT timestamp with time zone '2013-09-23 14:04:37-8'", [])

      assert :ok = query("SET SESSION TIME ZONE +1", [])

      assert [[%DateTime{year: 2013, month: 9, day: 23, hour: 22, minute: 4,
                         second: 37, microsecond: {0, 6},
                         time_zone: "Etc/UTC", utc_offset: 0}]] =
        query("SELECT timestamp with time zone '2013-09-23 14:04:37 PST'", [])
      assert [[%DateTime{year: 2013, month: 9, day: 23, hour: 22, minute: 4,
                         second: 37, microsecond: {0, 6},
                         time_zone: "Etc/UTC", utc_offset: 0}]] =
        query("SELECT timestamp with time zone '2013-09-23 14:04:37-8'", [])

      assert [[%DateTime{year: 1980, month: 1, day: 1, hour: 0, minute: 0,
                         second: 0, microsecond: {123456, 6},
                         time_zone: "Etc/UTC", utc_offset: 0}]] =
        query("SELECT timestamp with time zone '1980-01-01 01:00:00.123456'", [])
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
    end

    test "encode timestamptz", context do
      assert :ok = query("SET SESSION TIME ZONE UTC", [])

      assert [["2001-01-01 00:00:00+00"]] =
        query("SELECT $1::timestamp with time zone::text",
              [%DateTime{year: 2001, month: 1, day: 1, hour: 0, minute: 0,
                  second: 0, microsecond: {0, 6}, time_zone: "Etc/UTC",
                  zone_abbr: "UTC", utc_offset: 0, std_offset: 0}])

      assert [["2013-09-23 14:04:37.123+00"]] =
        query("SELECT $1::timestamp with time zone::text",
              [%DateTime{year: 2013, month: 9, day: 23, hour: 14, minute: 4,
                         second: 37, microsecond: {123000, 6},
                         time_zone: "Etc/UTC", zone_abbr: "UTC", utc_offset: 0,
                         std_offset: 0}])

      assert_raise ArgumentError, ~r"is not in UTC",
        fn() ->
          query("SELECT $1::timestamp with time zone::text",
              [%DateTime{year: 2013, month: 9, day: 23, hour: 14, minute: 4,
                         second: 37, microsecond: {123000, 6},
                         time_zone: "PST", zone_abbr: "PST", utc_offset: -8,
                         std_offset: 0}])
        end

      assert :ok = query("SET SESSION TIME ZONE +1", [])

      assert [["2013-09-23 15:04:37.123+01"]] =
        query("SELECT $1::timestamp with time zone::text",
              [%DateTime{year: 2013, month: 9, day: 23, hour: 14, minute: 4,
                         second: 37, microsecond: {123000, 6},
                         time_zone: "Etc/UTC", zone_abbr: "UTC", utc_offset: 0,
                         std_offset: 0}])

      assert [["1980-01-01 01:00:00.123456+01"]] =
        query("SELECT $1::timestamp with time zone::text",
              [%DateTime{year: 1980, month: 1, day: 1, hour: 0, minute: 0,
                         second: 0, microsecond: {123456, 6},
                         time_zone: "Etc/UTC", zone_abbr: "UTC", utc_offset: 0,
                         std_offset: 0}])
    end

    test "persit timestamp and timestamptz", context  do
      assert :ok = query("SET SESSION TIME ZONE +1", [])
      assert :ok = query("INSERT INTO calendar VALUES (timestamp without time zone '2001-01-01 00:00:00', timestamp with time zone '2001-01-01 00:00:00 UTC')", [])

      assert [[~N[2001-01-01 00:00:00.000000],
               %DateTime{year: 2001,  month: 1, day: 1, hour: 0, minute: 0,
                         second: 0}]] =
        query("SELECT a, b FROM calendar", [])

      assert :ok = query("SET SESSION TIME ZONE +2", [])

      assert [[~N[2001-01-01 00:00:00.000000],
               %DateTime{year: 2001,  month: 1, day: 1, hour: 0, minute: 0,
                         second: 0}]] =
        query("SELECT a, b FROM calendar", [])
    end
  end
end
