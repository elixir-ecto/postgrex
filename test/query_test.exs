defmodule QueryTest do
  use ExUnit.Case, async: true
  import Postgrex.TestHelper
  alias Postgrex.Connection, as: P

  setup do
    opts = [ database: "postgrex_test", backoff_type: :stop ]
    {:ok, pid} = P.start_link(opts)
    {:ok, [pid: pid]}
  end

  test "iodata", context do
    assert [[123]] = query(["S", ?E, ["LEC"|"T"], " ", '123'], [])
  end

  test "decode basic types", context do
    assert [[nil]] = query("SELECT NULL", [])
    assert [[true, false]] = query("SELECT true, false", [])
    assert [["e"]] = query("SELECT 'e'::char", [])
    assert [["ẽ"]] = query("SELECT 'ẽ'::char", [])
    assert [[42]] = query("SELECT 42", [])
    assert [[42.0]] = query("SELECT 42::float", [])
    assert [[:NaN]] = query("SELECT 'NaN'::float", [])
    assert [[:inf]] = query("SELECT 'inf'::float", [])
    assert [[:"-inf"]] = query("SELECT '-inf'::float", [])
    assert [["ẽric"]] = query("SELECT 'ẽric'", [])
    assert [["ẽric"]] = query("SELECT 'ẽric'::varchar", [])
    assert [[<<1, 2, 3>>]] = query("SELECT '\\001\\002\\003'::bytea", [])
  end

  test "decode numeric", context do
    assert [[Decimal.new("42")]] == query("SELECT 42::numeric", [])
    assert [[Decimal.new("42.0000000000")]] == query("SELECT 42.0::numeric(100, 10)", [])
    assert [[Decimal.new("1.001")]] == query("SELECT 1.001", [])
    assert [[Decimal.new("0.4242")]] == query("SELECT 0.4242", [])
    assert [[Decimal.new("42.4242")]] == query("SELECT 42.4242", [])
    assert [[Decimal.new("12345.12345")]] == query("SELECT 12345.12345", [])
    assert [[Decimal.new("0.00012345")]] == query("SELECT 0.00012345", [])
    assert [[Decimal.new("1000000000.0")]] == query("SELECT 1000000000.0", [])
    assert [[Decimal.new("1000000000.1")]] == query("SELECT 1000000000.1", [])
    assert [[Decimal.new("123456789123456789123456789")]] == query("SELECT 123456789123456789123456789::numeric", [])
    assert [[Decimal.new("123456789123456789123456789.123456789")]] == query("SELECT 123456789123456789123456789.123456789", [])
    assert [[Decimal.new("1.1234500000")]] == query("SELECT 1.1234500000", [])
    assert [[Decimal.new("NaN")]] == query("SELECT 'NaN'::numeric", [])
  end

  test "decode uuid", context do
    uuid = <<160,238,188,153,156,11,78,248,187,109,107,185,189,56,10,17>>
    assert [[^uuid]] = query("SELECT 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid", [])
  end

  test "decode arrays", context do
    assert [[[]]] = query("SELECT ARRAY[]::integer[]", [])
    assert [[[1]]] = query("SELECT ARRAY[1]", [])
    assert [[[1,2]]] = query("SELECT ARRAY[1,2]", [])
    assert [[[[0],[1]]]] = query("SELECT ARRAY[[0],[1]]", [])
    assert [[[[0]]]] = query("SELECT ARRAY[ARRAY[0]]", [])
  end

  test "decode time", context do
    assert [[%Postgrex.Time{hour: 0, min: 0, sec: 0, usec: 0}]] =
           query("SELECT time '00:00:00'", [])
    assert [[%Postgrex.Time{hour: 1, min: 2, sec: 3, usec: 0}]] =
           query("SELECT time '01:02:03'", [])
    assert [[%Postgrex.Time{hour: 23, min: 59, sec: 59, usec: 0}]] =
           query("SELECT time '23:59:59'", [])
    assert [[%Postgrex.Time{hour: 4, min: 5, sec: 6, usec: 0}]] =
           query("SELECT time '04:05:06 PST'", [])

    assert [[%Postgrex.Time{hour: 0, min: 0, sec: 0, usec: 123000}]] =
           query("SELECT time '00:00:00.123'", [])
    assert [[%Postgrex.Time{hour: 0, min: 0, sec: 0, usec: 123456}]] =
           query("SELECT time '00:00:00.123456'", [])
    assert [[%Postgrex.Time{hour: 1, min: 2, sec: 3, usec: 123456}]] =
           query("SELECT time '01:02:03.123456'", [])
  end

  test "decode date", context do
    assert [[%Postgrex.Date{year: 1, month: 1, day: 1}]] =
           query("SELECT date '0001-01-01'", [])
    assert [[%Postgrex.Date{year: 1, month: 2, day: 3}]] =
           query("SELECT date '0001-02-03'", [])
    assert [[%Postgrex.Date{year: 2013, month: 9, day: 23}]] =
           query("SELECT date '2013-09-23'", [])
  end

  test "decode timestamp", context do
    assert [[%Postgrex.Timestamp{year: 2001, month: 1, day: 1, hour: 0, min: 0, sec: 0, usec: 0}]] =
           query("SELECT timestamp '2001-01-01 00:00:00'", [])
    assert [[%Postgrex.Timestamp{year: 2013, month: 9, day: 23, hour: 14, min: 4, sec: 37, usec: 123000}]] =
           query("SELECT timestamp '2013-09-23 14:04:37.123'", [])
    assert [[%Postgrex.Timestamp{year: 2013, month: 9, day: 23, hour: 14, min: 4, sec: 37, usec: 0}]] =
           query("SELECT timestamp '2013-09-23 14:04:37 PST'", [])
    assert [[%Postgrex.Timestamp{year: 1, month: 1, day: 1, hour: 0, min: 0, sec: 0, usec: 123456}]] =
           query("SELECT timestamp '0001-01-01 00:00:00.123456'", [])

  end

  test "decode interval", context do
    assert [[%Postgrex.Interval{months: 0, days: 0, secs: 0}]] =
           query("SELECT interval '0'", [])
    assert [[%Postgrex.Interval{months: 100, days: 0, secs: 0}]] =
           query("SELECT interval '100 months'", [])
    assert [[%Postgrex.Interval{months: 0, days: 100, secs: 0}]] =
           query("SELECT interval '100 days'", [])
    assert [[%Postgrex.Interval{months: 0, days: 0, secs: 100}]] =
           query("SELECT interval '100 secs'", [])
    assert [[%Postgrex.Interval{months: 14, days: 40, secs: 10920}]] =
           query("SELECT interval '1 year 2 months 40 days 3 hours 2 minutes'", [])
  end

  test "decode record", context do
    assert [[{1, "2"}]] = query("SELECT (1, '2')::composite1", [])
    assert [[[{1, "2"}]]] = query("SELECT ARRAY[(1, '2')::composite1]", [])
  end

  test "decode enum", context do
    assert [["elixir"]] = query("SELECT 'elixir'::enum1", [])
  end

  @tag min_pg_version: "9.2"
  test "decode range", context do
    assert [[%Postgrex.Range{lower: 2, upper: 5, lower_inclusive: true, upper_inclusive: false}]] =
           query("SELECT '(1,5)'::int4range", [])
    assert [[%Postgrex.Range{lower: 1, upper: 7, lower_inclusive: true, upper_inclusive: false}]] =
           query("SELECT '[1,6]'::int4range", [])
    assert [[%Postgrex.Range{lower: nil, upper: 5, lower_inclusive: false, upper_inclusive: false}]] =
           query("SELECT '(,5)'::int4range", [])
    assert [[%Postgrex.Range{lower: 1, upper: nil, lower_inclusive: true, upper_inclusive: false}]] =
           query("SELECT '[1,)'::int4range", [])
    assert [[%Postgrex.Range{lower: nil, upper: nil, lower_inclusive: false, upper_inclusive: false}]] =
           query("SELECT '(,)'::int4range", [])
    assert [[%Postgrex.Range{lower: nil, upper: nil, lower_inclusive: false, upper_inclusive: false}]] =
           query("SELECT '[,]'::int4range", [])

    assert [[%Postgrex.Range{lower: 3, upper: 8, lower_inclusive: true, upper_inclusive: false}]] =
           query("SELECT '(2,8)'::int8range", [])

    assert [[%Postgrex.Range{lower: Decimal.new("1.2"), upper: Decimal.new("3.4"), lower_inclusive: false, upper_inclusive: false}]] ==
           query("SELECT '(1.2,3.4)'::numrange", [])

    assert [[%Postgrex.Range{lower: %Postgrex.Date{year: 2014, month: 1, day: 1}, upper: %Postgrex.Date{year: 2014, month: 12, day: 31}}]] =
           query("SELECT '[2014-1-1,2014-12-31)'::daterange", [])
    assert [[%Postgrex.Range{lower: nil, upper: %Postgrex.Date{year: 2014, month: 12, day: 31}}]] =
           query("SELECT '(,2014-12-31)'::daterange", [])
    assert [[%Postgrex.Range{lower: %Postgrex.Date{year: 2014, month: 1, day: 2}, upper: nil}]] =
           query("SELECT '(2014-1-1,]'::daterange", [])
  end

  @tag min_pg_version: "9.0"
  test "decode network types", context do
    assert [[%Postgrex.INET{address: {127, 0, 0, 1}}]] =
           query("SELECT '127.0.0.1'::inet", [])
    assert [[%Postgrex.INET{address: {8193, 43981, 0, 0, 0, 0, 0, 0}}]] =
           query("SELECT '2001:abcd::'::inet", [])
    assert [[%Postgrex.CIDR{address: {127, 0, 0, 1}, netmask: 32}]] =
           query("SELECT '127.0.0.1/32'::cidr", [])
    assert [[%Postgrex.CIDR{address: {8193, 43981, 0, 0, 0, 0, 0, 0}, netmask: 128}]] =
           query("SELECT '2001:abcd::/128'::cidr", [])
    assert [[%Postgrex.MACADDR{address: {8, 1, 43, 5, 7, 9}}]] =
           query("SELECT '08:01:2b:05:07:09'::macaddr", [])
  end

  test "decode oid and its aliases", context do
    assert [[4294967295]] = query("select 4294967295::oid;", [])

    assert [["-"]] = query("select '-'::regproc::text;", [])
    assert [["sum(integer)"]] = query("select 'sum(int4)'::regprocedure::text;", [])
    assert [["||/"]] = query("select 'pg_catalog.||/'::regoper::text;", [])
    assert [["+(integer,integer)"]] = query("select '+(integer,integer)'::regoperator::text;", [])
    assert [["pg_type"]] = query("select 'pg_type'::regclass::text;", [])
    assert [["integer"]] = query("select 'int4'::regtype::text;", [])

    assert [[0]] = query("select '-'::regproc;", [])
    assert [[44]] = query("select 'regprocin'::regproc;", [])
    assert [[2108]] = query("select 'sum(int4)'::regprocedure;", [])
    assert [[597]] = query("select 'pg_catalog.||/'::regoper;", [])
    assert [[551]] = query("select '+(integer,integer)'::regoperator;", [])
    assert [[1247]] = query("select 'pg_type'::regclass;", [])
    assert [[23]] = query("select 'int4'::regtype;", [])

    # xid type
    assert [[xmin, xmax]] = query("select xmin, xmax from pg_type limit 1;", [])
    assert is_number(xmin) and is_number(xmax)

    # cid type
    assert [[cmin, cmax]] = query("select cmin, cmax from pg_type limit 1;", [])
    assert is_number(cmin) and is_number(cmax)
  end

  test "encode oid and its aliases", context do
    # oid's range is 0 to 4294967295
    assert [[0]] = query("select $1::oid;", [0])
    assert [[4294967295]] = query("select $1::oid;", [4294967295])
    assert %ArgumentError{} = catch_error(query("SELECT $1::oid", [0 - 1]))
    assert %ArgumentError{} = catch_error(query("SELECT $1::oid", [4294967295 + 1]))

    assert [["-"]] = query("select $1::regproc::text;", [0])
    assert [["regprocin"]] = query("select $1::regproc::text;", [44])
    assert [["sum(integer)"]] = query("select $1::regprocedure::text;", [2108])
    assert [["||/"]] = query("select $1::regoper::text;", [597])
    assert [["+(integer,integer)"]] = query("select $1::regoperator::text;", [551])
    assert [["pg_type"]] = query("select $1::regclass::text;", [1247])
    assert [["integer"]] = query("select $1::regtype::text;", [23])

    assert [[0]] = query("select $1::text::regproc;", ["-"])
    assert [[44]] = query("select $1::text::regproc;", ["regprocin"])
    assert [[2108]] = query("select $1::text::regprocedure;", ["sum(int4)"])
    assert [[597]] = query("select $1::text::regoper;", ["pg_catalog.||/"])
    assert [[551]] = query("select $1::text::regoperator;", ["+(integer,integer)"])
    assert [[1247]] = query("select $1::text::regclass;", ["pg_type"])
    assert [[23]] = query("select $1::text::regtype;", ["int4"])
  end

  test "tuple ids", context do
    assert [[_tid]] = query("select ctid from pg_type limit 1;", [])
    assert [[{5, 10}]] = query("select $1::tid;", [{5, 10}])
  end

  test "encoding oids as binary fails with a helpful error message", context do
    assert %Postgrex.Error{message: message} = catch_error(query("select $1::regclass;", ["pg_type"]))
    assert message =~ "See https://github.com/ericmj/postgrex#oid-type-encoding"
  end

  test "fail on encoding wrong value", context do
    assert %ArgumentError{message: message} = catch_error(query("SELECT $1::integer", ["123"]))
    assert message =~ "Postgrex expected a term that can be encoded/cast to type \"int4\""
  end

  @tag min_pg_version: "9.0"
  test "decode hstore", context do
    assert [[%{}]] = query(~s|SELECT ''::hstore|, [])
    assert [[%{"Bubbles" => "7", "Name" => "Frank"}]] = query(~s|SELECT '"Name" => "Frank", "Bubbles" => "7"'::hstore|, [])
    assert [[%{"non_existant" => nil, "present" => "&accounted_for"}]] = query(~s|SELECT '"non_existant" => NULL, "present" => "&accounted_for"'::hstore|, [])
    assert [[%{"spaces in the key" => "are easy!", "floats too" => "66.6"}]] = query(~s|SELECT '"spaces in the key" => "are easy!", "floats too" => "66.6"'::hstore|, [])
    assert [[%{"this is true" => "true", "though not this" => "false"}]] = query(~s|SELECT '"this is true" => "true", "though not this" => "false"'::hstore|, [])
  end

  test "encode basic types", context do
    assert [[nil, nil]] = query("SELECT $1::text, $2::int", [nil, nil])
    assert [[true, false]] = query("SELECT $1::bool, $2::bool", [true, false])
    assert [["ẽ"]] = query("SELECT $1::char", ["ẽ"])
    assert [[42]] = query("SELECT $1::int", [42])
    assert [[42.0, 43.0]] = query("SELECT $1::float, $2::float", [42, 43.0])
    assert [[:NaN]] = query("SELECT $1::float", [:NaN])
    assert [[:inf]] = query("SELECT $1::float", [:inf])
    assert [[:"-inf"]] = query("SELECT $1::float", [:"-inf"])
    assert [["ẽric"]] = query("SELECT $1::varchar", ["ẽric"])
    assert [[<<1, 2, 3>>]] = query("SELECT $1::bytea", [<<1, 2, 3>>])
  end

  test "encode numeric", context do
    nums = [
      "42",
      "0.4242",
      "42.4242",
      "1.001",
      "0.01",
      "0.00012345",
      "1000000000",
      "1000000000.0",
      "123456789123456789123456789",
      "123456789123456789123456789.123456789",
      "1.1234500000",
      "1.0000000000",
      "NaN"
    ]

    Enum.each(nums, fn num ->
      dec = Decimal.new(num)
      assert [[dec]] == query("SELECT $1::numeric", [dec])
    end)
  end

  test "encode integers and floats as numeric", context do
    dec = Decimal.new(1)
    assert [[dec]] == query("SELECT $1::numeric", [1])

    dec = Decimal.new(1.0)
    assert [[dec]] == query("SELECT $1::numeric", [1.0])
  end

  test "encode enforces bounds on integers", context do
    # int2's range is -32768 to +32767
    assert [[-32768]] = query("SELECT $1::int2", [-32768])
    assert [[32767]] = query("SELECT $1::int2", [32767])
    assert %ArgumentError{} = catch_error(query("SELECT $1::int2", [32767 + 1]))
    assert %ArgumentError{} = catch_error(query("SELECT $1::int2", [-32768 - 1]))

    # int4's range is -2147483648 to +2147483647
    assert [[-2147483648]] = query("SELECT $1::int4", [-2147483648])
    assert [[2147483647]] = query("SELECT $1::int4", [2147483647])
    assert %ArgumentError{} = catch_error(query("SELECT $1::int4", [2147483647 + 1]))
    assert %ArgumentError{} = catch_error(query("SELECT $1::int4", [-2147483648 - 1]))

    # int8's range is  -9223372036854775808 to 9223372036854775807
    assert [[-9223372036854775808]] = query("SELECT $1::int8", [-9223372036854775808])
    assert [[9223372036854775807]] = query("SELECT $1::int8", [9223372036854775807])
    assert %ArgumentError{} = catch_error(query("SELECT $1::int8", [9223372036854775807 + 1]))
    assert %ArgumentError{} = catch_error(query("SELECT $1::int8", [-9223372036854775808 - 1]))
  end

  test "encode uuid", context do
    uuid = <<0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15>>
    assert [[^uuid]] = query("SELECT $1::uuid", [uuid])
  end

  test "encode date", context do
    assert [[%Postgrex.Date{year: 1, month: 1, day: 1}]] =
           query("SELECT $1::date", [%Postgrex.Date{year: 1, month: 1, day: 1}])
    assert [[%Postgrex.Date{year: 1, month: 2, day: 3}]] =
           query("SELECT $1::date", [%Postgrex.Date{year: 1, month: 2, day: 3}])
    assert [[%Postgrex.Date{year: 2013, month: 9, day: 23}]] =
           query("SELECT $1::date", [%Postgrex.Date{year: 2013, month: 9, day: 23}])
    assert [[%Postgrex.Date{year: 1999, month: 12, day: 31}]] =
           query("SELECT $1::date", [%Postgrex.Date{year: 1999, month: 12, day: 31}])
    assert [[%Postgrex.Date{year: 1999, month: 12, day: 31}]] =
           query("SELECT $1::date", [%Postgrex.Date{year: 1999, month: 12, day: 31}])
  end

  test "encode time", context do
    assert [[%Postgrex.Time{hour: 0, min: 0, sec: 0}]] =
           query("SELECT $1::time", [%Postgrex.Time{hour: 0, min: 0, sec: 0}])
    assert [[%Postgrex.Time{hour: 1, min: 2, sec: 3}]] =
           query("SELECT $1::time", [%Postgrex.Time{hour: 1, min: 2, sec: 3}])
    assert [[%Postgrex.Time{hour: 23, min: 59, sec: 59}]] =
           query("SELECT $1::time", [%Postgrex.Time{hour: 23, min: 59, sec: 59}])
    assert [[%Postgrex.Time{hour: 4, min: 5, sec: 6, usec: 123456}]] =
           query("SELECT $1::time", [%Postgrex.Time{hour: 4, min: 5, sec: 6, usec: 123456}])
  end

  test "encode timestamp", context do
    assert [[%Postgrex.Timestamp{year: 1, month: 1, day: 1, hour: 0, min: 0, sec: 0}]] =
      query("SELECT $1::timestamp", [%Postgrex.Timestamp{year: 1, month: 1, day: 1, hour: 0, min: 0, sec: 0}])
    assert [[%Postgrex.Timestamp{year: 2013, month: 9, day: 23, hour: 14, min: 4, sec: 37}]] =
      query("SELECT $1::timestamp", [%Postgrex.Timestamp{year: 2013, month: 9, day: 23, hour: 14, min: 4, sec: 37}])
    assert [[%Postgrex.Timestamp{year: 1, month: 1, day: 1, hour: 0, min: 0, sec: 0, usec: 123456}]] =
      query("SELECT $1::timestamp", [%Postgrex.Timestamp{year: 1, month: 1, day: 1, hour: 0, min: 0, sec: 0, usec: 123456}])
  end

  test "encode interval", context do
    assert [[%Postgrex.Interval{months: 0, days: 0, secs: 0}]] =
           query("SELECT $1::interval", [%Postgrex.Interval{months: 0, days: 0, secs: 0}])
    assert [[%Postgrex.Interval{months: 100, days: 0, secs: 0}]] =
           query("SELECT $1::interval", [%Postgrex.Interval{months: 100, days: 0, secs: 0}])
    assert [[%Postgrex.Interval{months: 0, days: 100, secs: 0}]] =
           query("SELECT $1::interval", [%Postgrex.Interval{months: 0, days: 100, secs: 0}])
    assert [[%Postgrex.Interval{months: 0, days: 0, secs: 100}]] =
           query("SELECT $1::interval", [%Postgrex.Interval{months: 0, days: 0, secs: 100}])
    assert [[%Postgrex.Interval{months: 14, days: 40, secs: 10920}]] =
           query("SELECT $1::interval", [%Postgrex.Interval{months: 14, days: 40, secs: 10920}])
  end

  test "encode arrays", context do
    assert [[[]]] = query("SELECT $1::integer[]", [[]])
    assert [[[1]]] = query("SELECT $1::integer[]", [[1]])
    assert [[[1,2]]] = query("SELECT $1::integer[]", [[1,2]])
    assert [[[[0],[1]]]] = query("SELECT $1::integer[]", [[[0],[1]]])
    assert [[[[0]]]] = query("SELECT $1::integer[]", [[[0]]])
    assert [[[1, nil, 3]]] = query("SELECT $1::integer[]", [[1, nil, 3]])
  end

  test "encode record", context do
    assert [[{1, "2"}]] = query("SELECT $1::composite1", [{1, "2"}])
    assert [[[{1, "2"}]]] = query("SELECT $1::composite1[]", [[{1, "2"}]])
    assert [[{1, nil, 3}]] = query("SELECT $1::composite2", [{1, nil, 3}])
  end

  test "encode enum", context do
    assert [["elixir"]] = query("SELECT $1::enum1", ["elixir"])
  end

  @tag min_pg_version: "9.2"
  test "encode range", context do
    assert [[%Postgrex.Range{lower: 1, upper: 4, lower_inclusive: true, upper_inclusive: false}]] =
           query("SELECT $1::int4range", [%Postgrex.Range{lower: 1, upper: 3, lower_inclusive: true, upper_inclusive: true}])
    assert [[%Postgrex.Range{lower: nil, upper: 6, lower_inclusive: false, upper_inclusive: false}]] =
           query("SELECT $1::int4range", [%Postgrex.Range{lower: nil, upper: 5, lower_inclusive: false, upper_inclusive: true}])
    assert [[%Postgrex.Range{lower: 3, upper: nil, lower_inclusive: true, upper_inclusive: false}]] =
           query("SELECT $1::int4range", [%Postgrex.Range{lower: 3, upper: nil, lower_inclusive: true, upper_inclusive: true}])
    assert [[%Postgrex.Range{lower: 4, upper: 5, lower_inclusive: true, upper_inclusive: false}]] =
           query("SELECT $1::int4range", [%Postgrex.Range{lower: 3, upper: 5, lower_inclusive: false, upper_inclusive: false}])

    assert [[%Postgrex.Range{lower: 1, upper: 4, lower_inclusive: true, upper_inclusive: false}]] =
           query("SELECT $1::int8range", [%Postgrex.Range{lower: 1, upper: 3, lower_inclusive: true, upper_inclusive: true}])

    assert [[%Postgrex.Range{lower: Decimal.new("1.2"), upper: Decimal.new("3.4"), lower_inclusive: true, upper_inclusive: true}]] ==
           query("SELECT $1::numrange", [%Postgrex.Range{lower: Decimal.new("1.2"), upper: Decimal.new("3.4"), lower_inclusive: true, upper_inclusive: true}])

    assert [[%Postgrex.Range{lower: %Postgrex.Date{year: 2014, month: 1, day: 1}, upper: %Postgrex.Date{year: 2015, month: 1, day: 1}}]] =
           query("SELECT $1::daterange", [%Postgrex.Range{lower: %Postgrex.Date{year: 2014, month: 1, day: 1}, upper: %Postgrex.Date{year: 2014, month: 12, day: 31}}])
    assert [[%Postgrex.Range{lower: nil, upper: %Postgrex.Date{year: 2015, month: 1, day: 1}}]] =
           query("SELECT $1::daterange", [%Postgrex.Range{lower: nil, upper: %Postgrex.Date{year: 2014, month: 12, day: 31}}])
    assert [[%Postgrex.Range{lower: %Postgrex.Date{year: 2014, month: 1, day: 1}, upper: nil}]] =
           query("SELECT $1::daterange", [%Postgrex.Range{lower: %Postgrex.Date{year: 2014, month: 1, day: 1}, upper: nil}])
  end

  @tag min_pg_version: "9.2"
  test "encode enforces bounds on integer ranges", context do
    # int4's range is -2147483648 to +2147483647
    assert [[%Postgrex.Range{lower: -2147483648}]] = query("SELECT $1::int4range", [%Postgrex.Range{lower: -2147483648}])
    assert [[%Postgrex.Range{upper: 2147483647}]] = query("SELECT $1::int4range", [%Postgrex.Range{upper: 2147483647, upper_inclusive: false}])
    assert %ArgumentError{} = catch_error(query("SELECT $1::int4range", [%Postgrex.Range{lower: -2147483649}]))
    assert %ArgumentError{} = catch_error(query("SELECT $1::int4range", [%Postgrex.Range{upper: 2147483648}]))

    # int8's range is -9223372036854775808 to 9223372036854775807
    assert [[%Postgrex.Range{lower: -9223372036854775807}]] = query("SELECT $1::int8range", [%Postgrex.Range{lower: -9223372036854775807}])
    assert [[%Postgrex.Range{upper: 9223372036854775806}]] = query("SELECT $1::int8range", [%Postgrex.Range{upper: 9223372036854775806, upper_inclusive: false}])
    assert %ArgumentError{} = catch_error(query("SELECT $1::int8range", [%Postgrex.Range{lower: -9223372036854775809}]))
    assert %ArgumentError{} = catch_error(query("SELECT $1::int8range", [%Postgrex.Range{upper: 9223372036854775808}]))
  end

  @tag min_pg_version: "9.0"
  test "encode hstore", context do
    assert [[%{"name" => "Frank", "bubbles" => "7", "limit" => nil, "chillin"=> "true", "fratty"=> "false", "atom" => "bomb"}]] =
           query ~s(SELECT $1::hstore), [%{"name" => "Frank", "bubbles" => "7", "limit" => nil, "chillin"=> "true", "fratty"=> "false", "atom" => "bomb"}]
  end

  @tag min_pg_version: "9.0"
  test "encode network types", context do
    assert [[%Postgrex.INET{address: {127, 0, 0, 1}}]] =
           query("SELECT $1::inet", [%Postgrex.INET{address: {127, 0, 0, 1}}])
    assert [[%Postgrex.INET{address: {8193, 43981, 0, 0, 0, 0, 0, 0}}]] =
           query("SELECT $1::inet", [%Postgrex.INET{address: {8193, 43981, 0, 0, 0, 0, 0, 0}}])
    assert [[%Postgrex.CIDR{address: {127, 0, 0, 1}, netmask: 32}]] =
           query("SELECT $1::cidr", [%Postgrex.CIDR{address: {127, 0, 0, 1}, netmask: 32}])
    assert [[%Postgrex.CIDR{address: {8193, 43981, 0, 0, 0, 0, 0, 0}, netmask: 128}]] =
           query("SELECT $1::cidr", [%Postgrex.CIDR{address: {8193, 43981, 0, 0, 0, 0, 0, 0}, netmask: 128}])
    assert [[%Postgrex.MACADDR{address: {8, 1, 43, 5, 7, 9}}]] =
           query("SELECT $1::macaddr", [%Postgrex.MACADDR{address: {8, 1, 43, 5, 7, 9}}])
  end

  test "fail on encode arrays", context do
    assert_raise ArgumentError, "nested lists must have lists with matching lengths", fn ->
      query("SELECT $1::integer[]", [[[1], [1,2]]])
    end
    assert [[42]] = query("SELECT 42", [])
  end

  test "fail on parameter length mismatch", context do
    assert_raise ArgumentError, ~r"parameters must be of length 1 for query", fn ->
      query("SELECT $1::integer", [1, 2])
    end

    assert_raise ArgumentError, ~r"parameters must be of length 0 for query", fn ->
      query("SELECT 42", [1])
    end

    assert [[42]] = query("SELECT 42", [])
  end

  test "non data statement", context do
    assert :ok = query("BEGIN", [])
    assert :ok = query("COMMIT", [])
  end

  test "result struct", context do
    assert {:ok, res} = P.query(context[:pid], "SELECT 123 AS a, 456 AS b", [])
    assert %Postgrex.Result{} = res
    assert res.command == :select
    assert res.columns == ["a", "b"]
    assert res.num_rows == 1
  end

  test "error struct", context do
    assert {:error, %Postgrex.Error{}} = P.query(context[:pid], "SELECT 123 + 'a'", [])
  end

  test "multi row result struct", context do
    assert {:ok, res} = P.query(context[:pid], "VALUES (1, 2), (3, 4)", [])
    assert res.num_rows == 2
    assert res.rows == [[1, 2], [3, 4]]
  end

  test "multi row result struct with decode mapper", context do
    map = &Enum.map(&1, fn x -> x * 2 end)
    assert [[2,4], [6,8]] = query("VALUES (1, 2), (3, 4)", [], decode_mapper: map)
  end

  test "insert", context do
    :ok = query("CREATE TABLE test (id int, text text)", [])
    [] = query("SELECT * FROM test", [])
    :ok = query("INSERT INTO test VALUES ($1, $2)", [42, "fortytwo"], [])
    [[42, "fortytwo"]] = query("SELECT * FROM test", [])
  end

  test "prepare, execute and close", context do
    assert (%Postgrex.Query{} = query) = prepare("42", "SELECT 42")
    assert [[42]] = execute(query, [])
    assert [[42]] = execute(query, [])
    assert :ok = close(query)
    assert [[42]] = query("SELECT 42", [])
  end

  test "prepare, close and execute", context do
    assert (%Postgrex.Query{} = query) = prepare("reuse", "SELECT $1::int")
    assert [[42]] = execute(query, [42])
    assert :ok = close(query)
    assert [[42]] = execute(query, [42])
  end

  test "execute with encode mapper", context do
    assert (%Postgrex.Query{} = query) = prepare("mapper", "SELECT $1::int")
    assert [[84]] = execute(query, [42], [encode_mapper: fn(n) -> n * 2 end])
    assert :ok = close(query)
    assert [[42]] = query("SELECT 42", [])
  end

  test "closing prepared query that does not exist succeeds", context do
    assert (%Postgrex.Query{} = query) = prepare("42", "SELECT 42")
    assert :ok = close(query)
    assert :ok = close(query)
  end

  test "error codes are translated", context  do
    assert %Postgrex.Error{postgres: %{code: :syntax_error}} = query("wat", [])
  end

  test "connection works after failure in parsing state", context do
    assert %Postgrex.Error{} = query("wat", [])
    assert [[42]] = query("SELECT 42", [])
  end

  test "connection works after failure in binding state", context do
    assert %Postgrex.Error{postgres: %{code: :invalid_text_representation}} =
      query("insert into uniques values (CAST($1::text AS int))", ["invalid"])
    assert [[42]] = query("SELECT 42", [])
  end

  test "connection works after failure in executing state", context do
    assert %Postgrex.Error{postgres: %{code: :unique_violation}} =
      query("insert into uniques values (1), (1);", [])
    assert [[42]] = query("SELECT 42", [])
  end

  test "connection works after failure during transaction", context do
    assert :ok = query("BEGIN", [])
    assert %Postgrex.Error{postgres: %{code: :unique_violation}} =
      query("insert into uniques values (1), (1);", [])
    assert %Postgrex.Error{postgres: %{code: :in_failed_sql_transaction}} =
      query("SELECT 42", [])
    assert :ok = query("ROLLBACK", [])
    assert [[42]] = query("SELECT 42", [])
  end

  test "connection works on custom transactions", context do
    assert :ok = query("BEGIN", [])
    assert :ok = query("COMMIT", [])
    assert :ok = query("BEGIN", [])
    assert :ok = query("ROLLBACK", [])
    assert [[42]] = query("SELECT 42", [])
  end

  test "connection works after failure in prepare", context do
    assert %Postgrex.Error{} = prepare("bad", "wat")
    assert [[42]] = query("SELECT 42", [])
  end

  test "connection works after failure in execute", context do
    %Postgrex.Query{} = query = prepare("unique", "insert into uniques values (1), (1);")
    assert %Postgrex.Error{postgres: %{code: :unique_violation}} =
      execute(query, [])
    assert %Postgrex.Error{postgres: %{code: :unique_violation}} =
      execute(query, [])
    assert [[42]] = query("SELECT 42", [])
  end

  test "connection reuses prepared query after query", context do
    %Postgrex.Query{} = query = prepare("", "SELECT 41")
    assert [[42]] = query("SELECT 42", [])
    assert [[41]] = execute(query, [])
  end

  test "connection reuses prepared query after failure in preparing state", context do
    %Postgrex.Query{} = query = prepare("", "SELECT 41")
    assert %Postgrex.Error{} = query("wat", [])
    assert [[41]] = execute(query, [])
  end

  test "connection reuses prepared query after failure in executing state", context do
    %Postgrex.Query{} = query = prepare("", "SELECT 41")
    assert %Postgrex.Error{postgres: %{code: :unique_violation}} =
      query("insert into uniques values (1), (1);", [])
    assert [[41]] = execute(query, [])
  end

  test "async test", context do
    self_pid = self
    Enum.each(1..10, fn _ ->
      spawn fn ->
        send self_pid, query("SELECT pg_sleep(0.1)", [])
      end
    end)

     Enum.each(1..10, fn _ ->
      assert_receive [[:void]], 1000
    end)
  end

  test "active client timeout", context do
    conn = context[:pid]

    Process.flag(:trap_exit, true)
    capture_log fn ->
      assert %Postgrex.Error{} = query("SELECT pg_sleep(0.1)", [], [timeout: 50])

      assert_receive {:EXIT, ^conn, {%DBConnection.Error{}, _}}
    end
  end

  test "active client cancel", context do
    conn = context[:pid]
    :sys.suspend(conn)

    assert {:timeout, _} = catch_exit(query("SELECT 42", [], [pool_timeout: 0]))

    Process.flag(:trap_exit, true)
    :sys.resume(conn)

    assert [[42]] = query("SELECT 42", [])
  end

  test "active client DOWN", context do
    self_pid = self
    conn = context[:pid]

    pid = spawn fn ->
      send self_pid, query("SELECT pg_sleep(0.2)", [])
    end

    :timer.sleep(100)
    Process.flag(:trap_exit, true)
    capture_log fn ->
      Process.exit(pid, :shutdown)
      assert_receive {:EXIT, ^conn, {%DBConnection.Error{}, [_|_]}}
    end
  end

  test "queued client cancel", context do
    self_pid = self
    Enum.each(1..10, fn _ ->
      spawn fn ->
        send self_pid, query("SELECT pg_sleep(0.1)", [])
      end
    end)

    assert_receive [[:void]], 1000

    assert {:timeout, _} = catch_exit(query("SELECT 42", [], [pool_timeout: 0]))
    assert [[42]] = query("SELECT 42", [])

     Enum.each(2..10, fn _ ->
      assert_received [[:void]]
    end)
  end

  test "queued client DOWN", context do
    self_pid = self
    Enum.each(1..10, fn _ ->
      spawn fn ->
        send self_pid, query("SELECT pg_sleep(0.1)", [])
      end
    end)

    assert_receive [[:void]], 1000

    pid = spawn fn ->
      send self_pid, query("SELECT 42", [])
    end

    :timer.sleep(100)
    Process.exit(pid, :shutdown)

    assert [[42]] = query("SELECT 42", [])

    Enum.each(2..10, fn _ ->
      assert_received [[:void]]
    end)
  end

  test "raise when trying to execute unprepared query", context do
    assert_raise ArgumentError, ~r/has not been prepared/,
      fn -> execute(%Postgrex.Query{name: "hi", statement: "BEGIN"}, []) end
  end

  test "raise when trying to prepare or close reserved query", context do
    assert_raise ArgumentError, ~r/uses reserved name/,
      fn -> prepare("POSTGREX_BEGIN", "COMMIT") end

    query = prepare("BEGIN", "BEGIN")
    query = %Postgrex.Query{query | name: "POSTGREX_BEGIN"}

    assert_raise ArgumentError, ~r/uses reserved name/, fn -> close(query) end
  end

  test "query struct interpolates to statement" do
    assert "#{%Postgrex.Query{statement: "BEGIN"}}" == "BEGIN"
  end
end
