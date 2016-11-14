defmodule QueryTest do
  use ExUnit.Case, async: true
  import Postgrex.TestHelper
  alias Postgrex, as: P

  setup context do
    opts = [ database: "postgrex_test", backoff_type: :stop,
             prepare: context[:prepare] || :named]
    {:ok, pid} = P.start_link(opts)
    {:ok, [pid: pid, options: opts]}
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

    assert [[%Postgrex.Time{hour: 0, min: 0, sec: 0, usec: 123000}]] =
           query("SELECT time '00:00:00.123'", [])
    assert [[%Postgrex.Time{hour: 0, min: 0, sec: 0, usec: 123456}]] =
           query("SELECT time '00:00:00.123456'", [])
    assert [[%Postgrex.Time{hour: 1, min: 2, sec: 3, usec: 123456}]] =
           query("SELECT time '01:02:03.123456'", [])

    assert [[%Postgrex.Time{hour: 2, min: 5, sec: 6, usec: 0}]] =
           query("SELECT timetz '04:05:06+02'", [])
    assert [[%Postgrex.Time{hour: 22, min: 5, sec: 6, usec: 0}]] =
           query("SELECT timetz '00:05:06+02'", [])
    assert [[%Postgrex.Time{hour: 1, min: 5, sec: 6, usec: 0}]] =
           query("SELECT timetz '23:05:06-02'", [])
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

  test "decode point", context do
    assert [[%Postgrex.Point{x: -97.5, y: 100.1}]] == query("SELECT point(-97.5, 100.1)::point", [])
  end

  test "encode point", context do
    assert [[%Postgrex.Point{x: -97.0, y: 100.0}]] == query("SELECT $1::point", [%Postgrex.Point{x: -97, y: 100}])
  end

  test "decode name", context do
    assert [["test"]] == query("SELECT 'test'::name", [])
  end

  test "encode name", context do
    assert [["test"]] == query("SELECT $1::name", ["test"])
  end

  test "decode \"char\"", context do
    assert [["X"]] == query("SELECT 'X'::\"char\"", [])
  end

  test "encode \"char\"", context do
    assert [["x"]] == query("SELECT $1::\"char\"", ["x"])
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

  @tag min_pg_version: "9.0"
  test "decode_binary: :copy returns copied binary", context do
    text = "hello world"
    assert [[bin]] = query("SELECT $1::text", [text])
    assert :binary.referenced_byte_size(bin) == byte_size(text)

    assert [[%{"hello" => world}]] = query("SELECT $1::hstore", [%{"hello" => "world"}])
    assert :binary.referenced_byte_size(world) == byte_size("world")
  end

  @tag min_pg_version: "9.0"
  test "decode_binary: :reference returns reference counted binary" do
    {:ok, pid} = P.start_link(database: "postgrex_test",
                              decode_binary: :reference)
    text = "hello world"
    assert %{rows: [[bin]]} = P.query!(pid, "SELECT $1::text", [text])
    assert :binary.referenced_byte_size(bin) > byte_size(text)

    assert %{rows: [[%{"hello" => world}]]} = P.query!(pid, "SELECT $1::hstore", [%{"hello" => "world"}])
    assert :binary.referenced_byte_size(world) > byte_size("world")
  end

  test "decode bit string", context do
    assert [[<<1::1,0::1,1::1>>]] == query("SELECT bit '101'", [])
    assert [[<<1::1,1::1,0::1>>]] == query("SELECT bit '110'", [])
    assert [[<<1::1,1::1,0::1>>]] == query("SELECT bit '110' :: varbit", [])
    assert [[<<1::1,0::1,1::1,1::1,0::1>>]] == query("SELECT bit '10110'", [])
    assert [[<<1::1,0::1,1::1,0::1,0::1>>]] ==
      query("SELECT bit '101' :: bit(5)", [])
    assert [[<<1::1,0::1,0::1,0::1,0::1,0::1,0::1,0::1,
             1::1,0::1,1::1>>]] ==
      query("SELECT bit '10000000101'", [])
    assert [[<<0::1,0::1,0::1,0::1,0::1,0::1,0::1,0::1,
             0::1,0::1,0::1,0::1,0::1,0::1,0::1,0::1,
             1::1,0::1,1::1>>]] ==
      query("SELECT bit '0000000000000000101'", [])
    assert [[<<1::1,0::1,0::1,0::1,0::1,0::1,0::1,0::1,
             0::1,0::1,0::1,0::1,0::1,0::1,0::1,0::1,
             1::1,0::1,1::1>>]] ==
      query("SELECT bit '1000000000000000101'", [])
    assert [[<<1::1,0::1,0::1,0::1,0::1,0::1,0::1,1::1,
             1::1,0::1,0::1,0::1,0::1,0::1,0::1,0::1,
             1::1,0::1,1::1>>]] ==
      query("SELECT bit '1000000110000000101'", [])
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
    assert message =~ "See https://github.com/elixir-ecto/postgrex#oid-type-encoding"
  end

  test "fail on encoding wrong value", context do
    assert %ArgumentError{message: message} = catch_error(query("SELECT $1::integer", ["123"]))
    assert message =~ "Postgrex expected an integer in -2147483648..2147483647 that can be encoded/cast to type \"int4\""
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
      "1.111101",
      "1.1111111101",
      "1.11110001",
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

    assert [[%Postgrex.Time{hour: 2, min: 5, sec: 6, usec: 0}]] =
           query("SELECT $1::timetz", [%Postgrex.Time{hour: 2, min: 5, sec: 6, usec: 0}])
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

  test "encode bit string", context do
    assert [["110"]] == query("SELECT $1::bit(3)::text", [<<1::1, 1::1, 0::1>>])
    assert [["110"]] == query("SELECT $1::varbit::text", [<<1::1, 1::1, 0::1>>])
    assert [["101"]] == query("SELECT $1::bit(3)::text", [<<1::1, 0::1, 1::1>>])
    assert [["11010"]] ==
      query("SELECT $1::bit(5)::text", [<<1::1, 1::1, 0::1, 1::1>>])
    assert [["10000000101"]] ==
      query("SELECT $1::bit(11)::text",
        [<<1::1,0::1,0::1,0::1,0::1,0::1,0::1,0::1,
         1::1,0::1,1::1>>])
    assert [["0000000000000000101"]] ==
      query("SELECT $1::bit(19)::text",
        [<<0::1,0::1,0::1,0::1,0::1,0::1,0::1,0::1,
         0::1,0::1,0::1,0::1,0::1,0::1,0::1,0::1,
         1::1,0::1,1::1>>])
    assert [["1000000000000000101"]] ==
      query("SELECT $1::bit(19)::text",
        [<<1::1,0::1,0::1,0::1,0::1,0::1,0::1,0::1,
         0::1,0::1,0::1,0::1,0::1,0::1,0::1,0::1,
         1::1,0::1,1::1>>])
    assert [["1000000110000000101"]] ==
      query("SELECT $1::bit(19)::text",
        [<<1::1,0::1,0::1,0::1,0::1,0::1,0::1,1::1,
         1::1,0::1,0::1,0::1,0::1,0::1,0::1,0::1,
         1::1,0::1,1::1>>])
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

  test "prepare query and execute different queries with same name", context do
    assert (%Postgrex.Query{name: "select"} = query42) = prepare("select", "SELECT 42")
    assert close(query42) == :ok
    assert %Postgrex.Query{} = prepare("select", "SELECT 41")
    assert [[42]] = execute(query42, [])

    assert [[42]] = query("SELECT 42", [])
  end

  test "prepare, close and execute", context do
    assert (%Postgrex.Query{} = query) = prepare("reuse", "SELECT $1::int")
    assert [[42]] = execute(query, [42])
    assert :ok = close(query)
    assert [[42]] = execute(query, [42])
  end

  test "closing prepared query that does not exist succeeds", context do
    assert (%Postgrex.Query{} = query) = prepare("42", "SELECT 42")
    assert :ok = close(query)
    assert :ok = close(query)
  end

  @tag prepare: :unnamed
  test "prepare named is unnamed when named not allowed", context do
    assert (%Postgrex.Query{name: ""} = query) = prepare("42", "SELECT 42")
    assert [[42]] = execute(query, [])
    assert [[42]] = execute(query, [])
    assert :ok = close(query)
    assert [[42]] = query("SELECT 42", [])
  end

  test "execute prepared query on another connection", context do
    query = prepare("S42", "SELECT 42")

    {:ok, pid2} = Postgrex.start_link(context[:options])
    assert {:ok, %Postgrex.Result{rows: [[42]]}} = Postgrex.execute(pid2, query, [])
    assert {:ok, %Postgrex.Result{rows: [[41]]}} = Postgrex.query(pid2, "SELECT 41", [])
  end

  test "raise when executing prepared query on connection with different types", context do
    query = prepare("S42", "SELECT 42")

    {:ok, pid2} = Postgrex.start_link([decode_binary: :reference] ++ context[:options])

    assert_raise ArgumentError, ~r"invalid types for the connection",
      fn() -> Postgrex.execute(pid2, query, []) end
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

  test "connection reuses prepared query after failure in unnamed preparing state", context do
    %Postgrex.Query{} = query = prepare("", "SELECT 41")
    assert %Postgrex.Error{postgres: %{code: :syntax_error}} = query("wat", [])
    assert [[41]] = execute(query, [])
  end

  test "connection reuses prepared query after failure in named preparing state", context do
    %Postgrex.Query{} = query = prepare("named", "SELECT 41")
    assert %Postgrex.Error{postgres: %{code: :syntax_error}} = prepare("named", "wat")
    assert [[41]] = execute(query, [])
  end

  test "connection reuses prepared query after failure in executing state", context do
    %Postgrex.Query{} = query = prepare("", "SELECT 41")
    assert %Postgrex.Error{postgres: %{code: :unique_violation}} =
      query("insert into uniques values (1), (1);", [])
    assert [[41]] = execute(query, [])
  end

  test "connection forces prepare on execute after prepare of same name", context do
    %Postgrex.Query{} = query41 = prepare("", "SELECT 41")
    assert %Postgrex.Query{} = query42 = prepare("", "SELECT 42")
    assert [[42]] = execute(query42, [])
    assert [[41]] = execute(query41, [])
  end

  test "connection describes query when already prepared", context do
    %Postgrex.Query{} = prepare("", "SELECT 41")
    %Postgrex.Query{} = query = prepare("", "SELECT 41")
    assert [[41]] = execute(query, [])
    assert [[42]] = query("SELECT 42", [])
  end

  test "async test", context do
    self_pid = self()
    Enum.each(1..10, fn _ ->
      spawn_link fn ->
        send self_pid, query("SELECT pg_sleep(0.05)", [])
      end
    end)

    assert [[42]] = query("SELECT 42", [])

    Enum.each(1..10, fn _ ->
      assert_receive [[:void]]
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

  test "raise when trying to execute reserved query", context do
    query = prepare("", "BEGIN")

    assert_raise ArgumentError, ~r/uses reserved name/,
      fn -> execute(%{query | name: "POSTGREX_COMMIT"}, []) end
  end

  test "query struct interpolates to statement" do
    assert "#{%Postgrex.Query{statement: "BEGIN"}}" == "BEGIN"
  end

  test "connection_id", context do
    assert {:ok, %Postgrex.Result{connection_id: connection_id, rows: [[backend_pid]]}} =
      Postgrex.query(context[:pid], "SELECT pg_backend_pid()", [])
    assert is_integer(connection_id)
    assert connection_id == backend_pid

    assert {:error, %Postgrex.Error{connection_id: connection_id}} =
      Postgrex.query(context[:pid], "FOO BAR", [])
    assert is_integer(connection_id)
  end

  test "empty query", context do
    assert %Postgrex.Result{command: nil, rows: nil, num_rows: 0} =
      Postgrex.query!(context[:pid], "", [])
  end

  test "query before and after idle ping" do
    opts = [ database: "postgrex_test", backoff_type: :stop, idle_timeout: 1]
    {:ok, pid} = P.start_link(opts)
    assert {:ok, _} = P.query(pid, "SELECT 42", [])
    :timer.sleep(20)
    assert {:ok, _} = P.query(pid, "SELECT 42", [])
    :timer.sleep(20)
    assert {:ok, _} = P.query(pid, "SELECT 42", [])
  end

  test "notices raised by functions do not reset rows", context do
    assert :ok = query("""
    CREATE FUNCTION raise_notice_and_return(what integer) RETURNS integer AS $$
    BEGIN
      RAISE NOTICE 'notice %', what;
      RETURN what;
    END;
    $$ LANGUAGE plpgsql;
    """, [])
    assert [[1], [2]] = query("SELECT raise_notice_and_return(x) FROM generate_series(1, 2) AS x", [])
  end

  test "too many parameters query disconnects", context do
    Process.flag(:trap_exit, true)
    params = 1..0x10000
    query = ["INSERT INTO uniques VALUES (0)" |
      (for n <- params, do: [", ($", to_string(n), "::int4)"])]
    params = Enum.into(params, [])

    capture_log fn ->
      assert_raise RuntimeError,
        "postgresql protocol can not handle 65536 parameters, the maximum is 65535",
        fn() -> query(query, params) end
      pid = context[:pid]
      assert_receive {:EXIT, ^pid, {:shutdown, %RuntimeError{}}}
    end
  end

  test "COPY FROM STDIN  returns error", context do
    assert %Postgrex.Error{postgres: %{code: :query_canceled}} =
      query("COPY uniques FROM STDIN", [])
  end

  test "COPY TO STDOUT", context do
    assert [] = query("COPY uniques TO STDOUT", [])
    assert ["1\t2\n"] = query("COPY (VALUES (1, 2)) TO STDOUT", [])
    assert ["1\t2\n", "3\t4\n"] = query("COPY (VALUES (1, 2), (3, 4)) TO STDOUT", [])
  end

  test "COPY TO STDOUT with decoder_mapper", context do
    opts = [decode_mapper: &String.split/1]
    assert [["1","2"], ["3","4"]] = query("COPY (VALUES (1, 2), (3, 4)) TO STDOUT", [], opts)
  end

  test "receive packet with remainder greater than 64MB", context do
    # to ensure remainder is more than 64MB use 64MBx2+1
    big_binary = :binary.copy(<<1>>, 128*1024*1024+1)
    assert [[binary]] = query("SELECT $1::bytea;", [big_binary])
    assert byte_size(binary) == 128 * 1024 * 1024 + 1
  end

  test "terminate backend", context do
    Process.flag(:trap_exit, true)
    assert {:ok, pid} = P.start_link([idle_timeout: 10] ++ context[:options])

    %Postgrex.Result{connection_id: connection_id} =
      Postgrex.query!(pid, "SELECT 42", [])

    capture_log(fn() ->
      assert [[true]] = query("SELECT pg_terminate_backend($1)", [connection_id])

      assert_receive {:EXIT, ^pid, {:shutdown, %Postgrex.Error{postgres: %{code: :admin_shutdown}}}}
    end)
  end
end
