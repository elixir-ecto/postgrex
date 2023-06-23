defmodule QueryTest do
  use ExUnit.Case, async: true
  import Postgrex.TestHelper
  import ExUnit.CaptureLog
  alias Postgrex, as: P

  setup context do
    opts = [
      database: "postgrex_test",
      backoff_type: :stop,
      prepare: context[:prepare] || :named,
      max_restarts: 0
    ]

    {:ok, pid} = P.start_link(opts)
    {:ok, [pid: pid, options: opts]}
  end

  test "iodata", context do
    assert [[123]] = query(["S", ?E, ["LEC" | "T"], " ", '123'], [])
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

    assert [[Decimal.new("123456789123456789123456789")]] ==
             query("SELECT 123456789123456789123456789::numeric", [])

    assert [[Decimal.new("123456789123456789123456789.123456789")]] ==
             query("SELECT 123456789123456789123456789.123456789", [])

    assert [[Decimal.new("1.1234500000")]] == query("SELECT 1.1234500000", [])
    assert [[Decimal.new("NaN")]] == query("SELECT 'NaN'::numeric", [])
  end

  @tag min_pg_version: "14.0"
  test "decode numeric infinity", context do
    assert [[Decimal.new("Inf")]] == query("SELECT NUMERIC 'Infinity'", [])
    assert [[Decimal.new("-Inf")]] == query("SELECT NUMERIC '-Infinity'", [])
  end

  @tag min_pg_version: "9.5"
  test "decode json/jsonb", context do
    assert [[%{"foo" => 42}]] == query("SELECT '{\"foo\": 42}'::json", [])
    assert [[%{"foo" => 42}]] == query("SELECT '{\"foo\": 42}'::jsonb", [])
  end

  test "decode uuid", context do
    uuid = <<160, 238, 188, 153, 156, 11, 78, 248, 187, 109, 107, 185, 189, 56, 10, 17>>
    assert [[^uuid]] = query("SELECT 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid", [])
  end

  test "decode arrays", context do
    assert [[[]]] = query("SELECT ARRAY[]::integer[]", [])
    assert [[[1]]] = query("SELECT ARRAY[1]", [])
    assert [[[1, 2]]] = query("SELECT ARRAY[1,2]", [])
    assert [[[[0], [1]]]] = query("SELECT ARRAY[[0],[1]]", [])
    assert [[[[0]]]] = query("SELECT ARRAY[ARRAY[0]]", [])
  end

  test "decode array domain", context do
    assert [[[1.0, 2.0, 3.0]]] = query("SELECT ARRAY[1, 2, 3]::floats_domain", [])

    assert [
             [
               [
                 %Postgrex.Point{x: 1.0, y: 1.0},
                 %Postgrex.Point{x: 2.0, y: 2.0},
                 %Postgrex.Point{x: 3.0, y: 3.0}
               ]
             ]
           ] = query("SELECT ARRAY[point '1,1', point '2,2', point '3,3']::points_domain", [])
  end

  test "encode array domain", context do
    floats = [1.0, 2.0, 3.0]
    floats_string = "{1,2,3}"
    assert [[^floats_string]] = query("SELECT $1::floats_domain::text", [floats])

    points = [
      %Postgrex.Point{x: 1.0, y: 1.0},
      %Postgrex.Point{x: 2.0, y: 2.0},
      %Postgrex.Point{x: 3.0, y: 3.0}
    ]

    points_string = "{\"(1,1)\",\"(2,2)\",\"(3,3)\"}"
    assert [[^points_string]] = query("SELECT $1::points_domain::text", [points])
  end

  test "decode interval", context do
    assert [[%Postgrex.Interval{months: 0, days: 0, secs: 0, microsecs: 0}]] =
             query("SELECT interval '0'", [])

    assert [[%Postgrex.Interval{months: 100, days: 0, secs: 0, microsecs: 0}]] =
             query("SELECT interval '100 months'", [])

    assert [[%Postgrex.Interval{months: 0, days: 100, secs: 0, microsecs: 0}]] =
             query("SELECT interval '100 days'", [])

    assert [[%Postgrex.Interval{months: 0, days: 0, secs: 100, microsecs: 0}]] =
             query("SELECT interval '100 secs'", [])

    assert [[%Postgrex.Interval{months: 14, days: 40, secs: 10920, microsecs: 0}]] =
             query("SELECT interval '1 year 2 months 40 days 3 hours 2 minutes'", [])

    assert [[%Postgrex.Interval{months: 0, days: 0, secs: 53, microsecs: 204_800}]] =
             query("SELECT interval '53 secs 204800 microseconds'", [])

    assert [[%Postgrex.Interval{months: 0, days: 0, secs: 10, microsecs: 240_000}]] =
             query("SELECT interval '10240000 microseconds'", [])
  end

  test "decode point", context do
    assert [[%Postgrex.Point{x: -97.5, y: 100.1}]] ==
             query("SELECT point(-97.5, 100.1)::point", [])
  end

  test "encode point", context do
    assert [[%Postgrex.Point{x: -97.0, y: 100.0}]] ==
             query("SELECT $1::point", [%Postgrex.Point{x: -97, y: 100}])
  end

  test "decode polygon", context do
    p1 = %Postgrex.Point{x: 100.0, y: 101.5}
    p2 = %Postgrex.Point{x: 100.0, y: -99.1}
    p3 = %Postgrex.Point{x: -91.1, y: -101.1}
    p4 = %Postgrex.Point{x: -100.0, y: 99.9}
    polygon = %Postgrex.Polygon{vertices: [p1, p2, p3, p4]}
    polystring = "((100.0,101.5),(100.0,-99.1),(-91.1,-101.1),(-100.0,99.9))"
    assert [[polygon]] == query("SELECT '#{polystring}'" <> "::polygon", [])
  end

  test "encode polygon", context do
    p1 = %Postgrex.Point{x: 100.0, y: 101.5}
    p2 = %Postgrex.Point{x: 100.0, y: -99.1}
    p3 = %Postgrex.Point{x: -91.1, y: -101.1}
    p4 = %Postgrex.Point{x: -100.0, y: 99.9}
    polygon = %Postgrex.Polygon{vertices: [p1, p2, p3, p4]}
    assert [[polygon]] == query("SELECT $1::polygon", [polygon])
    assert %DBConnection.EncodeError{} = catch_error(query("SELECT $1::polygon", [1]))
    bad_polygon = %Postgrex.Polygon{vertices: ["x"]}
    assert %DBConnection.EncodeError{} = catch_error(query("SELECT $1::polygon", [bad_polygon]))
  end

  @tag min_pg_version: "9.4"
  test "decode line", context do
    # 98.6x - y = 0 <=> y = 98.6x
    line = %Postgrex.Line{a: 98.6, b: -1.0, c: 0.0}
    assert [[line]] == query("SELECT '{98.6,-1.0,0.0}'::line", [])
    assert [[line]] == query("SELECT '(0.0,0.0),(1.0,98.6)'::line", [])
  end

  @tag min_pg_version: "9.4"
  test "encode line", context do
    # 98.6x - y = 0 <=> y = 98.6x
    line = %Postgrex.Line{a: 98.6, b: -1.0, c: 0.0}
    assert [[line]] == query("SELECT $1::line", [line])
    assert %DBConnection.EncodeError{} = catch_error(query("SELECT $1::line", ["foo"]))
    bad_line = %Postgrex.Line{a: nil, b: "foo"}
    assert %DBConnection.EncodeError{} = catch_error(query("SELECT $1::line", [bad_line]))
  end

  test "decode line segment", context do
    segment = %Postgrex.LineSegment{
      point1: %Postgrex.Point{x: 0.0, y: 0.0},
      point2: %Postgrex.Point{x: 1.0, y: 1.0}
    }

    assert [[segment]] == query("SELECT '(0.0,0.0)(1.0,1.0)'::lseg", [])
  end

  test "encode line segment", context do
    segment = %Postgrex.LineSegment{
      point1: %Postgrex.Point{x: 0.0, y: 0.0},
      point2: %Postgrex.Point{x: 1.0, y: 1.0}
    }

    assert [[segment]] == query("SELECT $1::lseg", [segment])
    assert %DBConnection.EncodeError{} = catch_error(query("SELECT $1::lseg", [1.0]))

    assert %DBConnection.EncodeError{} =
             catch_error(query("SELECT $1::lseg", [%Postgrex.LineSegment{}]))
  end

  test "decode box", context do
    box = %Postgrex.Box{
      upper_right: %Postgrex.Point{x: 1.0, y: 1.0},
      bottom_left: %Postgrex.Point{x: 0.0, y: 0.0}
    }

    # postgres automatically sorts the points so that we get UR/BL
    assert [[box]] == query("SELECT '(0.0,0.0)(1.0,1.0)'::box", [])
    assert [[box]] == query("SELECT '(1.0,1.0)(0.0,0.0)'::box", [])
    assert [[box]] == query("SELECT '(1.0,0.0)(0.0,1.0)'::box", [])
    assert [[box]] == query("SELECT '(0.0,1.0)(1.0,0.0)'::box", [])
  end

  test "encode box", context do
    box = %Postgrex.Box{
      upper_right: %Postgrex.Point{x: 1.0, y: 1.0},
      bottom_left: %Postgrex.Point{x: 0.0, y: 0.0}
    }

    assert [[box]] == query("SELECT $1::box", [box])
    assert %DBConnection.EncodeError{} = catch_error(query("SELECT $1::box", [1.0]))
    assert %DBConnection.EncodeError{} = catch_error(query("SELECT $1::box", [%Postgrex.Box{}]))
  end

  test "decode path", context do
    p1 = %Postgrex.Point{x: 0.0, y: 0.0}
    p2 = %Postgrex.Point{x: 1.0, y: 3.0}
    p3 = %Postgrex.Point{x: -4.0, y: 3.14}
    path = %Postgrex.Path{points: [p1, p2, p3], open: true}
    assert [[path]] == query("SELECT '[(0.0,0.0),(1.0,3.0),(-4.0,3.14)]'::path", [])

    assert [[%{path | open: false}]] ==
             query("SELECT '((0.0,0.0),(1.0,3.0),(-4.0,3.14))'::path", [])

    assert %DBConnection.EncodeError{} = catch_error(query("SELECT $1::path", [1.0]))
    bad_path = %Postgrex.Path{points: "foo", open: false}
    assert %DBConnection.EncodeError{} = catch_error(query("SELECT $1::path", [bad_path]))
    # open must be true/false
    bad_path = %Postgrex.Path{points: []}
    assert %DBConnection.EncodeError{} = catch_error(query("SELECT $1::path", [bad_path]))
  end

  test "encode path", context do
    p1 = %Postgrex.Point{x: 0.0, y: 0.0}
    p2 = %Postgrex.Point{x: 1.0, y: 3.0}
    p3 = %Postgrex.Point{x: -4.0, y: 3.14}
    path = %Postgrex.Path{points: [p1, p2, p3], open: false}
    assert [[path]] == query("SELECT $1::path", [path])
  end

  test "decode circle", context do
    center = %Postgrex.Point{x: 1.0, y: -3.5}
    circle = %Postgrex.Circle{center: center, radius: 100.0}
    assert [[circle]] == query("SELECT '<(1.0,-3.5),100.0>'::circle", [])
  end

  test "encode circle", context do
    center = %Postgrex.Point{x: 1.0, y: -3.5}
    circle = %Postgrex.Circle{center: center, radius: 100.0}
    assert [[circle]] == query("SELECT $1::circle", [circle])
    assert %DBConnection.EncodeError{} = catch_error(query("SELECT $1::path", ["snu"]))
    bad_circle = %Postgrex.Circle{center: 1.5, radius: 1.0}
    assert %DBConnection.EncodeError{} = catch_error(query("SELECT $1::path", [bad_circle]))
    bad_circle = %Postgrex.Circle{center: %Postgrex.Point{x: 1.0, y: 0.0}, radius: "five"}
    assert %DBConnection.EncodeError{} = catch_error(query("SELECT $1::path", [bad_circle]))
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

  @tag :capture_log
  test "decode record", context do
    assert [[{1, "2"}]] = query("SELECT (1, '2')::composite1", [])
    assert [[[{1, "2"}]]] = query("SELECT ARRAY[(1, '2')::composite1]", [])
  end

  test "decode enum", context do
    assert [["elixir"]] = query("SELECT 'elixir'::enum1", [])
  end

  @tag min_pg_version: "9.2"
  test "decode range", context do
    # These do not appear to match what is selected, but that's because
    # PostgreSQL itself returns range values this way.
    # `SELECT '(1,5)'::int4range` returns `[2,5)`.
    assert [[%Postgrex.Range{lower: 2, upper: 5, lower_inclusive: true, upper_inclusive: false}]] =
             query("SELECT '(1,5)'::int4range", [])

    assert [[%Postgrex.Range{lower: 1, upper: 7, lower_inclusive: true, upper_inclusive: false}]] =
             query("SELECT '[1,6]'::int4range", [])

    assert [
             [
               %Postgrex.Range{
                 lower: :unbound,
                 upper: 5,
                 lower_inclusive: false,
                 upper_inclusive: false
               }
             ]
           ] = query("SELECT '(,5)'::int4range", [])

    assert [
             [
               %Postgrex.Range{
                 lower: 1,
                 upper: :unbound,
                 lower_inclusive: true,
                 upper_inclusive: false
               }
             ]
           ] = query("SELECT '[1,)'::int4range", [])

    assert [
             [
               %Postgrex.Range{
                 lower: :empty,
                 upper: :empty,
                 lower_inclusive: false,
                 upper_inclusive: false
               }
             ]
           ] = query("SELECT '(1,1)'::int4range", [])

    assert [[%Postgrex.Range{lower: 1, upper: 2, lower_inclusive: true, upper_inclusive: false}]] =
             query("SELECT '[1,1]'::int4range", [])

    assert [
             [
               %Postgrex.Range{
                 lower: :unbound,
                 upper: :unbound,
                 lower_inclusive: false,
                 upper_inclusive: false
               }
             ]
           ] = query("SELECT '(,)'::int4range", [])

    assert [
             [
               %Postgrex.Range{
                 lower: :unbound,
                 upper: :unbound,
                 lower_inclusive: false,
                 upper_inclusive: false
               }
             ]
           ] = query("SELECT '[,]'::int4range", [])

    assert [[%Postgrex.Range{lower: 3, upper: 8, lower_inclusive: true, upper_inclusive: false}]] =
             query("SELECT '(2,8)'::int8range", [])

    assert [
             [
               %Postgrex.Range{
                 lower: Decimal.new("1.2"),
                 upper: Decimal.new("3.4"),
                 lower_inclusive: false,
                 upper_inclusive: false
               }
             ]
           ] ==
             query("SELECT '(1.2,3.4)'::numrange", [])

    assert [
             [
               %Postgrex.Range{
                 lower: %Date{year: 2014, month: 1, day: 1},
                 upper: %Date{year: 2014, month: 12, day: 31}
               }
             ]
           ] = query("SELECT '[2014-1-1,2014-12-31)'::daterange", [])

    assert [[%Postgrex.Range{lower: :unbound, upper: %Date{year: 2014, month: 12, day: 31}}]] =
             query("SELECT '(,2014-12-31)'::daterange", [])

    assert [[%Postgrex.Range{lower: %Date{year: 2014, month: 1, day: 2}, upper: :unbound}]] =
             query("SELECT '(2014-1-1,]'::daterange", [])

    assert [
             [
               %Postgrex.Range{
                 lower: :unbound,
                 upper: :unbound,
                 lower_inclusive: false,
                 upper_inclusive: false
               }
             ]
           ] = query("SELECT '(,)'::daterange", [])

    assert [
             [
               %Postgrex.Range{
                 lower: :unbound,
                 upper: :unbound,
                 lower_inclusive: false,
                 upper_inclusive: false
               }
             ]
           ] = query("SELECT '[,]'::daterange", [])
  end

  @tag min_pg_version: "14.0"
  test "decode multirange", context do
    # empty ranges
    empty_multirange = %Postgrex.Multirange{ranges: []}
    assert [[empty_multirange]] == query("SELECT '{}'::int4multirange", [])

    # Postgres will normalize discrete ranges so they are lower inclusive and upper exclusive
    expected_int_multirange = %Postgrex.Multirange{
      ranges: [
        %Postgrex.Range{lower: 3, upper: 5, lower_inclusive: true, upper_inclusive: false},
        %Postgrex.Range{lower: 14, upper: 28, lower_inclusive: true, upper_inclusive: false}
      ]
    }

    assert [[expected_int_multirange]] ==
             query("SELECT '{(2,5), [14, 27]}'::int4multirange", [])

    expected_date_multirange = %Postgrex.Multirange{
      ranges: [
        %Postgrex.Range{
          lower: %Date{year: 2014, month: 1, day: 2},
          upper: %Date{year: 2014, month: 1, day: 12},
          lower_inclusive: true,
          upper_inclusive: false
        },
        %Postgrex.Range{
          lower: %Date{year: 2015, month: 1, day: 1},
          upper: %Date{year: 2015, month: 1, day: 10},
          lower_inclusive: true,
          upper_inclusive: false
        }
      ]
    }

    assert [[expected_date_multirange]] ==
             query(
               "SELECT '{(2014-01-01, 2014-01-11], [2015-01-01, 2015-01-10)}'::datemultirange",
               []
             )

    # Contiuous ranges can't be normalized
    expected_num_multirange = %Postgrex.Multirange{
      ranges: [
        %Postgrex.Range{
          lower: Decimal.new("1.1"),
          upper: Decimal.new("3.3"),
          lower_inclusive: false,
          upper_inclusive: false
        },
        %Postgrex.Range{
          lower: Decimal.new("4.4"),
          upper: Decimal.new("6.6"),
          lower_inclusive: true,
          upper_inclusive: true
        }
      ]
    }

    assert [[expected_num_multirange]] ==
             query("SELECT '{(1.1, 3.3), [4.4, 6.6]}'::nummultirange", [])
  end

  @tag min_pg_version: "9.0"
  test "decode network types", context do
    assert [[%Postgrex.INET{address: {127, 0, 0, 1}, netmask: nil}]] =
             query("SELECT '127.0.0.1'::inet", [])

    assert [[%Postgrex.INET{address: {127, 0, 0, 1}, netmask: nil}]] =
             query("SELECT '127.0.0.1/32'::inet", [])

    assert [[%Postgrex.INET{address: {127, 0, 0, 1}, netmask: 32}]] =
             query("SELECT '127.0.0.1/32'::inet::cidr", [])

    assert [[%Postgrex.INET{address: {127, 0, 0, 1}, netmask: 32}]] =
             query("SELECT '127.0.0.1/32'::cidr", [])

    assert [[%Postgrex.INET{address: {127, 0, 0, 1}, netmask: 4}]] =
             query("SELECT '127.0.0.1/4'::inet", [])

    assert [[%Postgrex.INET{address: {112, 0, 0, 0}, netmask: 4}]] =
             query("SELECT '127.0.0.1/4'::inet::cidr", [])

    assert %Postgrex.Error{
             postgres: %{
               code: :invalid_text_representation,
               detail: "Value has bits set to right of mask.",
               message: "invalid cidr value: \"127.0.0.1/4\""
             },
             query: "SELECT '127.0.0.1/4'::cidr"
           } = query("SELECT '127.0.0.1/4'::cidr", [])

    assert [[%Postgrex.INET{address: {112, 0, 0, 0}, netmask: 4}]] =
             query("SELECT '112.0.0.0/4'::cidr", [])

    assert [[%Postgrex.INET{address: {0, 0, 0, 0, 0, 0, 0, 1}, netmask: nil}]] =
             query("SELECT '::1'::inet", [])

    assert [[%Postgrex.INET{address: {0, 0, 0, 0, 0, 0, 0, 1}, netmask: nil}]] =
             query("SELECT '::1/128'::inet", [])

    assert [[%Postgrex.INET{address: {0, 0, 0, 0, 0, 0, 0, 1}, netmask: 128}]] =
             query("SELECT '::1/128'::inet::cidr", [])

    assert [[%Postgrex.INET{address: {8193, 43981, 0, 0, 0, 0, 0, 0}, netmask: 8}]] =
             query("SELECT '2001:abcd::/8'::inet", [])

    assert [[%Postgrex.INET{address: {8192, 0, 0, 0, 0, 0, 0, 0}, netmask: 8}]] =
             query("SELECT '2001:abcd::/8'::inet::cidr", [])

    assert %Postgrex.Error{
             postgres: %{
               code: :invalid_text_representation,
               detail: "Value has bits set to right of mask.",
               message: "invalid cidr value: \"2001:abcd::/8\""
             },
             query: "SELECT '2001:abcd::/8'::cidr"
           } = query("SELECT '2001:abcd::/8'::cidr", [])

    assert [[%Postgrex.INET{address: {8192, 0, 0, 0, 0, 0, 0, 0}, netmask: 8}]] =
             query("SELECT '2000::/8'::cidr", [])

    assert [[%Postgrex.MACADDR{address: {8, 1, 43, 5, 7, 9}}]] =
             query("SELECT '08:01:2b:05:07:09'::macaddr", [])
  end

  test "decode oid and its aliases", context do
    assert [[4_294_967_295]] = query("select 4294967295::oid;", [])

    assert [["-"]] = query("select '-'::regproc::text;", [])
    assert [["sum(integer)"]] = query("select 'sum(int4)'::regprocedure::text;", [])
    assert [["||/"]] = query("select 'pg_catalog.||/'::regoper::text;", [])
    assert [["+(integer,integer)"]] = query("select '+(integer,integer)'::regoperator::text;", [])
    assert [["pg_type"]] = query("select 'pg_type'::regclass::text;", [])
    assert [["english"]] = query("select 'english'::regconfig::text;", [])
    assert [["integer"]] = query("select 'int4'::regtype::text;", [])

    assert [[0]] = query("select '-'::regproc;", [])
    assert [[44]] = query("select 'regprocin'::regproc;", [])
    assert [[2108]] = query("select 'sum(int4)'::regprocedure;", [])
    assert [[597]] = query("select 'pg_catalog.||/'::regoper;", [])
    assert [[551]] = query("select '+(integer,integer)'::regoperator;", [])
    assert [[1247]] = query("select 'pg_type'::regclass;", [])
    [[regconfig_oid]] = query("select 'english'::regconfig;", [])
    assert is_integer(regconfig_oid)
    assert [[23]] = query("select 'int4'::regtype;", [])

    # xid type
    assert [[xmin, xmax]] = query("select xmin, xmax from pg_type limit 1;", [])
    assert is_number(xmin) and is_number(xmax)

    # cid type
    assert [[cmin, cmax]] = query("select cmin, cmax from pg_type limit 1;", [])
    assert is_number(cmin) and is_number(cmax)
  end

  @tag min_pg_version: "9.0"
  test "hstore copies binaries by default", context do
    # For OTP 20+ refc binaries up to 64 bytes might be copied during a GC
    text = String.duplicate("hello world", 6)
    assert [[bin]] = query("SELECT $1::text", [text])
    assert :binary.referenced_byte_size(bin) == byte_size(text)

    assert [[%{"hello" => value}]] = query("SELECT $1::hstore", [%{"hello" => text}])
    assert :binary.referenced_byte_size(value) == byte_size(text)
  end

  test "decode bit string", context do
    assert [[<<1::1, 0::1, 1::1>>]] == query("SELECT bit '101'", [])
    assert [[<<1::1, 1::1, 0::1>>]] == query("SELECT bit '110'", [])
    assert [[<<1::1, 1::1, 0::1>>]] == query("SELECT bit '110' :: varbit", [])
    assert [[<<1::1, 0::1, 1::1, 1::1, 0::1>>]] == query("SELECT bit '10110'", [])

    assert [[<<1::1, 0::1, 1::1, 0::1, 0::1>>]] ==
             query("SELECT bit '101' :: bit(5)", [])

    assert [[<<1::1, 0::1, 0::1, 0::1, 0::1, 0::1, 0::1, 0::1, 1::1, 0::1, 1::1>>]] ==
             query("SELECT bit '10000000101'", [])

    assert [
             [
               <<0::1, 0::1, 0::1, 0::1, 0::1, 0::1, 0::1, 0::1, 0::1, 0::1, 0::1, 0::1, 0::1,
                 0::1, 0::1, 0::1, 1::1, 0::1, 1::1>>
             ]
           ] ==
             query("SELECT bit '0000000000000000101'", [])

    assert [
             [
               <<1::1, 0::1, 0::1, 0::1, 0::1, 0::1, 0::1, 0::1, 0::1, 0::1, 0::1, 0::1, 0::1,
                 0::1, 0::1, 0::1, 1::1, 0::1, 1::1>>
             ]
           ] ==
             query("SELECT bit '1000000000000000101'", [])

    assert [
             [
               <<1::1, 0::1, 0::1, 0::1, 0::1, 0::1, 0::1, 1::1, 1::1, 0::1, 0::1, 0::1, 0::1,
                 0::1, 0::1, 0::1, 1::1, 0::1, 1::1>>
             ]
           ] ==
             query("SELECT bit '1000000110000000101'", [])
  end

  @tag min_pg_version: "13.0"
  test "decode lquery", context do
    lquery = "*.path1.*"
    assert [[lquery]] == query("SELECT '#{lquery}'::lquery", [])
  end

  @tag min_pg_version: "13.0"
  test "decode ltree", context do
    ltree = "this.is.a.path"
    assert [[ltree]] == query("SELECT '#{ltree}'::ltree", [])
  end

  test "encode oid and its aliases", context do
    # oid's range is 0 to 4294967295
    assert [[0]] = query("select $1::oid;", [0])
    assert [[4_294_967_295]] = query("select $1::oid;", [4_294_967_295])
    assert %DBConnection.EncodeError{} = catch_error(query("SELECT $1::oid", [0 - 1]))
    assert %DBConnection.EncodeError{} = catch_error(query("SELECT $1::oid", [4_294_967_295 + 1]))

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
    assert_raise ArgumentError,
                 ~r"See https://github.com/elixir-ecto/postgrex#oid-type-encoding",
                 fn -> query("select $1::regclass;", ["pg_type"]) end
  end

  test "fail on encoding wrong value", context do
    assert %DBConnection.EncodeError{message: message} =
             catch_error(query("SELECT $1::integer", ["123"]))

    assert message =~ "Postgrex expected an integer in -2147483648..2147483647"
  end

  @tag min_pg_version: "9.0"
  test "decode hstore", context do
    assert [[%{}]] = query(~s|SELECT ''::hstore|, [])

    assert [[%{"Bubbles" => "7", "Name" => "Frank"}]] =
             query(~s|SELECT '"Name" => "Frank", "Bubbles" => "7"'::hstore|, [])

    assert [[%{"non_existant" => nil, "present" => "&accounted_for"}]] =
             query(~s|SELECT '"non_existant" => NULL, "present" => "&accounted_for"'::hstore|, [])

    assert [[%{"spaces in the key" => "are easy!", "floats too" => "66.6"}]] =
             query(
               ~s|SELECT '"spaces in the key" => "are easy!", "floats too" => "66.6"'::hstore|,
               []
             )

    assert [[%{"this is true" => "true", "though not this" => "false"}]] =
             query(
               ~s|SELECT '"this is true" => "true", "though not this" => "false"'::hstore|,
               []
             )
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
      "1.00123",
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
      "NaN",
      "-42"
    ]

    Enum.each(nums, fn num ->
      dec = Decimal.new(num)
      assert [[dec]] == query("SELECT $1::numeric", [dec])
    end)
  end

  test "encode integers and floats as numeric", context do
    dec = Decimal.new(1)
    assert [[dec]] == query("SELECT $1::numeric", [1])

    dec = Decimal.from_float(1.0)
    assert [[dec]] == query("SELECT $1::numeric", [1.0])
  end

  @tag min_pg_version: "14.0"
  test "encode infinite values as numeric", context do
    pos_inf = Decimal.new("Infinity")
    neg_inf = Decimal.new("Infinity")
    assert [[pos_inf]] == query("SELECT $1::numeric", [pos_inf])
    assert [[neg_inf]] == query("SELECT $1::numeric", [neg_inf])
  end

  @tag min_pg_version: "9.5"
  test "encode json/jsonb", context do
    json = %{"foo" => 42}
    assert [[json]] == query("SELECT $1::json", [json])
    assert [[json]] == query("SELECT $1::jsonb", [json])
  end

  test "encode custom numerics", context do
    assert [[%Decimal{sign: 1, coef: 1500, exp: 0}]] ==
             query("SELECT $1::numeric", [Decimal.from_float(1500.0)])

    assert [[%Decimal{sign: 1, coef: 1, exp: 0}]] ==
             query("SELECT $1::numeric", [Decimal.new(1, 1, 0)])

    assert [[%Decimal{sign: 1, coef: 10, exp: 0}]] ==
             query("SELECT $1::numeric", [Decimal.new(1, 1, 1)])

    assert [[%Decimal{sign: 1, coef: 100, exp: 0}]] ==
             query("SELECT $1::numeric", [Decimal.new(1, 1, 2)])

    assert [[%Decimal{sign: 1, coef: 1000, exp: 0}]] ==
             query("SELECT $1::numeric", [Decimal.new(1, 1, 3)])

    assert [[%Decimal{sign: 1, coef: 10000, exp: 0}]] ==
             query("SELECT $1::numeric", [Decimal.new(1, 1, 4)])

    assert [[%Decimal{sign: 1, coef: 100_000, exp: 0}]] ==
             query("SELECT $1::numeric", [Decimal.new(1, 1, 5)])

    assert [[%Decimal{sign: 1, coef: 1, exp: -5}]] ==
             query("SELECT $1::numeric", [Decimal.new(1, 1, -5)])
  end

  test "encode enforces bounds on integers", context do
    # int2's range is -32768 to +32767
    assert [[-32768]] = query("SELECT $1::int2", [-32768])
    assert [[32767]] = query("SELECT $1::int2", [32767])
    assert %DBConnection.EncodeError{} = catch_error(query("SELECT $1::int2", [32767 + 1]))
    assert %DBConnection.EncodeError{} = catch_error(query("SELECT $1::int2", [-32768 - 1]))

    # int4's range is -2147483648 to +2147483647
    assert [[-2_147_483_648]] = query("SELECT $1::int4", [-2_147_483_648])
    assert [[2_147_483_647]] = query("SELECT $1::int4", [2_147_483_647])

    assert %DBConnection.EncodeError{} =
             catch_error(query("SELECT $1::int4", [2_147_483_647 + 1]))

    assert %DBConnection.EncodeError{} =
             catch_error(query("SELECT $1::int4", [-2_147_483_648 - 1]))

    # int8's range is  -9223372036854775808 to 9223372036854775807
    assert [[-9_223_372_036_854_775_808]] = query("SELECT $1::int8", [-9_223_372_036_854_775_808])
    assert [[9_223_372_036_854_775_807]] = query("SELECT $1::int8", [9_223_372_036_854_775_807])

    assert %DBConnection.EncodeError{} =
             catch_error(query("SELECT $1::int8", [9_223_372_036_854_775_807 + 1]))

    assert %DBConnection.EncodeError{} =
             catch_error(query("SELECT $1::int8", [-9_223_372_036_854_775_808 - 1]))
  end

  @tag min_pg_version: "13.0"
  test "decode xid8 from pg_current_xact_id", context do
    assert [[int]] = query("SELECT pg_current_xact_id()", [])
    assert is_integer(int)
  end

  @tag min_pg_version: "13.0"
  test "encode enforces bounds on xid8", context do
    # xid8's range is 0 to +18_446_744_073_709_551_615
    assert [[0]] = query("SELECT $1::xid", [0])
    assert [[18_446_744_073_709_551_615]] = query("SELECT $1::xid8", [18_446_744_073_709_551_615])

    assert %DBConnection.EncodeError{} =
             catch_error(query("SELECT $1::xid8", [18_446_744_073_709_551_615 + 1]))

    assert %DBConnection.EncodeError{} = catch_error(query("SELECT $1::xid", [0 - 1]))
  end

  test "encode uuid", context do
    uuid = <<0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15>>
    assert [[^uuid]] = query("SELECT $1::uuid", [uuid])
  end

  test "encode interval", context do
    assert [[%Postgrex.Interval{months: 0, days: 0, secs: 0, microsecs: 0}]] =
             query("SELECT $1::interval", [
               %Postgrex.Interval{months: 0, days: 0, secs: 0, microsecs: 0}
             ])

    assert [[%Postgrex.Interval{months: 100, days: 0, secs: 0, microsecs: 0}]] =
             query("SELECT $1::interval", [
               %Postgrex.Interval{months: 100, days: 0, secs: 0, microsecs: 0}
             ])

    assert [[%Postgrex.Interval{months: 0, days: 100, secs: 0, microsecs: 0}]] =
             query("SELECT $1::interval", [
               %Postgrex.Interval{months: 0, days: 100, secs: 0, microsecs: 0}
             ])

    assert [[%Postgrex.Interval{months: 0, days: 0, secs: 100, microsecs: 0}]] =
             query("SELECT $1::interval", [
               %Postgrex.Interval{months: 0, days: 0, secs: 100, microsecs: 0}
             ])

    assert [[%Postgrex.Interval{months: 14, days: 40, secs: 10920, microsecs: 0}]] =
             query("SELECT $1::interval", [
               %Postgrex.Interval{months: 14, days: 40, secs: 10920, microsecs: 0}
             ])

    assert [[%Postgrex.Interval{months: 14, days: 40, secs: 10921, microsecs: 24000}]] =
             query("SELECT $1::interval", [
               %Postgrex.Interval{months: 14, days: 40, secs: 10920, microsecs: 1_024_000}
             ])
  end

  test "encode arrays", context do
    assert [[[]]] = query("SELECT $1::integer[]", [[]])
    assert [[[1]]] = query("SELECT $1::integer[]", [[1]])
    assert [[[1, 2]]] = query("SELECT $1::integer[]", [[1, 2]])
    assert [[[[0], [1]]]] = query("SELECT $1::integer[]", [[[0], [1]]])
    assert [[[[0]]]] = query("SELECT $1::integer[]", [[[0]]])
    assert [[[1, nil, 3]]] = query("SELECT $1::integer[]", [[1, nil, 3]])
  end

  @tag :capture_log
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
             query("SELECT $1::int4range", [
               %Postgrex.Range{lower: 1, upper: 3, lower_inclusive: true, upper_inclusive: true}
             ])

    assert [
             [
               %Postgrex.Range{
                 lower: :unbound,
                 upper: 6,
                 lower_inclusive: false,
                 upper_inclusive: false
               }
             ]
           ] =
             query("SELECT $1::int4range", [
               %Postgrex.Range{
                 lower: :unbound,
                 upper: 5,
                 lower_inclusive: false,
                 upper_inclusive: true
               }
             ])

    assert [
             [
               %Postgrex.Range{
                 lower: 3,
                 upper: :unbound,
                 lower_inclusive: true,
                 upper_inclusive: false
               }
             ]
           ] =
             query("SELECT $1::int4range", [
               %Postgrex.Range{
                 lower: 3,
                 upper: :unbound,
                 lower_inclusive: true,
                 upper_inclusive: true
               }
             ])

    assert [[%Postgrex.Range{lower: 4, upper: 5, lower_inclusive: true, upper_inclusive: false}]] =
             query("SELECT $1::int4range", [
               %Postgrex.Range{lower: 3, upper: 5, lower_inclusive: false, upper_inclusive: false}
             ])

    assert [
             [
               %Postgrex.Range{
                 lower: :unbound,
                 upper: :unbound,
                 lower_inclusive: false,
                 upper_inclusive: false
               }
             ]
           ] =
             query("SELECT $1::int4range", [
               %Postgrex.Range{
                 lower: :unbound,
                 upper: :unbound,
                 lower_inclusive: false,
                 upper_inclusive: false
               }
             ])

    assert [
             [
               %Postgrex.Range{
                 lower: :unbound,
                 upper: :unbound,
                 lower_inclusive: false,
                 upper_inclusive: false
               }
             ]
           ] =
             query("SELECT $1::int4range", [
               %Postgrex.Range{
                 lower: :unbound,
                 upper: :unbound,
                 lower_inclusive: true,
                 upper_inclusive: true
               }
             ])

    assert [
             [
               %Postgrex.Range{
                 lower: :empty,
                 upper: :empty,
                 lower_inclusive: false,
                 upper_inclusive: false
               }
             ]
           ] =
             query("SELECT $1::int4range", [
               %Postgrex.Range{
                 lower: :empty,
                 upper: :empty,
                 lower_inclusive: true,
                 upper_inclusive: true
               }
             ])

    assert [[%Postgrex.Range{lower: 1, upper: 4, lower_inclusive: true, upper_inclusive: false}]] =
             query("SELECT $1::int8range", [
               %Postgrex.Range{lower: 1, upper: 3, lower_inclusive: true, upper_inclusive: true}
             ])

    assert [
             [
               %Postgrex.Range{
                 lower: Decimal.new("1.2"),
                 upper: Decimal.new("3.4"),
                 lower_inclusive: true,
                 upper_inclusive: true
               }
             ]
           ] ==
             query("SELECT $1::numrange", [
               %Postgrex.Range{
                 lower: Decimal.new("1.2"),
                 upper: Decimal.new("3.4"),
                 lower_inclusive: true,
                 upper_inclusive: true
               }
             ])

    assert [
             [
               %Postgrex.Range{
                 lower: %Date{year: 2014, month: 1, day: 1},
                 upper: %Date{year: 2015, month: 1, day: 1}
               }
             ]
           ] =
             query("SELECT $1::daterange", [
               %Postgrex.Range{
                 lower: %Date{year: 2014, month: 1, day: 1},
                 upper: %Date{year: 2014, month: 12, day: 31}
               }
             ])

    assert [[%Postgrex.Range{lower: :unbound, upper: %Date{year: 2015, month: 1, day: 1}}]] =
             query("SELECT $1::daterange", [
               %Postgrex.Range{lower: :unbound, upper: %Date{year: 2014, month: 12, day: 31}}
             ])

    assert [[%Postgrex.Range{lower: %Date{year: 2014, month: 1, day: 1}, upper: :unbound}]] =
             query("SELECT $1::daterange", [
               %Postgrex.Range{lower: %Date{year: 2014, month: 1, day: 1}, upper: :unbound}
             ])

    assert [
             [
               %Postgrex.Range{
                 lower: :unbound,
                 upper: :unbound,
                 lower_inclusive: false,
                 upper_inclusive: false
               }
             ]
           ] =
             query("SELECT $1::daterange", [
               %Postgrex.Range{
                 lower: :unbound,
                 upper: :unbound,
                 lower_inclusive: false,
                 upper_inclusive: false
               }
             ])

    assert [
             [
               %Postgrex.Range{
                 lower: :unbound,
                 upper: :unbound,
                 lower_inclusive: false,
                 upper_inclusive: false
               }
             ]
           ] =
             query("SELECT $1::daterange", [
               %Postgrex.Range{
                 lower: :unbound,
                 upper: :unbound,
                 lower_inclusive: true,
                 upper_inclusive: true
               }
             ])
  end

  @tag min_pg_version: "9.2"
  test "encode enforces bounds on integer ranges", context do
    # int4's range is -2147483648 to +2147483647
    assert [[%Postgrex.Range{lower: -2_147_483_648}]] =
             query("SELECT $1::int4range", [%Postgrex.Range{lower: -2_147_483_648}])

    assert [[%Postgrex.Range{upper: 2_147_483_647}]] =
             query("SELECT $1::int4range", [
               %Postgrex.Range{upper: 2_147_483_647, upper_inclusive: false}
             ])

    assert %DBConnection.EncodeError{} =
             catch_error(query("SELECT $1::int4range", [%Postgrex.Range{lower: -2_147_483_649}]))

    assert %DBConnection.EncodeError{} =
             catch_error(query("SELECT $1::int4range", [%Postgrex.Range{upper: 2_147_483_648}]))

    # int8's range is -9223372036854775808 to 9223372036854775807
    assert [[%Postgrex.Range{lower: -9_223_372_036_854_775_807}]] =
             query("SELECT $1::int8range", [%Postgrex.Range{lower: -9_223_372_036_854_775_807}])

    assert [[%Postgrex.Range{upper: 9_223_372_036_854_775_806}]] =
             query("SELECT $1::int8range", [
               %Postgrex.Range{upper: 9_223_372_036_854_775_806, upper_inclusive: false}
             ])

    assert %DBConnection.EncodeError{} =
             catch_error(
               query("SELECT $1::int8range", [%Postgrex.Range{lower: -9_223_372_036_854_775_809}])
             )

    assert %DBConnection.EncodeError{} =
             catch_error(
               query("SELECT $1::int8range", [%Postgrex.Range{upper: 9_223_372_036_854_775_808}])
             )
  end

  @tag min_pg_version: "14.0"
  test "encode multirange", context do
    # empty ranges
    empty_multirange = %Postgrex.Multirange{ranges: []}
    assert [[empty_multirange]] == query("SELECT $1::int4multirange", [empty_multirange])

    # Postgres will normalize discrete ranges so that they are lower inclusive and upper exclusive
    int_multirange_param = %Postgrex.Multirange{
      ranges: [
        %Postgrex.Range{lower: 1, upper: 3, lower_inclusive: false, upper_inclusive: false},
        %Postgrex.Range{lower: 14, upper: 26, lower_inclusive: true, upper_inclusive: true}
      ]
    }

    expected_int_multirange = %Postgrex.Multirange{
      ranges: [
        %Postgrex.Range{lower: 2, upper: 3, lower_inclusive: true, upper_inclusive: false},
        %Postgrex.Range{lower: 14, upper: 27, lower_inclusive: true, upper_inclusive: false}
      ]
    }

    assert [[expected_int_multirange]] ==
             query("SELECT $1::int4multirange", [int_multirange_param])

    date_multirange_param = %Postgrex.Multirange{
      ranges: [
        %Postgrex.Range{
          lower: %Date{year: 2014, month: 1, day: 1},
          upper: %Date{year: 2014, month: 1, day: 10},
          lower_inclusive: false,
          upper_inclusive: true
        },
        %Postgrex.Range{
          lower: %Date{year: 2015, month: 1, day: 1},
          upper: %Date{year: 2015, month: 1, day: 10},
          lower_inclusive: true,
          upper_inclusive: false
        }
      ]
    }

    expected_date_multirange = %Postgrex.Multirange{
      ranges: [
        %Postgrex.Range{
          lower: %Date{year: 2014, month: 1, day: 2},
          upper: %Date{year: 2014, month: 1, day: 11},
          lower_inclusive: true,
          upper_inclusive: false
        },
        %Postgrex.Range{
          lower: %Date{year: 2015, month: 1, day: 1},
          upper: %Date{year: 2015, month: 1, day: 10},
          lower_inclusive: true,
          upper_inclusive: false
        }
      ]
    }

    assert [[expected_date_multirange]] ==
             query("SELECT $1::datemultirange", [date_multirange_param])

    # Continuous ranges can't be normalized
    num_multirange = %Postgrex.Multirange{
      ranges: [
        %Postgrex.Range{
          lower: Decimal.new("1.1"),
          upper: Decimal.new("3.3"),
          lower_inclusive: true,
          upper_inclusive: false
        },
        %Postgrex.Range{
          lower: Decimal.new("4.4"),
          upper: Decimal.new("6.6"),
          lower_inclusive: false,
          upper_inclusive: true
        }
      ]
    }

    assert [[num_multirange]] == query("SELECT $1::nummultirange", [num_multirange])
  end

  @tag min_pg_version: "9.0"
  test "encode hstore", context do
    assert [
             [
               %{
                 "name" => "Frank",
                 "bubbles" => "7",
                 "limit" => nil,
                 "chillin" => "true",
                 "fratty" => "false",
                 "atom" => "bomb"
               }
             ]
           ] =
             query(~s(SELECT $1::hstore), [
               %{
                 "name" => "Frank",
                 "bubbles" => "7",
                 "limit" => nil,
                 "chillin" => "true",
                 "fratty" => "false",
                 "atom" => "bomb"
               }
             ])
  end

  @tag min_pg_version: "9.0"
  test "encode network types", context do
    assert [["127.0.0.1/32"]] =
             query("SELECT $1::inet::text", [
               %Postgrex.INET{address: {127, 0, 0, 1}, netmask: nil}
             ])

    assert [["127.0.0.1/32"]] =
             query("SELECT $1::inet::text", [%Postgrex.INET{address: {127, 0, 0, 1}, netmask: 32}])

    assert [["127.0.0.1/32"]] =
             query("SELECT $1::inet::cidr::text", [
               %Postgrex.INET{address: {127, 0, 0, 1}, netmask: 32}
             ])

    assert [["127.0.0.1/32"]] =
             query("SELECT $1::cidr::text", [%Postgrex.INET{address: {127, 0, 0, 1}, netmask: 32}])

    assert [["127.0.0.1/4"]] =
             query("SELECT $1::inet::text", [%Postgrex.INET{address: {127, 0, 0, 1}, netmask: 4}])

    assert %Postgrex.Error{
             postgres: %{
               code: :invalid_binary_representation,
               detail: "Value has bits set to right of mask.",
               message: "invalid external \"cidr\" value"
             }
           } =
             query("SELECT $1::cidr::text", [%Postgrex.INET{address: {127, 0, 0, 1}, netmask: 4}])

    assert [["112.0.0.0/4"]] =
             query("SELECT $1::cidr::text", [%Postgrex.INET{address: {112, 0, 0, 0}, netmask: 4}])

    assert [["::1/128"]] =
             query("SELECT $1::inet::text", [
               %Postgrex.INET{address: {0, 0, 0, 0, 0, 0, 0, 1}, netmask: nil}
             ])

    assert [["::1/128"]] =
             query("SELECT $1::inet::text", [
               %Postgrex.INET{address: {0, 0, 0, 0, 0, 0, 0, 1}, netmask: 128}
             ])

    assert [["::1/128"]] =
             query("SELECT $1::inet::cidr::text", [
               %Postgrex.INET{address: {0, 0, 0, 0, 0, 0, 0, 1}, netmask: 128}
             ])

    assert [["2001:abcd::/8"]] =
             query("SELECT $1::inet::text", [
               %Postgrex.INET{address: {8193, 43981, 0, 0, 0, 0, 0, 0}, netmask: 8}
             ])

    assert [["2000::/8"]] =
             query("SELECT $1::inet::cidr::text", [
               %Postgrex.INET{address: {8192, 0, 0, 0, 0, 0, 0, 0}, netmask: 8}
             ])

    assert %Postgrex.Error{
             postgres: %{
               code: :invalid_binary_representation,
               detail: "Value has bits set to right of mask.",
               message: "invalid external \"cidr\" value"
             }
           } =
             query("SELECT $1::cidr::text", [
               %Postgrex.INET{address: {8193, 43981, 0, 0, 0, 0, 0, 0}, netmask: 8}
             ])

    assert [["2000::/8"]] =
             query("SELECT $1::cidr::text", [
               %Postgrex.INET{address: {8192, 0, 0, 0, 0, 0, 0, 0}, netmask: 8}
             ])

    assert [["08:01:2b:05:07:09"]] =
             query("SELECT $1::macaddr::text", [%Postgrex.MACADDR{address: {8, 1, 43, 5, 7, 9}}])
  end

  test "encode bit string", context do
    assert [["110"]] == query("SELECT $1::bit(3)::text", [<<1::1, 1::1, 0::1>>])
    assert [["110"]] == query("SELECT $1::varbit::text", [<<1::1, 1::1, 0::1>>])
    assert [["101"]] == query("SELECT $1::bit(3)::text", [<<1::1, 0::1, 1::1>>])

    assert [["11010"]] ==
             query("SELECT $1::bit(5)::text", [<<1::1, 1::1, 0::1, 1::1>>])

    assert [["10000000101"]] ==
             query(
               "SELECT $1::bit(11)::text",
               [<<1::1, 0::1, 0::1, 0::1, 0::1, 0::1, 0::1, 0::1, 1::1, 0::1, 1::1>>]
             )

    assert [["0000000000000000101"]] ==
             query(
               "SELECT $1::bit(19)::text",
               [
                 <<0::1, 0::1, 0::1, 0::1, 0::1, 0::1, 0::1, 0::1, 0::1, 0::1, 0::1, 0::1, 0::1,
                   0::1, 0::1, 0::1, 1::1, 0::1, 1::1>>
               ]
             )

    assert [["1000000000000000101"]] ==
             query(
               "SELECT $1::bit(19)::text",
               [
                 <<1::1, 0::1, 0::1, 0::1, 0::1, 0::1, 0::1, 0::1, 0::1, 0::1, 0::1, 0::1, 0::1,
                   0::1, 0::1, 0::1, 1::1, 0::1, 1::1>>
               ]
             )

    assert [["1000000110000000101"]] ==
             query(
               "SELECT $1::bit(19)::text",
               [
                 <<1::1, 0::1, 0::1, 0::1, 0::1, 0::1, 0::1, 1::1, 1::1, 0::1, 0::1, 0::1, 0::1,
                   0::1, 0::1, 0::1, 1::1, 0::1, 1::1>>
               ]
             )
  end

  @tag min_pg_version: "13.0"
  test "encode lquery", context do
    lquery = "*.path1.*"
    assert [[lquery]] == query("SELECT $1::lquery", [lquery])
  end

  @tag min_pg_version: "13.0"
  test "encode ltree", context do
    ltree = "this.is.a.path"
    assert [[ltree]] == query("SELECT $1::ltree", [ltree])
  end

  test "fail on encode arrays", context do
    assert_raise ArgumentError, "nested lists must have lists with matching lengths", fn ->
      query("SELECT $1::integer[]", [[[1], [1, 2]]])
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
    assert [[2, 4], [6, 8]] = query("VALUES (1, 2), (3, 4)", [], decode_mapper: map)
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

  test "prepare_execute, execute and close", context do
    assert {query, [[42]]} = prepare_execute("42", "SELECT $1::int", [42])
    assert [[41]] = execute(query, [41])
    assert :ok = close(query)
    assert [[43]] = execute(query, [43])
  end

  test "prepare and execute different queries with same name", context do
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
    assert {:ok, ^query, %Postgrex.Result{rows: [[42]]}} = Postgrex.execute(pid2, query, [])
    assert {:ok, %Postgrex.Result{rows: [[41]]}} = Postgrex.query(pid2, "SELECT 41", [])
  end

  test "execute prepared query when deallocated", context do
    query = prepare("S42", "SELECT 42")
    assert query("DEALLOCATE ALL", []) == :ok
    assert %Postgrex.Error{} = execute(query, [])
    assert execute(query, []) == [[42]]
  end

  test "error codes are translated", context do
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

    assert %Postgrex.Error{postgres: %{code: :in_failed_sql_transaction}} = query("SELECT 42", [])
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
    assert %Postgrex.Error{postgres: %{code: :unique_violation}} = execute(query, [])
    assert %Postgrex.Error{postgres: %{code: :unique_violation}} = execute(query, [])
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
      spawn_link(fn ->
        send(self_pid, query("SELECT pg_sleep(0.05)", []))
      end)
    end)

    assert [[42]] = query("SELECT 42", [])

    Enum.each(1..10, fn _ ->
      assert_receive [[:void]]
    end)
  end

  test "raise when trying to execute unprepared query", context do
    assert_raise ArgumentError, ~r/has not been prepared/, fn ->
      execute(%Postgrex.Query{name: "hi", statement: "BEGIN"}, [])
    end
  end

  test "raise when trying to parse prepared query", context do
    assert_raise ArgumentError, ~r/has already been prepared/, fn ->
      DBConnection.Query.parse(prepare("SELECT 42", []), [])
    end
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

  test "query from child spec", %{options: opts, test: test} do
    child_spec = Postgrex.child_spec([name: test] ++ opts)
    Supervisor.start_link([child_spec], strategy: :one_for_one)
    %Postgrex.Result{rows: [[42]]} = Postgrex.query!(test, "SELECT 42", [])
  end

  test "query before and after idle ping" do
    opts = [database: "postgrex_test", backoff_type: :stop, idle_interval: 1]
    {:ok, pid} = P.start_link(opts)
    assert {:ok, _} = P.query(pid, "SELECT 42", [])
    :timer.sleep(20)
    assert {:ok, _} = P.query(pid, "SELECT 42", [])
    :timer.sleep(20)
    assert {:ok, _} = P.query(pid, "SELECT 42", [])
  end

  test "too many parameters query disconnects", context do
    Process.flag(:trap_exit, true)
    params = 1..0x10000
    query = ["INSERT INTO uniques VALUES (0)" | Enum.map(params, &[", ($#{&1}::int4)"])]
    params = Enum.into(params, [])
    message = "postgresql protocol can not handle 65536 parameters, the maximum is 65535"

    assert capture_log(fn ->
             %Postgrex.QueryError{message: ^message} = query(query, params)
             pid = context[:pid]
             assert_receive {:EXIT, ^pid, :killed}
           end) =~ message
  end

  test "COPY FROM STDIN disconnects", context do
    Process.flag(:trap_exit, true)
    message = "trying to copy in but no copy data to send"

    assert capture_log(fn ->
             assert %RuntimeError{message: runtime} = query("COPY uniques FROM STDIN", [])
             assert runtime =~ message
             pid = context[:pid]
             assert_receive {:EXIT, ^pid, :killed}
           end) =~ message
  end

  test "COPY TO STDOUT", context do
    assert [] = query("COPY uniques TO STDOUT", [])
    assert ["1\t2\n"] = query("COPY (VALUES (1, 2)) TO STDOUT", [])
    assert ["1\t2\n", "3\t4\n"] = query("COPY (VALUES (1, 2), (3, 4)) TO STDOUT", [])
  end

  test "COPY TO STDOUT with decoder_mapper", context do
    opts = [decode_mapper: &String.split/1]
    assert [["1", "2"], ["3", "4"]] = query("COPY (VALUES (1, 2), (3, 4)) TO STDOUT", [], opts)
  end

  test "receive packet with remainder greater than 64MB", context do
    # to ensure remainder is more than 64MB use 64MBx2+1
    big_binary = :binary.copy(<<1>>, 128 * 1024 * 1024 + 1)
    assert [[binary]] = query("SELECT $1::bytea;", [big_binary])
    assert byte_size(binary) == 128 * 1024 * 1024 + 1
  end

  test "terminate backend", context do
    Process.flag(:trap_exit, true)
    assert {:ok, pid} = P.start_link([idle_interval: 10] ++ context[:options])

    %Postgrex.Result{connection_id: connection_id} = Postgrex.query!(pid, "SELECT 42", [])

    assert capture_log(fn ->
             assert [[true]] = query("SELECT pg_terminate_backend($1)", [connection_id])
             assert_receive {:EXIT, ^pid, :killed}, 5000
           end) =~ "** (Postgrex.Error) FATAL 57P01 (admin_shutdown)"
  end

  test "terminate backend with socket", context do
    Process.flag(:trap_exit, true)
    socket = System.get_env("PG_SOCKET_DIR") || "/tmp"
    assert {:ok, pid} = P.start_link([idle_interval: 10, socket_dir: socket] ++ context[:options])

    %Postgrex.Result{connection_id: connection_id} = Postgrex.query!(pid, "SELECT 42", [])

    capture_log(fn ->
      assert [[true]] = query("SELECT pg_terminate_backend($1)", [connection_id])
      assert_receive {:EXIT, ^pid, :killed}, 5000
    end)
  end

  test "table reader integration", context do
    assert {:ok, res} =
             P.query(
               context[:pid],
               "SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS tab (x, y)",
               []
             )

    assert res |> Table.to_rows() |> Enum.to_list() == [
             %{"x" => 1, "y" => "a"},
             %{"x" => 2, "y" => "b"},
             %{"x" => 3, "y" => "c"}
           ]

    columns = Table.to_columns(res)
    assert Enum.to_list(columns["x"]) == [1, 2, 3]
    assert Enum.to_list(columns["y"]) == ["a", "b", "c"]

    assert {_, %{count: 3}, _} = Table.Reader.init(res)
  end

  test "search_path", context do
    :ok = query("CREATE SCHEMA test_schema", [])
    :ok = query("CREATE TABLE test_schema.test_table (id int, text text)", [])
    :ok = query("INSERT INTO test_schema.test_table VALUES (1, 'foo')", [])

    # search path does not contain the appropriate schema
    %Postgrex.Error{postgres: error} = query("SELECT * from test_table", [])
    assert error.message =~ "\"test_table\" does not exist"

    # search path does contain the appropriate schema
    {:ok, pid} = P.start_link(database: "postgrex_test", search_path: ["public", "test_schema"])
    %{rows: [[1, "foo"]]} = P.query!(pid, "SELECT * from test_table", [])
  end
end
