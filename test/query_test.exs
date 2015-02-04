defmodule QueryTest do
  use ExUnit.Case, async: true
  import Postgrex.TestHelper
  alias Postgrex.Connection, as: P

  setup do
    opts = [ database: "postgrex_test" ]
    {:ok, pid} = P.start_link(opts)
    {:ok, [pid: pid]}
  end

  test "rebootstrap", context do
    assert [{42}] = query("SELECT $1::int", [42])
    P.rebootstrap(context.pid)
    assert [{42}] = query("SELECT $1::int", [42])
  end

  test "iodata", context do
    assert [{123}] = query(["S", ?E, ["LEC"|"T"], " ", '123'], [])
  end

  test "decode basic types", context do
    assert [{nil}] = query("SELECT NULL", [])
    assert [{true, false}] = query("SELECT true, false", [])
    assert [{"e"}] = query("SELECT 'e'::char", [])
    assert [{"ẽ"}] = query("SELECT 'ẽ'::char", [])
    assert [{42}] = query("SELECT 42", [])
    assert [{42.0}] = query("SELECT 42::float", [])
    assert [{:NaN}] = query("SELECT 'NaN'::float", [])
    assert [{:inf}] = query("SELECT 'inf'::float", [])
    assert [{:"-inf"}] = query("SELECT '-inf'::float", [])
    assert [{"ẽric"}] = query("SELECT 'ẽric'", [])
    assert [{"ẽric"}] = query("SELECT 'ẽric'::varchar", [])
    assert [{<<1, 2, 3>>}] = query("SELECT '\\001\\002\\003'::bytea", [])
  end

  test "decode numeric", context do
    assert [{Decimal.new("42")}] == query("SELECT 42::numeric", [])
    assert [{Decimal.new("42.0000000000")}] == query("SELECT 42.0::numeric(100, 10)", [])
    assert [{Decimal.new("0.4242")}] == query("SELECT 0.4242", [])
    assert [{Decimal.new("42.4242")}] == query("SELECT 42.4242", [])
    assert [{Decimal.new("12345.12345")}] == query("SELECT 12345.12345", [])
    assert [{Decimal.new("0.00012345")}] == query("SELECT 0.00012345", [])
    assert [{Decimal.new("1000000000.0")}] == query("SELECT 1000000000.0", [])
    assert [{Decimal.new("1000000000.1")}] == query("SELECT 1000000000.1", [])
    assert [{Decimal.new("123456789123456789123456789")}] == query("SELECT 123456789123456789123456789::numeric", [])
    assert [{Decimal.new("123456789123456789123456789.123456789")}] == query("SELECT 123456789123456789123456789.123456789", [])
    assert [{Decimal.new("1.1234500000")}] == query("SELECT 1.1234500000", [])
    assert [{Decimal.new("NaN")}] == query("SELECT 'NaN'::numeric", [])
  end

  test "decode uuid", context do
    uuid = <<160,238,188,153,156,11,78,248,187,109,107,185,189,56,10,17>>
    assert [{^uuid}] = query("SELECT 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid", [])
  end

  test "decode arrays", context do
    assert [{[]}] = query("SELECT ARRAY[]::integer[]", [])
    assert [{[1]}] = query("SELECT ARRAY[1]", [])
    assert [{[1,2]}] = query("SELECT ARRAY[1,2]", [])
    assert [{[[0],[1]]}] = query("SELECT ARRAY[[0],[1]]", [])
    assert [{[[0]]}] = query("SELECT ARRAY[ARRAY[0]]", [])
  end

  test "decode time", context do
    assert [{{0,0,0}}] = query("SELECT time '00:00:00'", [])
    assert [{{1,2,3}}] = query("SELECT time '01:02:03'", [])
    assert [{{23,59,59}}] = query("SELECT time '23:59:59'", [])
    assert [{{4,5,6}}] = query("SELECT time '04:05:06 PST'", [])
    # query("SELECT time '00:00:00.123'", [])
    # query("SELECT time '00:00:00.123456'", [])

    assert [{{0,0,0}}] = query("SELECT timetz '00:00:00'", [])
    assert [{{1,2,3}}] = query("SELECT timetz '01:02:03+100'", [])
    assert [{{23,59,59}}] = query("SELECT timetz '23:59:59-100'", [])
    assert [{{4,5,6}}] = query("SELECT timetz '04:05:06 PST'", [])
    # query("SELECT time '00:00:00.123456+100'", [])
  end

  test "decode date", context do
    assert [{{1,1,1}}] = query("SELECT date '0001-01-01'", [])
    assert [{{1,2,3}}] = query("SELECT date '0001-02-03'", [])
    assert [{{2013,9,23}}] = query("SELECT date '2013-09-23'", [])
    # query("SELECT date 'infinity'", [])
    # query("SELECT date '-infinity'", [])
    # query("SELECT date 'January 8, 99 BC'", [])
    # query("SELECT date '10000-1-1'", [])
  end

  test "decode timestamp", context do
    assert [{{{1,1,1},{0,0,0}}}] = query("SELECT timestamp '0001-01-01 00:00:00'", [])
    assert [{{{2013,9,23},{14,4,37}}}] = query("SELECT timestamp '2013-09-23 14:04:37.123'", [])
    assert [{{{2013,9,23},{14,4,37}}}] = query("SELECT timestamp '2013-09-23 14:04:37 PST'", [])

    # assert [{{{1,1,1},{0,0,0}}}] = query("SELECT timestamptz '0001-01-01 00:00:00'", [])
    # assert [{{{2013,9,23},{14,4,37}}}] = query("SELECT timestamptz '2013-09-23 14:04:37.123'", [])
    # assert [{{{2013,9,23},{14,4,37}}}] = query("SELECT timestamptz '2013-09-23 14:04:37 PST'", [])
    # assert [{{{2013,9,23},{14,4,37}}}] = query("SELECT timestamptz '2013-09-23 14:04:37.123 PST'", [])

  end

  test "decode interval", context do
    assert [{%{year: 0, mon: 0, day: 0, hour: 0, min: 0, sec: 0}}] = query("SELECT interval '0'", [])
    assert [{%{year: 0, mon: 0, day: 100, hour: 0, min: 0, sec: 0}}] = query("SELECT interval '100 days'", [])
    assert [{%{year: 0, mon: 0, day: 0, hour: 50, min: 0, sec: 0}}] = query("SELECT interval '50 hours'", [])
    assert [{%{year: 0, mon: 0, day: 0, hour: 0, min: 0, sec: 1}}] = query("SELECT interval '1 second'", [])
    assert [{%{year: 1, mon: 2, day: 40, hour: 3, min: 2, sec: 0}}] = query("SELECT interval '1 year 2 months 40 days 3 hours 2 minutes'", [])
  end

  test "decode record", context do
    assert [{{1, "2"}}] = query("SELECT (1, '2')::composite1", [])
    assert [{[{1, "2"}]}] = query("SELECT ARRAY[(1, '2')::composite1]", [])
  end

  @tag min_pg_version: "9.2"
  test "decode range", context do
    assert [{{2,4}}] = query("SELECT '(1,5)'::int4range", [])
    assert [{{1,6}}] = query("SELECT '[1,6]'::int4range", [])
    assert [{{:"-inf",4}}] = query("SELECT '(,5)'::int4range", [])
    assert [{{1,:inf}}] = query("SELECT '[1,)'::int4range", [])

    assert [{{3,7}}] = query("SELECT '(2,8)'::int8range", [])
    assert [{{2,4}}] = query("SELECT '[2,4]'::int8range", [])
    assert [{{:"-inf",3}}] = query("SELECT '(,4)'::int8range", [])
    assert [{{7,:inf}}] = query("SELECT '(6,]'::int8range", [])

    assert [{{Decimal.new("1.0"),Decimal.new("5.999")}}] == query("SELECT numrange(1.0,5.999)", [])
    assert [{{Decimal.new("1.0"),Decimal.new("5.999")}}] == query("SELECT '[1.0,5.999]'::numrange", [])
    assert [{{:"-inf",Decimal.new("1.0000000001")}}] == query("SELECT numrange(NULL,1.0000000001)", [])
    assert [{{Decimal.new("99999999999.9"),:inf}}] == query("SELECT '[99999999999.9,]'::numrange", [])

    # assert [{{{2014,1,1},{2014,12,30}}}] = query("SELECT '[1-1-2014,12-31-2014)'::daterange", [])
    # assert [{{{2014,1,2},{2014,12,31}}}] = query("SELECT '(1-1-2014,12-31-2014]'::daterange", [])
    # assert [{{:"-inf",{2014,12,30}}}] = query("SELECT '(,12-31-2014)'::daterange", [])
    # assert [{{{2014,1,2},:inf}}] = query("SELECT '(1-1-2014,]'::daterange", [])

    # assert [{{{{2014,1,1},{12,0,0}},{{2014,12,31},{12,0,0}}}}] = query("SELECT '[1-1-2014 12:00:00, 12-31-2014 12:00:00)'::tsrange", [])
    # assert [{{{{2014,1,1},{12,0,0}},{{2014,12,31},{12,0,0}}}}] = query("SELECT '(1-1-2014 12:00:00, 12-31-2014 12:00:00]'::tsrange", [])
    # assert [{{:"-inf",{{2014,12,31},{12,0,0}}}}] = query("SELECT '[,12-31-2014 12:00:00)'::tsrange", [])
    # assert [{{{{2014,1,1},{12,0,0}},:inf}}] = query("SELECT '[1-1-2014 12:00:00,)'::tsrange", [])

    # assert [{{{{2014,1,1},{20,0,0}},{{2014,12,31},{20,0,0}}}}] = query("SELECT '[1-1-2014 12:00:00-800, 12-31-2014 12:00:00-800)'::tstzrange", [])
    # assert [{{:"-inf",{{2014,12,31},{8,0,0}}}}] = query("SELECT '[,12-31-2014 12:00:00+400]'::tstzrange", [])
    # assert [{{{{2014,1,1},{16,0,0}},:inf}}] = query("SELECT '(1-1-2014 12:00:00-4:00:00,]'::tstzrange", [])
  end

  test "encode basic types", context do
    assert [{nil, nil}] = query("SELECT $1::text, $2::int", [nil, nil])
    assert [{true, false}] = query("SELECT $1::bool, $2::bool", [true, false])
    assert [{"ẽ"}] = query("SELECT $1::char", ["ẽ"])
    assert [{42}] = query("SELECT $1::int", [42])
    assert [{42.0, 43.0}] = query("SELECT $1::float, $2::float", [42, 43.0])
    assert [{:NaN}] = query("SELECT $1::float", [:NaN])
    assert [{:inf}] = query("SELECT $1::float", [:inf])
    assert [{:"-inf"}] = query("SELECT $1::float", [:"-inf"])
    assert [{"ẽric"}] = query("SELECT $1::varchar", ["ẽric"])
    assert [{<<1, 2, 3>>}] = query("SELECT $1::bytea", [<<1, 2, 3>>])
  end

  test "encode numeric", context do
    nums = [
      "42",
      "0.4242",
      "42.4242",
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
      assert [{dec}] == query("SELECT $1::numeric", [dec])
    end)
  end

  test "encode enforces bounds on integers", context do
    # int2's range is -32768 to +32767
    assert [{-32768}] = query("SELECT $1::int2", [-32768])
    assert [{32767}] = query("SELECT $1::int2", [32767])
    assert :function_clause = catch_error(query("SELECT $1::int2", [32767 + 1]))
    assert :function_clause = catch_error(query("SELECT $1::int2", [-32768 - 1]))

    # int4's range is -2147483648 to +2147483647
    assert [{-2147483648}] = query("SELECT $1::int4", [-2147483648])
    assert [{2147483647}] = query("SELECT $1::int4", [2147483647])
    assert :function_clause = catch_error(query("SELECT $1::int4", [2147483647 + 1]))
    assert :function_clause = catch_error(query("SELECT $1::int4", [-2147483648 - 1]))

    # int8's range is  -9223372036854775808 to 9223372036854775807
    assert [{-9223372036854775808}] = query("SELECT $1::int8", [-9223372036854775808])
    assert [{9223372036854775807}] = query("SELECT $1::int8", [9223372036854775807])
    assert :function_clause = catch_error(query("SELECT $1::int8", [9223372036854775807 + 1]))
    assert :function_clause = catch_error(query("SELECT $1::int8", [-9223372036854775808 - 1]))
  end

  test "encode uuid", context do
    uuid = <<0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15>>
    assert [{^uuid}] = query("SELECT $1::uuid", [uuid])
  end

  test "encode date", context do
    assert [{{1,1,1}}] = query("SELECT $1::date", [{1,1,1}])
    assert [{{1,2,3}}] = query("SELECT $1::date", [{1,2,3}])
    assert [{{2013,9,23}}] = query("SELECT $1::date", [{2013,9,23}])
    assert [{{1999,12,31}}] = query("SELECT $1::date", [{1999,12,31}])
  end

  test "encode time", context do
    assert [{{0,0,0}}] = query("SELECT $1::time", [{0,0,0}])
    assert [{{1,2,3}}] = query("SELECT $1::time", [{1,2,3}])
    assert [{{23,59,59}}] = query("SELECT $1::time", [{23,59,59}])
    assert [{{4,5,6}}] = query("SELECT $1::time", [{4,5,6}])
  end

  test "encode timestamp", context do
    assert [{{{1,1,1},{0,0,0}}}] =
      query("SELECT $1::timestamp", [{{1,1,1},{0,0,0}}])
    assert [{{{2013,9,23},{14,4,37}}}] =
      query("SELECT $1::timestamp", [{{2013,9,23},{14,4,37}}])
    assert [{{{2013,9,23},{14,4,37}}}] =
      query("SELECT $1::timestamp", [{{2013,9,23},{14,4,37}}])
  end

  test "encode interval", context do
    assert [{%{year: 0, mon: 0, day: 0, hour: 0, min: 0, sec: 0}}] =
      query("SELECT $1::interval", [%{year: 0, mon: 0, day: 0, hour: 0, min: 0, sec: 0}])
    assert [{%{year: 0, mon: 0, day: 0, hour: 0, min: 2, sec: 0}}] =
      query("SELECT $1::interval", [%{year: 0, mon: 0, day: 0, hour: 0, min: 2, sec: 0}])
    assert [{%{year: 8, mon: 4, day: 0, hour: 0, min: 0, sec: 0}}] =
      query("SELECT $1::interval", [%{year: 0, mon: 100, day: 0, hour: 0, min: 0, sec: 0}])
    assert [{%{year: 1, mon: 2, day: 40, hour: 3, min: 2, sec: 0}}] =
      query("SELECT $1::interval", [%{year: 1, mon: 2, day: 40, hour: 3, min: 2, sec: 0}])
  end

  test "encode arrays", context do
    assert [{[]}] = query("SELECT $1::integer[]", [[]])
    assert [{[1]}] = query("SELECT $1::integer[]", [[1]])
    assert [{[1,2]}] = query("SELECT $1::integer[]", [[1,2]])
    assert [{[[0],[1]]}] = query("SELECT $1::integer[]", [[[0],[1]]])
    assert [{[[0]]}] = query("SELECT $1::integer[]", [[[0]]])
    assert [{[1, nil, 3]}] = query("SELECT $1::integer[]", [[1, nil, 3]])
  end

  test "encode record", context do
    assert [{{1, "2"}}] = query("SELECT $1::composite1", [{1, "2"}])
    assert [{[{1, "2"}]}] = query("SELECT $1::composite1[]", [[{1, "2"}]])
    assert [{{1, nil, 3}}] = query("SELECT $1::composite2", [{1, nil, 3}])
  end

  @tag min_pg_version: "9.2"
  test "encode range", context do
    assert [{{1,3}}] = query("SELECT $1::int4range", [{1,3}])
    assert [{{:"-inf",5}}] = query("SELECT $1::int4range", [{:"-inf",5}])
    assert [{{3,:inf}}] = query("SELECT $1::int4range", [{3,:inf}])

    assert [{{2,9}}] = query("SELECT $1::int8range", [{2,9}])
    assert [{{:"-inf",3}}] = query("SELECT $1::int8range", [{:"-inf",3}])
    assert [{{6,:inf}}] = query("SELECT $1::int8range", [{6,:inf}])

    assert [{{Decimal.new("0.1"),Decimal.new("9.9")}}] == query("SELECT $1::numrange", [{Decimal.new("0.1"),Decimal.new("9.9")}])
    assert [{{:"-inf",Decimal.new("99999.99999")}}] == query("SELECT $1::numrange", [{:"-inf",Decimal.new("99999.99999")}])
    assert [{{Decimal.new("0.000000001"),:inf}}] == query("SELECT $1::numrange", [{Decimal.new("0.000000001"),:inf}])

    # assert [{{{2014,1,1},{2014,12,31}}}] = query("SELECT $1::daterange", [{{2014,1,1},{2014,12,31}}])
    # assert [{{:"-inf",{2014,12,31}}}] = query("SELECT $1::daterange", [{:"-inf",{2014,12,31}}])
    # assert [{{{2014,1,1},:inf}}] = query("SELECT $1::daterange", [{{2014,1,1},:inf}])

    # assert [{{{{2014,1,1},{12,0,0}},{{2014,12,31},{12,0,0}}}}] = query("SELECT $1::tsrange", [{{{2014,1,1},{12,0,0}},{{2014,12,31},{12,0,0}}}])
    # assert [{{:"-inf",{{2014,12,31},{12,0,0}}}}] = query("SELECT $1::tsrange", [{:"-inf",{{2014,12,31},{12,0,0}}}])
    # assert [{{{{2014,1,1},{12,0,0}},:inf}}] = query("SELECT $1::tsrange", [{{{2014,1,1},{12,0,0}},:inf}])

    # assert [{{{{2014,1,1},{12,0,0}},{{2014,12,31},{12,0,0}}}}] = query("SELECT $1::tstzrange", [{{{2014,1,1},{12,0,0}},{{2014,12,31},{12,0,0}}}])
    # assert [{{:"-inf",{{2014,12,31},{12,0,0}}}}] = query("SELECT $1::tstzrange", [{:"-inf",{{2014,12,31},{12,0,0}}}])
    # assert [{{{{2014,1,1},{12,0,0}},:inf}}] = query("SELECT $1::tstzrange", [{{{2014,1,1},{12,0,0}},:inf}])
  end

  @tag min_pg_version: "9.2"
  test "encode enforces bounds on integer ranges", context do
    # int4's range is -2147483648 to +2147483647,
    # but Postgres' ranges are lower bound inclusive, upper bound exclusive
    assert [{{-2147483648, 0}}] = query("SELECT $1::int4range", [{-2147483648, 0}])
    assert [{{0, 2147483646}}] = query("SELECT $1::int4range", [{0, 2147483646}])
    assert :function_clause = catch_error(query("SELECT $1::int4range", [{-2147483648 - 1, 0}]))
    assert :function_clause = catch_error(query("SELECT $1::int4range", [{0, 2147483646 + 1}]))

    # int8's range is -9223372036854775808 to 9223372036854775807,
    # but Postgres' ranges are lower bound inclusive, upper bound exclusive
    assert [{{-9223372036854775807, 0}}] = query("SELECT $1::int8range", [{-9223372036854775807, 0}])
    assert [{{0, 9223372036854775806}}] = query("SELECT $1::int8range", [{0, 9223372036854775806}])
    assert :function_clause = catch_error(query("SELECT $1::int8range", [{-9223372036854775808 - 1, 0}]))
    assert :function_clause = catch_error(query("SELECT $1::int8range", [{0, 9223372036854775806 + 1}]))
  end

  test "fail on encode arrays", context do
    assert_raise ArgumentError, "nested lists must have lists with matching lengths", fn ->
      query("SELECT $1::integer[]", [[[1], [1,2]]])
    end
    assert [{42}] = query("SELECT 42", [])
  end

  test "fail on encode wrong value", context do
    assert_raise FunctionClauseError, fn ->
      query("SELECT $1::integer", ["123"])
    end
    assert_raise FunctionClauseError, fn ->
      query("SELECT $1::text", [4.0])
    end
    assert [{42}] = query("SELECT 42", [])
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

  test "error record", context do
    assert {:error, %Postgrex.Error{}} = P.query(context[:pid], "SELECT 123 + 'a'", [])
  end

  test "multi row result", context do
    assert {:ok, res} = P.query(context[:pid], "VALUES (1, 2), (3, 4)", [])
    assert res.num_rows == 2
    assert res.rows == [{1, 2}, {3, 4}]
  end

  test "insert", context do
    :ok = query("CREATE TABLE test (id int, text text)", [])
    [] = query("SELECT * FROM test", [])
    :ok = query("INSERT INTO test VALUES ($1, $2)", [42, "fortytwo"], [])
    [{42, "fortytwo"}] = query("SELECT * FROM test", [])
  end

  test "connection works after failure", context do
    assert %Postgrex.Error{} = query("wat", [])
    assert [{42}] = query("SELECT 42", [])
  end

  test "async test", context do
    self_pid = self
    Enum.each(1..10, fn _ ->
      spawn fn ->
        send self_pid, query("SELECT pg_sleep(0.1)", [])
      end
    end)

     Enum.each(1..10, fn _ ->
      assert_receive [{:void}], 1000
    end)
  end
end
