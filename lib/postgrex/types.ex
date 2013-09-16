defmodule Postgrex.Types do
  import Postgrex.BinaryUtils

  @types [
    bool: 16,
    bytea: 17,
    char: 18,
    name: 19,
    int8: 20,
    int2: 21,
    int2vector: 22,
    int4: 23,
    regproc: 24,
    text: 25,
    oid: 26,
    tid: 27,
    xid: 28,
    cid: 29,
    oidvector: 30,
    pg_type_reltype: 71,
    pg_attribute_reltype: 75,
    pg_proc_reltype: 81,
    pg_class_reltype: 83,
    xml: 142,
    point: 600,
    lseg: 601,
    path: 602,
    box: 603,
    polygon: 604,
    line: 628,
    float4: 700,
    float8: 701,
    abstime: 702,
    reltime: 703,
    tinterval: 704,
    unknown: 705,
    circle: 718,
    cash: 790,
    macaddr: 829,
    inet: 869,
    cidr: 650,
    boolarray: 1000,
    int2array: 1005,
    int4array: 1007,
    textarray: 1009,
    chararray: 1014,
    int8array: 1016,
    float4array: 1021,
    float8array: 1022,
    aclitem: 1033,
    cstringarray: 1263,
    bpchar: 1042,
    varchar: 1043,
    date: 1082,
    time: 1083,
    timestamp: 1114,
    timestamptz: 1184,
    interval: 1186,
    timetz: 1266,
    bit: 1560,
    varbit: 1562,
    numeric: 1700,
    refcursor: 1790,
    regprocedure: 2202,
    regoper: 2203,
    regoperator: 2204,
    regclass: 2205,
    regtype: 2206,
    regtypearray: 2211,
    tsvector: 3614,
    gtsvector: 3642,
    tsquery: 3615,
    regconfig: 3734,
    regdictionary: 3796,
    record: 2249,
    cstring: 2275,
    any: 2276,
    anyarray: 2277,
    void: 2278,
    trigger: 2279,
    language_handler: 2280,
    internal: 2281,
    opaque: 2282,
    anyelement: 2283,
    anynonarray: 2776,
    anyenum: 3500
  ]

  Enum.each(@types, fn { type, oid } ->
    def oid_to_type(unquote(oid)), do:  unquote(type)
    def type_to_oid(unquote(type)), do: unquote(oid)
  end)

  def decode(:bool, << 1 :: int8 >>), do: true
  def decode(:bool, << 0 :: int8 >>), do: false
  def decode(:bpchar, << c :: int8 >>), do: c
  def decode(:int2, << n :: int16 >>), do: n
  def decode(:int4, << n :: int32 >>), do: n
  def decode(:int8, << n :: int64 >>), do: n
  def decode(:float4, << n :: float32 >>), do: n
  def decode(:float8, << n :: float64 >>), do: n
  def decode(_, bin), do: bin
end
