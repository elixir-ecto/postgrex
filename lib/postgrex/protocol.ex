defmodule Postgrex.Protocol.Messages do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      defrecordp :msg_auth, [:type, :data]
      defrecordp :msg_startup, [:params]
      defrecordp :msg_password, [:pass]
      defrecordp :msg_error, [:fields]
      defrecordp :msg_parameter, [:name, :value]
      defrecordp :msg_backend_key, [:pid, :key]
      defrecordp :msg_ready, [:status]
      defrecordp :msg_notice, [:fields]
      defrecordp :msg_parse, [:name, :query, :type_oids]
      defrecordp :msg_describe, [:type, :name]
      defrecordp :msg_flush, []
      defrecordp :msg_parse_complete, []
      defrecordp :msg_parameter_desc, [:type_oids]
      defrecordp :msg_row_desc, [:fields]
      defrecordp :msg_no_data, []
      defrecordp :msg_bind, [:name_port, :name_stat, :param_formats, :params,
                             :result_formats]
      defrecordp :msg_execute, [:name_port, :max_rows]
      defrecordp :msg_sync, []
      defrecordp :msg_query, [:query]
      defrecordp :msg_bind_complete, []
      defrecordp :msg_portal_suspend, []
      defrecordp :msg_data_row, [:values]
      defrecordp :msg_command_complete, [:tag]
      defrecordp :msg_empty_query, []
      defrecordp :msg_terminate, []
      defrecordp :msg_ssl_request, []

      defrecordp :row_field, [:name, :table_oid, :column, :type_oid, :type_size,
                              :type_mod, :format]
    end
  end
end

defmodule Postgrex.Protocol do
  @moduledoc false

  use Postgrex.Protocol.Messages
  import Postgrex.BinaryUtils

  @protocol_vsn_major 3
  @protocol_vsn_minor 0

  @auth_types [ ok: 0, kerberos: 2, cleartext: 3, md5: 5, scm: 6, gss: 7,
                sspi: 9, gss_cont: 8 ]

  @error_fields [ severity: ?S, code: ?C, message: ?M, detail: ?D, hint: ?H,
                  position: ?P, internal_position: ?p, internal_query: ?q,
                  where: ?W, schema: ?s, table: ?t, column: ?c, data_type: ?d,
                  constraint: ?n, file: ?F, line: ?L, routine: ?R ]

  ### decoders ###

  # auth
  def parse(?R, size, << type :: int32, rest :: binary >>) do
    type = decode_auth_type(type)
    case type do
      :md5 -> << data :: [binary, size(4)] >> = rest
      :gss_cont ->
        rest_size = size - 2
        << data :: size(rest_size) >> = rest
      _ -> data = nil
    end
    msg_auth(type: type, data: data)
  end

  # error
  def parse(?E, _size, rest) do
    fields = decode_fields(rest)
    msg_error(fields: fields)
  end

  # notice
  def parse(?N, _size, rest) do
    fields = decode_fields(rest)
    msg_notice(fields: fields)
  end

  # parameter
  def parse(?S, _size, rest) do
    { name, rest } = decode_string(rest)
    { value, "" } = decode_string(rest)
    msg_parameter(name: name, value: value)
  end

  # backend_key
  def parse(?K, _size, rest) do
    << pid :: int32, key :: int32 >> = rest
    msg_backend_key(pid: pid, key: key)
  end

  # ready
  def parse(?Z, _size, rest) do
    << status :: int8 >> = rest
    status = case status do
      ?I -> :idle
      ?T -> :transaction
      ?E -> :failed
    end
    msg_ready(status: status)
  end

  # parse_complete
  def parse(?1, _size, _rest) do
    msg_parse_complete()
  end

  # parameter_desc
  def parse(?t, _size, rest) do
    << len :: int16, rest :: binary(len, 32) >> = rest
    oids = lc << oid :: size(32) >> inbits rest, do: oid
    msg_parameter_desc(type_oids: oids)
  end

  # row_desc
  def parse(?T, _size, rest) do
    << len :: int16, rest :: binary >> = rest
    fields = decode_row_fields(rest, len)
    msg_row_desc(fields: fields)
  end

  # no_data
  def parse(?n, _size, _rest) do
    msg_no_data()
  end

  # bind_complete
  def parse(?2, _size, _rest) do
    msg_bind_complete()
  end

  # portal_suspended
  def parse(?s, _size, _rest) do
    msg_portal_suspend()
  end

  # data_row
  def parse(?D, _size, rest) do
    << count :: int16, rest :: binary >> = rest
    values = decode_row_values(rest, count)
    msg_data_row(values: values)
  end

  # command_complete
  def parse(?C, _size, rest) do
    { tag, "" } = decode_string(rest)
    msg_command_complete(tag: tag)
  end

  # empty_query
  def parse(?I, _size, _rest) do
    msg_empty_query()
  end

  ### encoders ###

  def msg_to_binary(msg) do
    { first, iolist } = to_binary(msg)
    binary = iolist_to_binary(iolist)
    size = byte_size(binary) + 4

    if first do
      << first :: int8, size :: int32, binary :: binary >>
    else
     << size :: int32, binary :: binary >>
   end
  end

  # startup
  defp to_binary(msg_startup(params: params)) do
    params = Enum.map(params, fn { key, value } ->
      [ to_string(key), 0, value, 0 ]
    end)
    vsn = << @protocol_vsn_major :: int16, @protocol_vsn_minor :: int16 >>
    { nil, [vsn, params, 0] }
  end

  # password
  defp to_binary(msg_password(pass: pass)) do
    { ?p, [pass, 0] }
  end

  # parse
  defp to_binary(msg_parse(name: name, query: query, type_oids: oids)) do
    oids = bc oid inlist oids, do: << oid :: int32 >>
    len = << div(byte_size(oids), 4) :: int16 >>
    { ?P, [name, 0, query, 0, len, oids] }
  end

  # describe
  defp to_binary(msg_describe(type: type, name: name)) do
    byte = case type do
      :statement -> ?S
      :portal -> ?P
    end
    { ?D, [byte, name, 0] }
  end

  # flush
  defp to_binary(msg_flush()) do
    { ?H, "" }
  end

  # bind
  defp to_binary(msg_bind(name_port: port, name_stat: stat, param_formats: param_formats,
                          params: params, result_formats: result_formats)) do
    pfs = bc format inlist param_formats,  do: << format(format) :: int16 >>
    rfs = bc format inlist result_formats, do: << format(format) :: int16 >>
    ps  = bc param  inlist params,         do: << encode_param(param) :: binary >>

    len_pfs = << div(byte_size(pfs), 2) :: int16 >>
    len_rfs = << div(byte_size(rfs), 2) :: int16 >>
    len_ps  = << length(params) :: int16 >>

    { ?B, [ port, 0, stat, 0, len_pfs, pfs, len_ps, ps, len_rfs, rfs ] }
  end

  # execute
  defp to_binary(msg_execute(name_port: port, max_rows: rows)) do
    { ?E, [port, 0, << rows :: int32 >>] }
  end

  # sync
  defp to_binary(msg_sync()) do
    { ?S, "" }
  end

  # query
  defp to_binary(msg_query(query: query)) do
    IO.puts "Query #{query}"
    { ?Q, [query, 0]}
  end

  # terminate
  defp to_binary(msg_terminate()) do
    { ?X, "" }
  end

  # ssl_request
  defp to_binary(msg_ssl_request()) do
    { nil, << 1234 :: int16, 5679 :: int16 >> }
  end

  ### encode helpers ###

  defp format(:text),   do: 0
  defp format(:binary), do: 1

  defp encode_param(param) do
    if nil?(param) do
      << -1 :: int32 >>
    else
      << byte_size(param) :: int32, param :: binary >>
    end
  end

  ### decode helpers ###

  defp decode_fields(<< 0 >>), do: []

  defp decode_fields(<< field :: int8, rest :: binary >>) do
    type = decode_field_type(field)
    { string, rest } = decode_string(rest)
    [ { type, string } | decode_fields(rest) ]
  end

  defp decode_string(bin) do
    { pos, 1 } = :binary.match(bin, << 0 >>)
    { string, << 0, rest :: binary >> } = :erlang.split_binary(bin, pos)
    { string, rest }
  end

  defp decode_row_fields("", 0), do: []

  defp decode_row_fields(rest, count) do
    { field, rest } = decode_row_field(rest)
    [ field | decode_row_fields(rest, count-1) ]
  end

  defp decode_row_field(rest) do
    { name, rest } = decode_string(rest)
    << table_oid :: int32, column :: int16, type_oid :: int32,
       type_size :: int16, type_mod :: int32, format :: int16,
       rest :: binary >> = rest
    field = row_field(name: name, table_oid: table_oid, column: column, type_oid: type_oid,
                      type_size: type_size, type_mod: type_mod, format: format)
    { field, rest }
  end

  defp decode_row_values("", 0), do: []

  defp decode_row_values(<< -1 :: int32, rest :: binary >>, count) do
    [ nil | decode_row_values(rest, count-1) ]
  end

  defp decode_row_values(<< length :: int32, value :: binary(length), rest :: binary >>, count) do
    [ value | decode_row_values(rest, count-1) ]
  end

  Enum.each(@auth_types, fn { type, value } ->
    def decode_auth_type(unquote(value)), do: unquote(type)
  end)

  Enum.each(@error_fields, fn { field, char } ->
    def decode_field_type(unquote(char)), do: unquote(field)
  end)
  def decode_field_type(_), do: :unknown
end
