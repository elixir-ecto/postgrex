defmodule Postgrex.Messages do
  @moduledoc false

  import Postgrex.BinaryUtils
  import Record, only: [defrecord: 2]

  @protocol_vsn_major 3
  @protocol_vsn_minor 0

  @auth_types [ ok: 0, kerberos: 2, cleartext: 3, md5: 5, scm: 6, gss: 7,
                sspi: 9, gss_cont: 8 ]

  @error_fields [ severity: ?S, code: ?C, message: ?M, detail: ?D, hint: ?H,
                  position: ?P, internal_position: ?p, internal_query: ?q,
                  where: ?W, schema: ?s, table: ?t, column: ?c, data_type: ?d,
                  constraint: ?n, file: ?F, line: ?L, routine: ?R ]

  defrecord :msg_auth, [:type, :data]
  defrecord :msg_startup, [:params]
  defrecord :msg_password, [:pass]
  defrecord :msg_error, [:fields]
  defrecord :msg_parameter, [:name, :value]
  defrecord :msg_backend_key, [:pid, :key]
  defrecord :msg_ready, [:status]
  defrecord :msg_notice, [:fields]
  defrecord :msg_parse, [:name, :query, :type_oids]
  defrecord :msg_describe, [:type, :name]
  defrecord :msg_flush, []
  defrecord :msg_parse_complete, []
  defrecord :msg_parameter_desc, [:type_oids]
  defrecord :msg_row_desc, [:fields]
  defrecord :msg_no_data, []
  defrecord :msg_notify, [:pg_pid, :channel, :payload]
  defrecord :msg_bind, [:name_port, :name_stat, :param_formats, :params,
                        :result_formats]
  defrecord :msg_execute, [:name_port, :max_rows]
  defrecord :msg_sync, []
  defrecord :msg_bind_complete, []
  defrecord :msg_portal_suspend, []
  defrecord :msg_data_row, [:values]
  defrecord :msg_command_complete, [:tag]
  defrecord :msg_empty_query, []
  defrecord :msg_terminate, []
  defrecord :msg_ssl_request, []

  defrecord :row_field, [:name, :table_oid, :column, :type_oid, :type_size,
                         :type_mod, :format]

  ### decoders ###

  # auth
  def parse(<<type :: int32, rest :: binary>>, ?R, size) do
    type = decode_auth_type(type)
    case type do
      :md5 ->
        <<data :: binary-size(4)>> = rest
      :gss_cont ->
        rest_size = size - 2
        <<data :: size(rest_size)>> = rest
      _ ->
        data = nil
    end
    msg_auth(type: type, data: data)
  end

  # backend_key
  def parse(<<pid :: int32, key :: int32>>, ?K, _size) do
    msg_backend_key(pid: pid, key: key)
  end

  # ready
  def parse(<<status :: int8>>, ?Z, _size) do
    status = case status do
      ?I -> :idle
      ?T -> :transaction
      ?E -> :failed
    end
    msg_ready(status: status)
  end

  # parameter_desc
  def parse(<<len :: int16, rest :: binary(len, 32)>>, ?t, _size) do
    oids = for <<oid :: size(32) <- rest>>, do: oid
    msg_parameter_desc(type_oids: oids)
  end

  # row_desc
  def parse(<<len :: int16, rest :: binary>>, ?T, _size) do
    fields = decode_row_fields(rest, len)
    msg_row_desc(fields: fields)
  end

  # data_row
  def parse(<<count :: int16, rest :: binary>>, ?D, _size) do
    values = decode_row_values(rest, count)
    msg_data_row(values: values)
  end

  # notify
  def parse(<<pg_pid :: int32, rest :: binary>>, ?A, _size) do
    {channel, rest} = decode_string(rest)
    {payload, ""} = decode_string(rest)
    msg_notify(pg_pid: pg_pid, channel: channel, payload: payload)
  end

  # error
  def parse(rest, ?E, _size) do
    fields = decode_fields(rest)
    msg_error(fields: fields)
  end

  # notice
  def parse(rest, ?N, _size) do
    fields = decode_fields(rest)
    msg_notice(fields: fields)
  end

  # parameter
  def parse(rest, ?S, _size) do
    {name, rest} = decode_string(rest)
    {value, ""} = decode_string(rest)
    msg_parameter(name: name, value: value)
  end

  # parse_complete
  def parse(_rest, ?1, _size) do
    msg_parse_complete()
  end

  # no_data
  def parse(_rest, ?n, _size) do
    msg_no_data()
  end

  # bind_complete
  def parse(_rest, ?2, _size) do
    msg_bind_complete()
  end

  # portal_suspended
  def parse(_rest, ?s, _size) do
    msg_portal_suspend()
  end

  # command_complete
  def parse(rest, ?C, _size) do
    {tag, ""} = decode_string(rest)
    msg_command_complete(tag: tag)
  end

  # empty_query
  def parse(_rest, ?I, _size) do
    msg_empty_query()
  end

  ### encoders ###

  def encode_msg(msg) do
    {first, data} = encode(msg)
    size = IO.iodata_length(data) + 4

    if first do
      [first, <<size :: int32>>, data]
    else
     [<<size :: int32>>, data]
   end
  end

  # startup
  defp encode(msg_startup(params: params)) do
    params = Enum.reduce(params, [], fn {key, value}, acc ->
      [acc, to_string(key), 0, value, 0]
    end)
    vsn = <<@protocol_vsn_major :: int16, @protocol_vsn_minor :: int16>>
    {nil, [vsn, params, 0]}
  end

  # password
  defp encode(msg_password(pass: pass)) do
    {?p, [pass, 0]}
  end

  # parse
  defp encode(msg_parse(name: name, query: query, type_oids: oids)) do
    oids = for oid <- oids, into: "", do: <<oid :: uint32>>
    len = <<div(byte_size(oids), 4) :: int16>>
    {?P, [name, 0, query, 0, len, oids]}
  end

  # describe
  defp encode(msg_describe(type: type, name: name)) do
    byte = case type do
      :statement -> ?S
      :portal -> ?P
    end
    {?D, [byte, name, 0]}
  end

  # flush
  defp encode(msg_flush()) do
    {?H, ""}
  end

  # bind
  defp encode(msg_bind(name_port: port, name_stat: stat, param_formats: param_formats,
                          params: params, result_formats: result_formats)) do
    pfs = for format <- param_formats,  into: "", do: <<format(format) :: int16>>
    rfs = for format <- result_formats, into: "", do: <<format(format) :: int16>>
    ps  = for param  <- params,                   do: param

    len_pfs = <<div(byte_size(pfs), 2) :: int16>>
    len_rfs = <<div(byte_size(rfs), 2) :: int16>>
    len_ps  = <<length(ps) :: int16>>

    {?B, [port, 0, stat, 0, len_pfs, pfs, len_ps, ps, len_rfs, rfs]}
  end

  # execute
  defp encode(msg_execute(name_port: port, max_rows: rows)) do
    {?E, [port, 0, <<rows :: int32>>]}
  end

  # sync
  defp encode(msg_sync()) do
    {?S, ""}
  end

  # terminate
  defp encode(msg_terminate()) do
    {?X, ""}
  end

  # ssl_request
  defp encode(msg_ssl_request()) do
    {nil, <<1234 :: int16, 5679 :: int16>>}
  end

  ### encode helpers ###

  defp format(:text),   do: 0
  defp format(:binary), do: 1

  ### decode helpers ###

  defp decode_fields(<<0>>), do: []

  defp decode_fields(<<field :: int8, rest :: binary>>) do
    type = decode_field_type(field)
    {string, rest} = decode_string(rest)
    [{type, string} | decode_fields(rest)]
  end

  defp decode_string(bin) do
    {pos, 1} = :binary.match(bin, <<0>>)
    {string, <<0, rest :: binary>>} = :erlang.split_binary(bin, pos)
    {string, rest}
  end

  defp decode_row_fields("", 0), do: []

  defp decode_row_fields(rest, count) do
    {field, rest} = decode_row_field(rest)
    [field | decode_row_fields(rest, count-1)]
  end

  defp decode_row_field(rest) do
    {name, rest} = decode_string(rest)
    <<table_oid :: uint32, column :: int16, type_oid :: uint32,
       type_size :: int16, type_mod :: int32, format :: int16,
       rest :: binary>> = rest
    field = row_field(name: name, table_oid: table_oid, column: column, type_oid: type_oid,
                      type_size: type_size, type_mod: type_mod, format: format)
    {field, rest}
  end

  defp decode_row_values("", 0), do: []

  defp decode_row_values(<<-1 :: int32, rest :: binary>>, count) do
    [nil | decode_row_values(rest, count-1)]
  end

  defp decode_row_values(<<length :: int32, value :: binary(length), rest :: binary>>, count) do
    [value | decode_row_values(rest, count-1)]
  end

  Enum.each(@auth_types, fn {type, value} ->
    def decode_auth_type(unquote(value)), do: unquote(type)
  end)

  Enum.each(@error_fields, fn {field, char} ->
    def decode_field_type(unquote(char)), do: unquote(field)
  end)
  def decode_field_type(_), do: :unknown
end
