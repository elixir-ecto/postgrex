defmodule Postgrex.Connection do
  use GenServer.Behaviour
  use Postgrex.Protocol.Messages
  alias Postgrex.Protocol
  alias Postgrex.Types
  import Postgrex.BinaryUtils

  # possible states: auth, init, parsing, describing, binding, executing, ready

  defrecordp :state, [ :opts, :sock, :tail, :state, :reply_to, :parameters,
                       :backend_key, :rows, :statement, :portal, :qparams,
                       :bootstrap, :types, :transactions ]

  defrecordp :statement, [:row_info, :columns]
  defrecordp :portal, [:param_oids]

  def start_link(opts, params // []) do
    case :gen_server.start_link(__MODULE__, [], []) do
      { :ok, pid } ->
        case :gen_server.call(pid, { :connect, opts, params }) do
          :ok -> { :ok, pid }
          err -> err
        end
      err -> err
    end
  end

  def stop(pid) do
    :gen_server.call(pid, :stop)
  end

  def query(pid, statement, params) do
    :gen_server.call(pid, { :query, statement, params })
  end

  def parameters(pid) do
    :gen_server.call(pid, :parameters)
  end

  def begin(pid) do
    :gen_server.call(pid, :begin)
  end

  def rollback(pid) do
    :gen_server.call(pid, :rollback)
  end

  def commit(pid) do
    :gen_server.call(pid, :commit)
  end

  def init([]) do
    { :ok, state(state: :ready, tail: "", parameters: [], rows: [],
                 bootstrap: false, transactions: 0) }
  end

  def handle_call(:stop, from, state(state: :ready) = s) do
    { :stop, :normal, state(s, reply_to: from) }
  end

  def handle_call({ :connect, opts, params }, from, state(state: :ready) = s) do
    sock_opts = [ { :active, :once }, { :packet, :raw }, :binary ]
    hostname = opts[:hostname]
    hostname = if is_binary(hostname), do: String.to_char_list!(hostname)

    case :gen_tcp.connect(hostname, opts[:port], sock_opts) do
      { :ok, sock } ->
        msg = msg_startup(params: [user: opts[:username], database: opts[:database]] ++ params)
        case send(msg, sock) do
          :ok ->
            { :noreply, state(s, opts: opts, sock: sock, reply_to: from, state: :auth) }
          { :error, reason } ->
            :gen_server.reply(from, Postgrex.Error[reason: "tcp send: #{reason}"])
            { :stop, :normal, s }
        end

      { :error, reason } ->
        :gen_server.reply(from, Postgrex.Error[reason: "tcp connect: #{reason}"])
        { :stop, :normal, s }
    end
  end

  def handle_call({ :query, statement, params }, from, state(state: :ready) = s) do
    case send_query(statement, s) do
      { :ok, s } ->
        { :noreply, state(s, qparams: params, reply_to: from) }
      { :error, reason, s } ->
        :gen_server.reply(from, { :error, reason })
        { :stop, :normal, s }
    end
  end

  def handle_call(:parameters, from, state(parameters: params, state: :ready) = s) do
    :gen_server.reply(from, params)
    { :noreply, s }
  end

  def handle_call(:begin, from, state(transactions: trans, state: :ready) = s) do
    if trans == 0 do
      s = state(s, transactions: 1)
      handle_call({ :query, "BEGIN", [] }, from, s)
    else
      s = state(s, transactions: trans + 1)
      handle_call({ :query, "SAVEPOINT postgrex_#{trans}", [] }, from, s)
    end
  end

  def handle_call(:rollback, from, state(transactions: trans, state: :ready) = s) do
    cond do
      trans == 0 ->
        :gen_server.reply(from, :ok)
        { :noreply, s }
      trans == 1 ->
        s = state(s, transactions: 0)
        handle_call({ :query, "ROLLBACK", [] }, from, s)
      true ->
        trans = trans - 1
        s = state(s, transactions: trans)
        handle_call({ :query, "ROLLBACK TO SAVEPOINT postgrex_#{trans}", [] }, from, s)
    end
  end

  def handle_call(:commit, from, state(transactions: trans, state: :ready) = s) do
    case trans do
      0 ->
        :gen_server.reply(from, :ok)
        { :noreply, s }
      1 ->
        s = state(s, transactions: 0)
        handle_call({ :query, "COMMIT", [] }, from, s)
      _ ->
        :gen_server.reply(from, :ok)
        { :noreply, state(s, transactions: trans - 1) }
    end
  end

  def handle_info({ :tcp, _, data }, state(reply_to: from, sock: sock, tail: tail) = s) do
    case handle_data(tail <> data, state(s, tail: "")) do
      { :ok, s } ->
        :inet.setopts(sock, active: :once)
        { :noreply, s }
      { :error, error, s } ->
        if from do
          :gen_server.reply(from, error)
          { :stop, :normal, s }
        else
          { :stop, error, s }
        end
    end
  end

  def handle_info({ :tcp_closed, _ }, state(reply_to: from) = s) do
    error = Postgrex.Error[reason: "tcp closed"]
    if from do
      :gen_server.reply(from, error)
      { :stop, :normal, s }
    else
      { :stop, error, s }
    end
  end

  def handle_info({ :tcp_error, _, reason }, state(reply_to: from) = s) do
    error = Postgrex.Error[reason: "tcp error: #{reason}"]
    if from do
      :gen_server.reply(from, error)
      { :stop, :normal, s }
    else
      { :stop, error, s }
    end
  end

  def terminate(reason, state(sock: sock) = s) do
    if sock do
      send(msg_terminate(), sock)
      :gen_tcp.close(sock)
    end
    if reason == :normal do
      reply(:ok, s)
    else
      reply(Postgrex.Error[reason: "terminated: #{reason}"], s)
    end
  end

  defp handle_data(<< type :: int8, size :: int32, data :: binary >> = tail, s) do
    size = size - 4

    case data do
      << data :: binary(size), tail :: binary >> ->
        msg = Protocol.decode(type, size, data)
        case message(msg, s) do
          { :ok, s } -> handle_data(tail, s)
          { :error, _, _ } = err -> err
        end
      _ ->
        { :ok, state(s, tail: tail) }
    end
  end

  defp handle_data(data, state(tail: tail) = s) do
    { :ok, state(s, tail: tail <> data) }
  end

  ### auth state ###

  defp message(msg_auth(type: :ok), state(state: :auth) = s) do
    { :ok, state(s, state: :init) }
  end

  defp message(msg_auth(type: :cleartext), state(opts: opts, state: :auth) = s) do
    msg = msg_password(pass: opts[:password])
    send_to_result(msg, s)
  end

  defp message(msg_auth(type: :md5, data: salt), state(opts: opts, state: :auth) = s) do
    digest = :crypto.hash(:md5, [opts[:password], opts[:username]]) |> hexify
    digest = :crypto.hash(:md5, [digest, salt]) |> hexify
    msg = msg_password(pass: ["md5", digest])
    send_to_result(msg, s)
  end

  defp message(msg_error(fields: fields), state(state: :auth) = s) do
    { :error, Postgrex.Error[postgres: fields], s }
  end

  ### init state ###

  defp message(msg_backend_key(pid: pid, key: key), state(state: :init) = s) do
    { :ok, state(s, backend_key: { pid, key }) }
  end

  defp message(msg_ready(), state(state: :init) = s) do
    s = state(s, bootstrap: true)
    send_query(Types.bootstrap_query, state(s, qparams: []))
  end

  defp message(msg_error(fields: fields), state(state: :init) = s) do
    { :error, Postgrex.Error[postgres: fields], s }
  end

  ### parsing state ###

  defp message(msg_parse_complete(), state(state: :parsing) = s) do
    { :ok, state(s, state: :describing) }
  end

  ### describing state ###

  defp message(msg_parse_complete(), state(state: :describing) = s) do
    msgs = [
        msg_bind(name_port: "", name_stat: "", param_formats: [], params: [], result_formats: []),
        msg_execute(name_port: "", max_rows: 0),
        msg_sync() ]

      case send_to_result(msgs, s) do
        { :ok, s } ->
          { :ok, state(s, qparams: nil) }
        err ->
          err
      end
  end

  defp message(msg_parameter_desc(type_oids: oids), state(state: :describing) = s) do
    { :ok, state(s, portal: portal(param_oids: oids)) }
  end

  defp message(msg_row_desc(fields: fields), state(types: types, bootstrap: bootstrap, qparams: params,
                                                   portal: portal, opts: opts, state: :describing) = s) do
    rfs = []
    if not bootstrap do
      { info, rfs, cols } = extract_row_info(fields, types)
      stat = statement(columns: cols, row_info: list_to_tuple(info))
    end

    param_oids = portal(portal, :param_oids)
    try do
      encoder = opts[:encoder]
      zipped = Enum.zip(param_oids, params)
      params = Enum.map(zipped, fn { oid, param } ->
        sender = Types.oid_to_sender(types, oid)
        if encoder, do: param = encoder.pre_encode(sender, oid, param)
        value = if Types.can_decode?(types, oid), do: Types.encode(sender, param, oid, types)
        if encoder, do: encoder.post_encode(sender, oid, param, value), else: value
      end)

      msgs = [
        msg_bind(name_port: "", name_stat: "", param_formats: [:binary], params: params, result_formats: rfs),
        msg_execute(name_port: "", max_rows: 0),
        msg_sync() ]

      case send_to_result(msgs, s) do
        { :ok, s } ->
          { :ok, state(s, statement: stat, qparams: nil) }
        err ->
          err
      end
    catch
      { :postgrex_encode, msg } ->
        s = reply(Postgrex.Error[reason: msg], s)
        { :ok, state(s, portal: nil, qparams: nil, state: :ready) }
    end
  end

  defp message(msg_no_data(), state(state: :describing) = s) do
    { :ok, s }
  end

  defp message(msg_ready(), state(state: :describing) = s) do
    { :ok, state(s, state: :binding) }
  end

  ### binding state ###

  defp message(msg_bind_complete(), state(state: :binding) = s) do
    { :ok, state(s, state: :executing) }
  end

  ### executing state ###

  # defp message(msg_portal_suspend(), state(state: :executing) = s)

  defp message(msg_data_row(values: values), state(rows: rows, state: :executing) = s) do
    { :ok, state(s, rows: [values|rows]) }
  end

  defp message(msg_command_complete(), state(bootstrap: true, rows: rows, state: :executing) = s) do
    types = Types.build_types(rows)
    s = reply(:ok, s)
    { :ok, state(s, rows: [], bootstrap: false, types: types) }
  end

  defp message(msg_command_complete(tag: tag), state(statement: stat,
      types: types, rows: rows, opts: opts, state: :executing) = s) do
    if nil?(stat) do
      s = reply(create_result(tag), s)
    else
      statement(row_info: info, columns: cols) = stat
      decoder = opts[:decoder]

      result = Enum.reduce(rows, [], fn values, acc ->
        { _, row } = Enum.reduce(values, { 0, [] }, fn value, { count, list } ->
          { sender, oid, can_decode } = elem(info, count)
          if can_decode do
            decoded = Types.decode(sender, value, types)
          end
          if decoder, do: decoded = decoder.decode(sender, oid, value, decoded)
          if nil?(decoded), do: decoded = value
          { count+1, [decoded|list] }
        end)
        row = Enum.reverse(row) |> list_to_tuple
        [ row | acc ]
      end)

      s = reply(create_result(tag, result, cols), s)
    end
    { :ok, state(s, rows: [], statement: nil, portal: nil) }
  end

  defp message(msg_empty_query(), state(state: :executing) = s) do
    s = reply(Postgrex.Result[empty?: true], s)
    { :ok, s }
  end

  ### asynchronous messages ###

  defp message(msg_ready(), s) do
    { :ok, state(s, state: :ready) }
  end

  defp message(msg_parameter(name: name, value: value), state(parameters: params) = s) do
    params = Dict.put(params, name, value)
    { :ok, state(s, parameters: params) }
  end

  defp message(msg_error(fields: fields), s) do
    s = reply(Postgrex.Error[postgres: fields], s)
    # TODO: subscribers
    { :ok, s }
  end

  defp message(msg_notice(), s) do
    # TODO: subscribers
    { :ok, s }
  end

  ### helpers ###

  defp extract_row_info(fields, types) do
    Enum.map(fields, fn row_field(name: name, type_oid: oid) ->
      sender = Types.oid_to_sender(types, oid)
      can_decode = Types.can_decode?(types, oid)
      format = if can_decode, do: :binary, else: :text
      { { sender, oid, can_decode }, format, name }
    end) |> List.unzip |> list_to_tuple
  end

  defp send_query(statement, s) do
    msgs = [
      msg_parse(name: "", query: statement, type_oids: []),
      msg_describe(type: :statement, name: ""),
      msg_sync() ]

    case send_to_result(msgs, s) do
      { :ok, s } ->
        { :ok, state(s, statement: nil, state: :parsing) }
      err ->
        err
    end
  end

  defp create_result(tag) do
    create_result(tag, true, nil, nil)
  end

  defp create_result(tag, rows, cols) do
    create_result(tag, false, rows, cols)
  end

  defp create_result(tag, empty, rows, cols) do
    { command, nrows } = decode_tag(tag)
    Postgrex.Result[command: command, size: nrows, rows: rows, empty?: empty,
                    columns: cols]
  end

  defp decode_tag(tag) do
    words = String.split(tag, " ")
    words = Enum.map(words, fn word ->
      case String.to_integer(word) do
        { num, "" } -> num
        :error -> word
      end
    end)

    { command, nums } = Enum.split_while(words, &is_binary(&1))
    command = Enum.join(command, "_") |> String.downcase |> binary_to_atom
    { command, List.last(nums) }
  end

  defp reply(_msg, state(reply_to: nil) = s), do: s

  defp reply(msg, state(reply_to: from) = s) do
    if from, do: :gen_server.reply(from, msg)
    state(s, reply_to: nil)
  end

  defp send(msg, state(sock: sock)), do: send(msg, sock)

  defp send(msgs, sock) when is_list(msgs) do
    binaries = Enum.map(msgs, &Protocol.encode(&1))
    :gen_tcp.send(sock, binaries)
  end

  defp send(msg, sock) do
    binary = Protocol.encode(msg)
    :gen_tcp.send(sock, binary)
  end

  defp send_to_result(msg, s) do
    case send(msg, s) do
      :ok ->
        { :ok, s }
      { :error, reason } ->
        { :error, Postgrex.Error[reason: "tcp send: #{reason}"] , s }
    end
  end

  defp hexify(bin) do
    bc << high :: size(4), low :: size(4) >> inbits bin do
      << hex_char(high), hex_char(low) >>
    end
  end

  defp hex_char(n) when n < 10, do: ?0 + n
  defp hex_char(n) when n < 16, do: ?a - 10 + n
end
