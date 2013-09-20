defmodule Postgrex.Connection do
  use GenServer.Behaviour
  use Postgrex.Protocol.Messages
  alias Postgrex.Protocol
  alias Postgrex.Types
  import Postgrex.BinaryUtils

  # possible states: auth, init, parsing, describing, binding, executing, "ready"

  defrecordp :state, [ :opts, :sock, :tail, :state, :reply_to, :parameters,
                       :backend_key, :rows, :statement, :bootstrap, :types ]

  defrecordp :statement, [:result_oids]

  def start_link() do
    :gen_server.start_link(__MODULE__, [], [])
  end

  def stop(pid) do
    :gen_server.call(pid, :stop)
  end

  def connect(pid, opts) do
    :gen_server.call(pid, { :connect, opts })
  end

  def query(pid, statement) do
    :gen_server.call(pid, { :query, statement })
  end

  def init([]) do
    { :ok, state(state: [], tail: "", parameters: [], rows: [], bootstrap: false) }
  end

  def handle_call(:stop, from, state(state: []) = s) do
    { :stop, :normal, state(s, reply_to: from) }
  end

  def handle_call({ :connect, opts }, from, state(state: []) = s) do
    sock_opts = [ { :active, :once }, { :packet, :raw }, :binary ]
    address = opts[:address]
    address = if is_binary(address), do: String.to_char_list!(address)

    case :gen_tcp.connect(address, opts[:port], sock_opts) do
      { :ok, sock } ->
        msg = msg_startup(params: [user: opts[:username], database: opts[:database]])
        case send(msg, sock) do
          :ok ->
            states = [:auth, :init]
            { :noreply, state(s, opts: opts, sock: sock, reply_to: from, state: states) }
          { :error, reason } ->
            reason = { :tcp_send, reason }
            :gen_server.reply(from, { :error, reason })
            { :stop, :normal, s }
        end

      { :error, reason } ->
        reason = { :tcp_connect, reason }
        :gen_server.reply(from, { :error, reason })
        { :stop, :normal, s }
    end
  end

  def handle_call({ :query, statement }, from, state(state: []) = s) do
    case send_query(statement, s) do
      { :ok, s } ->
        { :noreply, state(s, reply_to: from) }
      { :error, reason, s } ->
        :gen_server.reply(from, { :error, reason })
        { :stop, :normal, s }
    end
  end

  def handle_info({ :tcp, _, data }, state(reply_to: from, sock: sock, tail: tail) = s) do
    case handle_data(tail <> data, state(s, tail: "")) do
      { :ok, s } ->
        :inet.setopts(sock, active: :once)
        { :noreply, s }
      { :error, reason, s } ->
        if from do
          :gen_server.reply(from, { :error, reason })
          { :stop, :normal, s }
        else
          { :stop, reason, s }
        end
    end
  end

  def handle_info({ :tcp_closed, _ }, state(reply_to: from) = s) do
    reason = :tcp_closed
    if from do
      :gen_server.reply(from, { :error, reason })
      { :stop, :normal, s }
    else
      { :stop, reason, s }
    end
  end

  def handle_info({ :tcp_error, _, reason }, state(reply_to: from) = s) do
    reason = { :tcp_error, reason }
    if from do
      :gen_server.reply(from, { :error, reason })
      { :stop, :normal, s }
    else
      { :stop, reason, s }
    end
  end

  def terminate(_reason, state(sock: sock) = s) do
    if sock do
      send(msg_terminate(), sock)
      :gen_tcp.close(sock)
    end
    reply(:ok, s)
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

  defp message(msg_auth(type: :ok), state(state: [:auth|_]) = s) do
    { :ok, next_state(s) }
  end

  defp message(msg_auth(type: :cleartext), state(opts: opts, state: [:auth|_]) = s) do
    msg = msg_password(pass: opts[:password])
    send_to_result(msg, s)
  end

  defp message(msg_auth(type: :md5, data: salt), state(opts: opts, state: [:auth|_]) = s) do
    digest = :crypto.hash(:md5, [opts[:password], opts[:username]]) |> hexify
    digest = :crypto.hash(:md5, [digest, salt]) |> hexify
    msg = msg_password(pass: ["md5", digest])
    send_to_result(msg, s)
  end

  defp message(msg_error(fields: fields), state(state: [:auth|_]) = s) do
    error = { :pgsql_error, fields }
    { :error, error, s }
  end

  ### init state ###

  defp message(msg_backend_key(pid: pid, key: key), state(state: [:init|_]) = s) do
    { :ok, state(s, backend_key: { pid, key }) }
  end

  defp message(msg_ready(), state(state: [:init|_]) = s) do
    s = state(s, bootstrap: true) |> next_state
    send_query(Types.bootstrap_query, s)
  end

  defp message(msg_error(fields: fields), state(state: [:init|_]) = s) do
    error = { :pgsql_error, fields }
    { :error, error, s }
  end

  ### parsing state ###

  defp message(msg_parse_complete(), state(state: [:parsing|_]) = s) do
    { :ok, next_state(s) }
  end

  ### describing state ###

  defp message(msg_parameter_desc(), state(state: [:describing|_]) = s) do
    { :ok, s }
  end

  defp message(msg_row_desc(fields: fields), state(state: [:describing|_]) = s) do
    oids = Enum.map(fields, fn row_field(type_oid: oid) -> oid end)
    stat = statement(result_oids: oids)
    { :ok, state(s, statement: stat) |> next_state }
  end

  defp message(msg_no_data(), state(state: [:describing|_]) = s) do
    { :ok, next_state(s) }
  end

  ### binding state ###

  defp message(msg_bind_complete(), state(state: [:binding|_]) = s) do
    { :ok, next_state(s) }
  end

  ### executing state ###

  # defp message(msg_portal_suspend(), state(state: [:executing|_]) = s)

  defp message(msg_data_row(values: values), state(rows: rows, state: [:executing|_]) = s) do
    { :ok, state(s, rows: [values|rows]) }
  end

  defp message(msg_command_complete(), state(bootstrap: true, rows: rows, state: [:executing|_]) = s) do
    types = Types.build_types(rows)
    s = reply(:ok, s)
    { :ok, state(s, rows: [], statement: nil, bootstrap: false, types: types) }
  end

  defp message(msg_command_complete(), state(statement: stat, types: types,
                                             rows: rows, state: [:executing|_]) = s) do
    senders = statement(stat, :result_oids)
      |> Enum.map(&Dict.get(types, &1))
      |> list_to_tuple

    result = Enum.reduce(rows, [], fn values, acc ->
      { _, row } = Enum.reduce(values, { 0, [] }, fn value, { count, list } ->
        decoded = Types.decode(elem(senders, count), value)
        { count+1, [decoded|list] }
      end)
      row = Enum.reverse(row) |> list_to_tuple
      [ row | acc ]
    end)

    s = reply({ :ok, result }, s)
    { :ok, state(s, rows: [], statement: nil) }
  end

  defp message(msg_empty_query(), state(state: [:executing|_]) = s) do
    s = reply({ :ok, []}, s)
    { :ok, s }
  end

  ### asynchronous messages ###

  defp message(msg_ready(), s) do
    { :ok, state(s, state: []) }
  end

  defp message(msg_parameter(name: name, value: value), state(parameters: params) = s) do
    params = Dict.put(params, name, value)
    { :ok, state(s, parameters: params) }
  end

  defp message(msg_error(fields: fields), s) do
    error = { :pgsql_error, fields }
    s = reply({ :error, error }, s)
    # TODO: subscribers
    { :ok, s }
  end

  defp message(msg_notice(), s) do
    # TODO: subscribers
    { :ok, s }
  end

  ### helpers ###

  defp send_query(statement, state(bootstrap: bootstrap) = s) do
    rfs = if bootstrap, do: [], else: [:binary]

    msgs = [
      msg_parse(name: "", query: statement, type_oids: []),
      msg_describe(type: :statement, name: ""),
      msg_bind(name_port: "", name_stat: "", param_formats: [], params: [], result_formats: rfs),
      msg_execute(name_port: "", max_rows: 0),
      msg_sync() ]

    case send_to_result(msgs, s) do
      { :ok, s } ->
        { :ok, state(s, state: [:parsing, :describing, :binding, :executing]) }
      err ->
        err
    end
  end

  defp next_state(state(state: [_|next]) = s), do: state(s, state: next)

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
      :ok -> { :ok, s }
      { :error, reason } -> { :error, { :tcp_send, reason }, s }
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
