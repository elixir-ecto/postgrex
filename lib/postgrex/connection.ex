defmodule Postgrex.Connection do
  use GenServer.Behaviour
  alias Postgrex.Protocol
  use Postgrex.Protocol.Messages

  # possible states: not_started, auth, init, ready, parsing, describing

  defrecordp :state, [ :opts, :sock, :tail, :state, :reply_to, :parameters,
                       :backend_key ]

  def start_link() do
    :gen_server.start_link(__MODULE__, [], [])
  end

  def connect(pid, opts) do
    :gen_server.call(pid, { :connect, opts })
  end

  def parse(pid, statement) do
    :gen_server.call(pid, { :parse, statement })
  end

  def init([]) do
    { :ok, state(state: [], tail: "", parameters: []) }
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
            { :noreply, next_state(:auth, state(s, opts: opts, sock: sock, reply_to: from)) }
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

  def handle_call({ :parse, statement }, from, state(state: []) = s) do
    msgs = [
      msg_parse(name: "", query: statement, type_oids: []),
      msg_describe(type: :statement, name: ""),
      msg_flush() ]

    case send_to_result(msgs, s) do
      { :ok, _ } ->
        { :noreply, next_state(:parsing, state(s, reply_to: from)) }
      err ->
        :gen_server.reply(from, err)
        { :stop, :normal, s }
    end
  end

  def handle_info({ :tcp, _, data }, state(sock: sock, tail: tail) = s) do
    case handle_data(tail <> data, state(s, tail: "")) do
      { :ok, s } ->
        :inet.setopts(sock, active: :once)
        { :noreply, s }
      { :error, reason } ->
        if from do
          s = reply(s, { :error, reason })
          { :stop, :normal, s }
        else
          { :stop, reason, s }
        end
    end
  end

  def handle_info({ :tcp_closed, _ }, s) do
    reason = :tcp_closed
    if from do
      s = reply(s, { :error, reason })
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

  defp handle_data(<< type :: size(8), size :: size(32), data :: binary >>, s) do
    size = size - 4

    case data do
      << data :: [binary, unit(8), size(size)], tail :: binary >> ->
        msg = Protocol.decode(type, size, data)
        case message(msg, s) do
          { :ok, s } -> handle_data(tail, s)
          { :error, _ } = err -> err
        end
      tail ->
        state(s, tail: tail)
    end
  end

  defp handle_data(data, state(tail: tail) = s) do
    { :ok, state(s, tail: tail <> data) }
  end

  ### auth state ###

  defp message(msg_auth(type: :ok), state(state: [:auth|_]) = s) do
    { :ok, next_state(:init, s) }
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

  ### init state ###

  defp message(msg_backend_key(pid: pid, key: key), state(state: [:init|_]) = s) do
    { :ok, state(s, backend_key: { pid, key }) }
  end

  defp message(msg_ready(), state(state: [:init|_]) = s) do
    { :ok, next_state(s) }
  end

  ### parsing state ###

  defp message(msg_parse_complete(), state(state: [:parsing|_]) = s) do
    { :ok, next_state(:describing, s) }
  end

  ### describing state ###

  defp message(msg_parameter_desc(), state(state: [:describing|_]) = s) do
    { :ok, s }
  end

  defp message(msg_row_desc(), state(state: [:describing|_]) = s) do
    { :ok, next_state(s) }
  end

  ### asynchronous messages ###

  defp message(msg_parameter(name: name, value: value), state(parameters: params) = s) do
    params = Dict.put(params, name, value)
    { :ok, state(s, parameters: params) }
  end

  defp message(msg_error(fields: fields), _s) do
    error = { :pgsql_error, fields }
    { :error, error }
  end

  defp message(msg_notice(), s) do
    # TODO: subscribers
    { :ok, s }
  end

  ### helpers ###

  defp next_state(state(state: [_]) = s) do
    reply(:ok, s) |> state(state: [])
  end

  defp next_state(state(state: [_|next]) = s), do: state(s, state: next)

  defp next_state(new, state(state: [_|next]) = s), do: state(s, state: [new|next])

  defp next_state(new, state(state: []) = s), do: state(s, state: [new])

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
      { :error, reason } -> { :error, { :tcp_send, reason } }
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
