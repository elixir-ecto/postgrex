defmodule Postgrex.Connection do
  use GenServer.Behaviour
  alias Postgrex.Protocol
  use Postgrex.Protocol.Messages

  # possible states: not_started, auth, init, ready

  defrecordp :state, [ :opts, :sock, :tail, :state, :reply_to, :parameters,
                       :backend_key ]

  def start_link() do
    :gen_server.start_link(__MODULE__, [], [])
  end

  def connect(pid, opts) do
    :gen_server.call(pid, { :connect, opts })
  end

  def init([]) do
    { :ok, state(state: :not_started, tail: "", parameters: []) }
  end

  def handle_call({ :connect, opts }, from, state(state: :not_started) = s) do
    sock_opts = [ { :active, :once }, { :packet, :raw }, :binary ]
    host = String.to_char_list!(opts[:host])

    case :gen_tcp.connect(host, opts[:port], sock_opts) do
      { :ok, sock } ->
        msg = startup(params: [user: opts[:username], database: opts[:database]])
        case send(msg, sock) do
          :ok ->
            { :noreply, state(s, opts: opts, sock: sock, reply_to: from, state: :auth) }
          { :error, reason } ->
            reason = { :tcp_send, reason }
            :gen_server.reply(from, { :error, reason })
            { :stop, reason, s }
        end

      { :error, reason } ->
        reason = { :tcp_connect, reason }
        :gen_server.reply(from, { :error, reason })
        { :stop, reason, s }
    end

  end

  def handle_info({ :tcp, _, data }, state(reply_to: from, sock: sock, tail: tail) = s) do
    case handle_data(tail <> data, state(s, tail: "")) do
      { :ok, s } ->
        :inet.setopts(sock, active: :once)
        { :noreply, s }
      { :error, reason } ->
        if from, do: :gen_server.reply(from, { :error, reason })
        { :stop, reason, s }
    end
  end

  def handle_info({ :tcp_closed, _ }, state(reply_to: from) = s) do
    if from, do: :gen_server.reply(from, { :error, :tcp_closed })
    { :stop, :tcp_closed, s }
  end

  def handle_info({ :tcp_error, _, reason }, state(reply_to: from) = s) do
    reason = { :tcp_error, reason }
    if from, do: :gen_server.reply(from, { :error, reason })
    { :stop, reason, s }
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

  defp message(auth(type: :ok), state(state: :auth) = s) do
    { :ok, state(s, state: :init) }
  end

  defp message(auth(type: :cleartext), state(opts: opts, state: :auth) = s) do
    msg = password(pass: opts[:password])
    send_to_result(msg, s)
  end

  defp message(auth(type: :md5, data: salt), state(opts: opts, state: :auth) = s) do
    digest = :crypto.hash(:md5, [opts[:password], opts[:username]]) |> hexify
    digest = :crypto.hash(:md5, [digest, salt]) |> hexify
    msg = password(pass: ["md5", digest])
    send_to_result(msg, s)
  end

  ### init state ###

  defp message(backend_key(pid: pid, key: key), state(state: :init) = s) do
    { :ok, state(s, backend_key: { pid, key }) }
  end

  defp message(ready(), state(reply_to: from, state: :init) = s) do
    :gen_server.reply(from, :ok)
    { :ok, state(s, state: :ready) }
  end

  ### asynchronous messages ###

  defp message(parameter(name: name, value: value), state(parameters: params) = s) do
    params = Dict.put(params, name, value)
    { :ok, state(s, parameters: params) }
  end

  defp message(error(fields: fields), _s) do
    error = { :pgsql_error, fields }
    { :error, error }
  end

  defp send(msg, state(sock: sock)), do: send(msg, sock)

  defp send(msg, sock) do
    binary = Protocol.encode(msg)
    :gen_tcp.send(sock, binary)
  end

  defp send_to_result(msg, state(sock: sock) = s) do
    case send(msg, sock) do
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
