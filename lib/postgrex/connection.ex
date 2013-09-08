defmodule Postgrex.Connection do
  use GenServer.Behaviour
  alias Postgrex.Protocol
  use Postgrex.Protocol.Messages

  defrecordp :state, [:opts, :sock, :tail, :state]

  def start_link(opts) do
    :gen_server.start_link(__MODULE__, opts, [])
  end

  def init(opts) do
    :gen_server.cast(self, :startup)
    { :ok, state(state: :startup, opts: opts, tail: "") }
  end

  def handle_cast(:startup, state(opts: opts, state: :startup) = s) do
    sock_opts = [ { :active, :once }, { :packet, :raw }, :binary ]
    host = String.to_char_list!(opts[:host])
    { :ok, sock } = :gen_tcp.connect(host, opts[:port], sock_opts)

    msg = startup(params: [{ "user", opts[:username] }, { "database", opts[:database] }])
    send(msg, sock)
    { :noreply, state(s, sock: sock, state: :auth) }
  end

  def handle_info({ :tcp, _, data }, state(sock: sock, tail: tail) = s) do
    IO.inspect data
    s = handle_data(tail <> data, state(s, tail: ""))
    :inet.setopts(sock, active: :once)
    { :noreply, s }
  end

  def handle_info({ :tcp_closed, _ }, s) do
    { :stop, :tcp_closed, s }
  end

  def handle_info({ :tcp_error, _, reason }, s) do
    { :stop, { :tcp_error, reason }, s }
  end

  defp handle_data(<< type :: size(8), size :: size(32), data :: binary >>, s) do
    size = size - 4

    case data do
      << data :: [binary, unit(8), size(size)], tail :: binary >> ->
        msg = Protocol.decode(type, size, data)
        s = message(msg, s)
        handle_data(tail, s)
      tail ->
        state(s, tail: tail)
    end
  end

  defp handle_data(data, state(tail: tail) = s) do
    state(s, tail: tail <> data)
  end

  defp message(auth(type: :ok), state(state: :auth) = s) do
    state(s, state: :init)
  end

  defp message(auth(type: :cleartext), state(opts: opts, state: :auth) = s) do
    IO.puts "yo"
    msg = password(pass: opts[:password])
    send(msg, s)
    s
  end

  defp message(auth(type: :md5, data: salt), state(opts: opts, state: :auth) = s) do
    IO.puts "heyo"
    digest = :crypto.hash(:md5, [opts[:password], opts[:username]]) |> hexify
    digest = :crypto.hash(:md5, [digest, salt]) |> hexify
    msg = password(pass: ["md5", digest])
    send(msg, s)
    s
  end

  defp send(msg, state(sock: sock)), do: send(msg, sock)

  defp send(msg, sock) do
    binary = Protocol.encode(IO.inspect msg)
    :ok = :gen_tcp.send(sock, binary)
  end

  defp hexify(bin) do
    bc << high :: size(4), low :: size(4) >> inbits bin do
      << hex_char(high), hex_char(low) >>
    end
  end

  defp hex_char(n) when n < 10, do: ?0 + n
  defp hex_char(n) when n < 16, do: ?a - 10 + n
end
