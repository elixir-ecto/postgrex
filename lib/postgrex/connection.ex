defmodule Postgrex.Connection do
  @moduledoc """
  Main API for Postgrex. This module handles the connection to postgres.
  """

  use GenServer
  alias Postgrex.Protocol
  alias Postgrex.Messages
  import Postgrex.BinaryUtils
  import Postgrex.Utils

  @timeout 5000
  @default_extensions [{Postgrex.Extensions.Binary, nil}, {Postgrex.Extensions.Text, nil}]

  ### PUBLIC API ###

  @doc """
  Start the connection process and connect to postgres.

  ## Options

    * `:hostname` - Server hostname (default: PGHOST env variable, then localhost);
    * `:port` - Server port (default: 5432);
    * `:database` - Database (required);
    * `:username` - Username (default: PGUSER env variable, then USER env var);
    * `:password` - User password (default PGPASSWORD);
    * `:parameters` - Keyword list of connection parameters;
    * `:timeout` - Connect timeout in milliseconds (default: `#{@timeout}`);
    * `:ssl` - Set to `true` if ssl should be used (default: `false`);
    * `:ssl_opts` - A list of ssl options, see ssl docs;
    * `:socket_options` - Options to be given to the underlying socket;
    * `:sync_connect` - Block in `start_link/1` until connection is set up (default: `false`)
    * `:extensions` - A list of `{module, opts}` pairs where `module` is
      implementing the `Postgrex.Extension` behaviour and `opts` are the
      extension options;
  """
  @spec start_link(Keyword.t) :: {:ok, pid} | {:error, Postgrex.Error.t | term}
  def start_link(opts) do
    opts = opts
      |> Keyword.put_new(:username, System.get_env("PGUSER") || System.get_env("USER"))
      |> Keyword.put_new(:password, System.get_env("PGPASSWORD"))
      |> Keyword.put_new(:hostname, System.get_env("PGHOST") || "localhost")
      |> Enum.reject(fn {_k,v} -> is_nil(v) end)

    timeout = opts[:timeout] || @timeout

    case GenServer.start_link(__MODULE__, opts) do
      {:ok, pid} ->
        if opts[:sync_connect] do
          case GenServer.call(pid, :connect, timeout) do
            :ok                 -> {:ok, pid}
            {:error, _} = error -> error
          end
        else
          GenServer.cast(pid, :connect)
          {:ok, pid}
        end
      {:error, _} = error ->
        error
    end
  end

  @doc """
  Stop the process and disconnect.

  ## Options

    * `:timeout` - Call timeout (default: `#{@timeout}`)
  """
  @spec stop(pid, Keyword.t) :: :ok
  def stop(pid, opts \\ []) do
    GenServer.call(pid, :stop, opts[:timeout] || @timeout)
  end

  @doc """
  Runs an (extended) query and returns the result as `{:ok, %Postgrex.Result{}}`
  or `{:error, %Postgrex.Error{}}` if there was an error. Parameters can be
  set in the query as `$1` embedded in the query string. Parameters are given as
  a list of elixir values. See the README for information on how Postgrex
  encodes and decodes Elixir values by default. See `Postgrex.Result` for the
  result data.

  ## Options

    * `:timeout` - Call timeout (default: `#{@timeout}`)

  ## Examples

      Postgrex.Connection.query(pid, "CREATE TABLE posts (id serial, title text)", [])

      Postgrex.Connection.query(pid, "INSERT INTO posts (title) VALUES ('my title')", [])

      Postgrex.Connection.query(pid, "SELECT title FROM posts", [])

      Postgrex.Connection.query(pid, "SELECT id FROM posts WHERE title like $1", ["%my%"])

  """
  @spec query(pid, iodata, list, Keyword.t) :: {:ok, Postgrex.Result.t} | {:error, Postgrex.Error.t}
  def query(pid, statement, params, opts \\ []) do
    message = {:query, statement, params}
    timeout = opts[:timeout] || @timeout
    case GenServer.call(pid, message, timeout) do
      %Postgrex.Result{} = res ->
        {:ok, res}
      %Postgrex.Error{} = err ->
        {:error, err}
      {:error, kind, reason, stack} ->
        :erlang.raise(kind, reason, stack)
    end
  end

  @doc """
  Runs an (extended) query and returns the result or raises `Postgrex.Error` if
  there was an error. See `query/3`.
  """
  @spec query!(pid, iodata, list, Keyword.t) :: Postgrex.Result.t
  def query!(pid, statement, params, opts \\ []) do
    case query(pid, statement, params, opts) do
      {:ok, res}    -> res
      {:error, err} -> raise err
    end
  end

  @doc """
  Listens to an asynchronous notification channel using the `LISTEN` command.
  A message `{:notification, connection_pid, ref, channel, payload}` will be
  sent to the calling process when a notification is received.

  ## Options

    * `:timeout` - Call timeout (default: `#{@timeout}`)
  """
  @spec listen(pid, String.t, Keyword.t) :: {:ok, reference} | {:error, Postgrex.Error.t}
  def listen(pid, channel, opts \\ []) do
    message = {:listen, channel, self()}
    timeout = opts[:timeout] || @timeout
    case GenServer.call(pid, message, timeout) do
      ref when is_reference(ref)  -> {:ok, ref}
      %Postgrex.Error{} = err     -> {:error, err}
    end
  end

  @doc """
  Listens to an asynchronous notification channel `channel`. See `listen/2`.
  """
  @spec listen!(pid, String.t, Keyword.t) :: reference
  def listen!(pid, channel, opts \\ []) do
    case listen(pid, channel, opts) do
      {:ok, ref}    -> ref
      {:error, err} -> raise err
    end
  end

  @doc """
  Stops listening on the given channel by passing the reference returned from
  `listen/2`.

  ## Options

    * `:timeout` - Call timeout (default: `#{@timeout}`)
  """
  @spec unlisten(pid, reference, Keyword.t) :: :ok | {:error, Postgrex.Error.t}
  def unlisten(pid, ref, opts \\ []) do
    message = {:unlisten, ref}
    timeout = opts[:timeout] || @timeout
    case GenServer.call(pid, message, timeout) do
      :ok -> :ok
      %ArgumentError{} = err -> raise err
      %Postgrex.Error{} = err -> {:error, err}
    end
  end

  @doc """
  Stops listening on the given channel by passing the reference returned from
  `listen/2`.
  """
  @spec unlisten!(pid, reference, Keyword.t) :: :ok
  def unlisten!(pid, ref, opts \\ []) do
    case unlisten(pid, ref, opts) do
      :ok           -> :ok
      {:error, err} -> raise err
    end
  end

  @doc """
  Returns a cached map of connection parameters.

  ## Options

    * `:timeout` - Call timeout (default: `#{@timeout}`)
  """
  @spec parameters(pid, Keyword.t) :: map
  def parameters(pid, opts \\ []) do
    GenServer.call(pid, :parameters, opts[:timeout] || @timeout)
  end

  ### GEN_SERVER CALLBACKS ###

  @doc false
  def init(opts) do
    {:ok, %{sock: nil, tail: "", state: :ready, parameters: %{}, backend_key: nil,
            rows: [], statement: nil, portal: nil, bootstrap: false, types: nil,
            queue: :queue.new, opts: opts, extensions: nil, listeners: HashDict.new,
            listener_channels: HashDict.new, types_key: nil}}
  end

  @doc false
  def format_status(opt, [_pdict, s]) do
    s = %{s | types: :types_removed}
    if opt == :normal do
      [data: [{'State', s}]]
    else
      s
    end
  end

  @doc false
  def handle_call(:connect, from, s) do
    connect(from, s)
  end

  def handle_call(:stop, from, s) do
    reply(:ok, from)
    {:stop, :normal, s}
  end

  def handle_call(:parameters, _from, %{parameters: params} = s) do
    {:reply, params, s}
  end

  def handle_call(command, from, %{state: state} = s) do
    command = new_command(command, from)
    s = update_in(s.queue, &:queue.in(command, &1))

    if state == :ready do
      case next(s) do
        {:ok, s} ->
          {:noreply, s}
        {:error, error, s} ->
          error(error, s)
      end
    else
      {:noreply, s}
    end
  end

  @doc false
  def handle_cast(:connect, s) do
    connect(nil, s)
  end

  def handle_info({:DOWN, ref, :process, _, _}, s) do
    s =
      case HashDict.fetch(s.listeners, ref) do
        {:ok, {channel, _pid}} ->
          s = update_in(s.listener_channels[channel], &HashSet.delete(&1, ref))
          s = update_in(s.listeners, &HashDict.delete(&1, ref))

          if HashSet.size(s.listener_channels[channel]) == 0 do
            s = update_in(s.listener_channels, &HashDict.delete(&1, channel))
            s = add_dummy_command(s)
            {:ok, s} = new_query("UNLISTEN #{channel}", [], s)
            s
          else
            s
          end
        :error ->
          s
      end

    {:noreply, s}
  end

  def handle_info({:tcp, _, data}, %{sock: {:gen_tcp, sock}, opts: opts, state: :ssl} = s) do
    case data do
      <<?S>> ->
        case :ssl.connect(sock, opts[:ssl_opts] || []) do
          {:ok, ssl_sock} ->
            :ssl.setopts(ssl_sock, active: :once)
            Protocol.startup(%{s | sock: {:ssl, ssl_sock}})
          {:error, reason} ->
            error(%Postgrex.Error{message: "ssl negotiation failed: #{reason}"}, s)
        end

      <<?N>> ->
        error(%Postgrex.Error{message: "ssl not available"}, s)
    end
  end

  def handle_info({tag, _, data}, %{sock: {mod, sock}, tail: tail} = s)
      when tag in [:tcp, :ssl] do
    case new_data(tail <> data, %{s | tail: ""}) do
      {:ok, s} ->
        case mod do
          :gen_tcp -> :inet.setopts(sock, active: :once)
          :ssl     -> :ssl.setopts(sock, active: :once)
        end
        {:noreply, s}
      {:error, error, s} ->
        error(error, s)
    end
  end

  def handle_info({tag, _}, s) when tag in [:tcp_closed, :ssl_closed] do
    error(%Postgrex.Error{message: "tcp closed"}, s)
  end

  def handle_info({tag, _, reason}, s) when tag in [:tcp_error, :ssl_error] do
    error(%Postgrex.Error{message: "tcp error: #{reason}"}, s)
  end

  @doc false
  def new_query(statement, params, %{queue: queue} = s) do
    {{:value, command}, queue} = :queue.out(queue)
    new_command = {:query, statement, params}
    command = %{command | command: new_command}

    queue = :queue.in_r(command, queue)
    command(new_command, %{s | queue: queue})
  end

  @doc false
  def next(%{queue: queue} = s) do
    case :queue.out(queue) do
      {{:value, %{command: command}}, _queue} ->
        command(command, s)
      {:empty, _queue} ->
        {:ok, s}
    end
  end

  ### PRIVATE FUNCTIONS ###

  defp connect(from, %{queue: queue, opts: opts} = s) do
    host       = Keyword.fetch!(opts, :hostname) |> to_char_list
    port       = opts[:port] || 5432
    timeout    = opts[:timeout] || @timeout
    sock_opts  = [{:packet, :raw}, :binary] ++ (opts[:socket_options] || [])
    custom     = opts[:extensions] || []
    extensions = custom ++ @default_extensions

    command = new_command({:connect, opts}, from)
    queue = :queue.in(command, queue)
    types_key = {host, port, Keyword.fetch!(opts, :database), custom}
    s = %{s | queue: queue, extensions: extensions, types_key: types_key}

    case :gen_tcp.connect(host, port, sock_opts, timeout) do
      {:ok, sock} ->
        s = put_in(s.sock, {:gen_tcp, sock})
        # A suitable :buffer is only set if :recbuf is included in
        # :socket_options.
        {:ok, [sndbuf: sndbuf, recbuf: recbuf, buffer: buffer]} =
          :inet.getopts(sock, [:sndbuf, :recbuf, :buffer])
        buffer = buffer
          |> max(sndbuf)
          |> max(recbuf)
        :ok = :inet.setopts(sock, [buffer: buffer, active: :once])

        if opts[:ssl] do
          Protocol.startup_ssl(s)
        else
          Protocol.startup(s)
        end

      {:error, reason} ->
        error(%Postgrex.Error{message: "tcp connect: #{reason}"}, s)
    end
  end

  defp command({:query, statement, _params}, s) do
    Protocol.send_query(statement, s)
  end

  defp command({:listen, channel, pid}, s) do
    ref = Process.monitor(pid)
    s = update_in(s.listeners, &HashDict.put(&1, ref, {channel, pid}))
    s = update_in(s.listener_channels[channel], fn set ->
      (set || HashSet.new) |> HashSet.put(ref)
    end)

    if HashSet.size(s.listener_channels[channel]) == 1 do
      s = add_reply_to_queue(ref, s)
      new_query("LISTEN #{channel}", [], s)
    else
      reply(ref, s)
      {:ok, s}
    end
  end

  defp command({:unlisten, ref}, s) do
    case HashDict.fetch(s.listeners, ref) do
      {:ok, {channel, _pid}} ->
        s = update_in(s.listener_channels[channel], &HashSet.delete(&1, ref))
        s = update_in(s.listeners, &HashDict.delete(&1, ref))

        if HashSet.size(s.listener_channels[channel]) == 0 do
          s = update_in(s.listener_channels, &HashDict.delete(&1, channel))
          s = add_reply_to_queue(:ok, s)
          new_query("UNLISTEN #{channel}", [], s)
        else
          reply(:ok, s)
          {:ok, s}
        end

      :error ->
        reply(%ArgumentError{}, s)
        {:ok, s}
    end
  end

  defp new_data(<<type :: int8, size :: int32, data :: binary>> = tail, %{state: state} = s) do
    size = size - 4

    case data do
      <<data :: binary(size), tail :: binary>> ->
        msg = Messages.parse(data, type, size)
        case Protocol.message(state, msg, s) do
          {:ok, s} -> new_data(tail, s)
          {:error, _, _} = err -> err
        end
      _ ->
        {:ok, %{s | tail: tail}}
    end
  end

  defp new_data(data, %{tail: tail} = s) do
    # NOTE: This can be optimized by building an iolist and only concat to
    #       a binary when we know we have enough data according to the
    #       message header.
    {:ok, %{s | tail: tail <> data}}
  end

  defp add_dummy_command(s) do
    command = new_command(:DUMMY, nil)
    %{s | queue: :queue.in_r(command, s.queue)}
  end

  defp add_reply_to_queue(reply, %{queue: queue} = s) do
    {{:value, command}, queue} = :queue.out(queue)
    command = %{command | reply: {:reply, reply}}
    %{s | queue: :queue.in_r(command, queue)}
  end

  defp new_command(command, from) do
    %{command: command, from: from, reply: :no_reply}
  end
end
