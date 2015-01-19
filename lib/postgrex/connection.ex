defmodule Postgrex.Connection do
  @moduledoc """
  Main API for Postgrex. This module handles the connection to postgres.
  """

  use GenServer
  alias Postgrex.Protocol
  alias Postgrex.Messages
  import Postgrex.BinaryUtils
  import Postgrex.Utils

  @timeout :infinity

  ### PUBLIC API ###

  @doc """
  Start the connection process and connect to postgres.

  ## Options

    * `:hostname` - Server hostname (default: PGHOST env variable, then localhost);
    * `:port` - Server port (default: 5432);
    * `:database` - Database (required);
    * `:username` - Username (default: PGUSER env variable, then USER env var);
    * `:password` - User password (default PGPASSWORD);
    * `:encoder` - Custom encoder function;
    * `:decoder` - Custom decoder function;
    * `:formatter` - Function deciding the format for a type;
    * `:parameters` - Keyword list of connection parameters;
    * `:timeout` - Connect timeout in milliseconds (default: `#{@timeout}`);
    * `:ssl` - Set to `true` if ssl should be used (default: `false`);
    * `:ssl_opts` - A list of ssl options, see ssl docs;

  ## Function signatures

      @spec encoder(info :: TypeInfo.t, default :: fun, param :: term) ::
            binary
      @spec decoder(info :: TypeInfo.t, default :: fun, bin :: binary) ::
            term
      @spec formatter(info :: TypeInfo.t) ::
            :binary | :text | nil
  """
  @spec start_link(Keyword.t) :: {:ok, pid} | {:error, Postgrex.Error.t | term}
  def start_link(opts) do
    opts = opts
      |> Keyword.put_new(:username, System.get_env("PGUSER") || System.get_env("USER"))
      |> Keyword.put_new(:password, System.get_env("PGPASSWORD"))
      |> Keyword.put_new(:hostname, System.get_env("PGHOST") || "localhost")
      |> Enum.reject(fn {_k,v} -> is_nil(v) end)
    case GenServer.start_link(__MODULE__, []) do
      {:ok, pid} ->
        timeout = opts[:timeout] || @timeout
        case GenServer.call(pid, {:connect, opts}, timeout) do
          :ok -> {:ok, pid}
          err -> {:error, err}
        end
      err -> err
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
  encodes and decodes elixir values by default. See `Postgrex.Result` for the
  result data.

  A *type hinted* query is run if both the options `:param_types` and
  `:result_types` are given. One client-server round trip can be saved by
  providing the types to Postgrex because the server doesn't have to be queried
  for the types of the parameters and the result.

  ## Options

    * `:timeout` - Call timeout (default: `#{@timeout}`)
    * `:param_types` - A list of type names for the parameters
    * `:result_types` - A list of type names for the result rows

  ## Examples

      Postgrex.Connection.query(pid, "CREATE TABLE posts (id serial, title text)", [])

      Postgrex.Connection.query(pid, "INSERT INTO posts (title) VALUES ('my title')", [])

      Postgrex.Connection.query(pid, "SELECT title FROM posts", [])

      Postgrex.Connection.query(pid, "SELECT id FROM posts WHERE title like $1", ["%my%"])

      Postgrex.Connection.query(pid, "SELECT $1 || $2", ["4", "2"],
                                param_types: ["text", "text"], result_types: ["text"])

  """
  @spec query(pid, iodata, list, Keyword.t) :: {:ok, Postgrex.Result.t} | {:error, Postgrex.Error.t}
  def query(pid, statement, params, opts \\ []) do
    message = {:query, statement, params, opts}
    timeout = opts[:timeout] || @timeout
    case GenServer.call(pid, message, timeout) do
      %Postgrex.Result{} = res -> {:ok, res}
      %Postgrex.Error{} = err  -> {:error, err}
    end
  end

  @doc """
  Runs an (extended) query and returns the result or raises `Postgrex.Error` if
  there was an error. See `query/3`.
  """
  @spec query!(pid, iodata, list, Keyword.t) :: Postgrex.Result.t
  def query!(pid, statement, params, opts \\ []) do
    message = {:query, statement, params, opts}
    timeout = opts[:timeout] || @timeout
    case GenServer.call(pid, message, timeout) do
      %Postgrex.Result{} = res -> res
      %Postgrex.Error{} = err  -> raise err
    end
  end

  @doc """
  Listens to an asynchronous notification channel using the `LISTEN` command.
  A message `{:notification, connection_pid, payload}` will be sent to the
  calling process when a notification is received.

  ## Options

    * `:timeout` - Call timeout (default: `#{@timeout}`)
  """
  @spec listen(pid, String.t, Keyword.t) :: :ok | {:error, Postgrex.Error.t}
  def listen(pid, channel, opts \\ []) do
    message = {:listen, channel, self(), opts}
    timeout = opts[:timeout] || @timeout
    case GenServer.call(pid, message, timeout) do
      %Postgrex.Result{}      -> :ok
      %Postgrex.Error{} = err -> {:error, err}
    end
  end

  @doc """
  Listens to an asynchronous notification channel `channel`. See `listen/2`.
  """
  @spec listen!(pid, String.t, Keyword.t) :: :ok
  def listen!(pid, channel, opts \\ []) do
    message = {:listen, channel, self(), opts}
    timeout = opts[:timeout] || @timeout
    case GenServer.call(pid, message, timeout) do
      %Postgrex.Result{}      -> :ok
      %Postgrex.Error{} = err -> raise err
    end
  end

  @doc """
  Unlistens a notification channel `channel` for the calling process.

  ## Options

    * `:timeout` - Call timeout (default: `#{@timeout}`)
  """
  @spec unlisten(pid, String.t, Keyword.t) :: :ok | {:error, Postgrex.Error.t}
  def unlisten(pid, channel, opts \\ []) do
    message = {:unlisten, channel, self(), opts}
    timeout = opts[:timeout] || @timeout
    case GenServer.call(pid, message, timeout) do
      %Postgrex.Result{}      -> :ok
      %Postgrex.Error{} = err -> {:error, err}
    end
  end

  @doc """
  Unlistens a previously-listened notification channel `channel`. See
  `unlisten/2`.
  """
  @spec unlisten!(pid, String.t, Keyword.t) :: :ok
  def unlisten!(pid, channel, opts \\ []) do
    message = {:unlisten, channel, self(), opts}
    timeout = opts[:timeout] || @timeout
    case GenServer.call(pid, message, timeout) do
      %Postgrex.Result{}      -> :ok
      %Postgrex.Error{} = err -> raise err
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
  def init([]) do
    {:ok, %{sock: nil, tail: "", state: :ready, parameters: %{}, backend_key: nil,
            rows: [], statement: nil, portal: nil, bootstrap: false, types: nil,
            queue: :queue.new, opts: nil, listeners: HashDict.new,
            listener_monitors: HashDict.new, listener_pids: HashDict.new}}
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
  def handle_call(:stop, from, s) do
    reply(:ok, from)
    {:stop, :normal, s}
  end

  def handle_call({:connect, opts}, from, %{queue: queue} = s) do
    host      = Keyword.fetch!(opts, :hostname)
    host      = if is_binary(host), do: String.to_char_list(host), else: host
    port      = opts[:port] || 5432
    timeout   = opts[:timeout] || @timeout
    sock_opts = [{:active, :once}, {:packet, :raw}, :binary]

    queue = :queue.in({{:connect, opts}, from}, queue)
    s = %{s | opts: opts, queue: queue}

    case :gen_tcp.connect(host, port, sock_opts, timeout) do
      {:ok, sock} ->
        s = put_in s.sock, {:gen_tcp, sock}

        if opts[:ssl] do
          Protocol.startup_ssl(s)
        else
          Protocol.startup(s)
        end

      {:error, reason} ->
        error(%Postgrex.Error{message: "tcp connect: #{reason}"}, s)
    end
  end

  def handle_call(:parameters, _from, %{parameters: params} = s) do
    {:reply, params, s}
  end

  def handle_call(command, from, %{state: state} = s) do
    s = update_in s.queue, &:queue.in({command, from}, &1)

    if state == :ready do
      case next(s) do
        {:ok, s} ->
          {:noreply, s}
        {:soft_error, error, s} ->
          {:reply, error, s}
        {:error, error, s} ->
          error(error, s)
      end
    else
      {:noreply, s}
    end
  end

  def handle_info({:DOWN, monitor_ref, :process, _, _}, s) do
    state =
      case HashDict.fetch(s.listener_monitors, monitor_ref) do
        {:ok, {pid, channel}} ->
          s = update_in(s.listener_monitors, &HashDict.delete(&1, monitor_ref))
          s = update_in(s.listener_pids, &HashDict.delete(&1, {pid, channel}))
          s = update_in(s.listeners[channel], &HashSet.delete(&1, pid))

          if HashSet.size(s.listeners[channel]) == 0 do
            # There are no more listeners for this channel
            # we can send a UNLISTEN command now.
            s = update_in(s.queue, &:queue.in({:query, {nil, nil}}, &1))
            {:ok, s} = new_query("UNLISTEN #{channel}", [], s)
            update_in(s.listeners, &HashDict.delete(&1, channel))
          else
            s
          end
        :error ->
          s
      end

    {:noreply, state}
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
    command = {:query, statement, params, []}
    {{:value, {_command, from}}, queue} = :queue.out(queue)
    queue = :queue.in_r({command, from}, queue)
    command(command, %{s | queue: queue})
  end

  @doc false
  def next(%{queue: queue} = s) do
    case :queue.out(queue) do
      {{:value, {command, _from}}, _queue} ->
        command(command, s)
      {:empty, _queue} ->
        {:ok, s}
    end
  end

  ### PRIVATE FUNCTIONS ###

  defp command({:query, statement, _params, opts}, s) do
    param_types  = opts[:param_types]
    result_types = opts[:result_types]

    if param_types && result_types do
      Protocol.send_hinted_query(statement, param_types, result_types, s)
    else
      Protocol.send_query(statement, s)
    end
  end

  defp command({:listen, channel, pid, _opts}, s) do
    # Subscribe `pid` to listen to `channel` notifications.

    channel_listeners = case HashDict.fetch(s.listeners, channel) do
      {:ok, channel_listeners} ->
        HashSet.put(channel_listeners, pid)
      :error ->
        HashSet.new |> HashSet.put(pid)
    end

    s = put_in(s.listeners[channel], channel_listeners)

    # Don't monitor the process twice for the same channel.
    unless HashDict.has_key?(s.listener_pids, {pid, channel}) do
      monitor_ref = Process.monitor pid

      s = put_in(s.listener_monitors[monitor_ref], {pid, channel})
      s = put_in(s.listener_pids[{pid, channel}], monitor_ref)

      new_query("LISTEN #{channel}", [], s)
    else
      reply(%Postgrex.Result{command: :listen}, s)
      {:ok, s}
    end
  end

  defp command({:unlisten, channel, pid, _opts}, s) do
    # Unsubscribe `pid` to listen to `channel` notifications.
    if monitor_ref = HashDict.get(s.listener_pids, {pid, channel}) do
      Process.demonitor monitor_ref

      s = update_in(s.listener_pids, &HashDict.delete(&1, {pid, channel}))
      s = update_in(s.listener_monitors, &HashDict.delete(&1, monitor_ref))
      s = update_in(s.listeners[channel], &HashSet.delete(&1, pid))

      if HashSet.size(s.listeners[channel]) == 0 do
        s = update_in(s.listeners, &HashDict.delete(&1, channel))
        new_query("UNLISTEN #{channel}", [], s)
      else
        reply(%Postgrex.Result{command: :unlisten}, s)
        {:ok, s}
      end
    else
      reply(%Postgrex.Result{command: :unlisten}, s)
      {:ok, s}
    end
  end

  defp new_data(<<type :: int8, size :: int32, data :: binary>> = tail, %{state: state} = s) do
    size = size - 4

    case data do
      <<data :: binary(size), tail :: binary>> ->
        msg = Messages.parse(type, size, data)
        case Protocol.message(state, msg, s) do
          {:ok, s} -> new_data(tail, s)
          {:error, _, _} = err -> err
        end
      _ ->
        {:ok, %{s | tail: tail}}
    end
  end

  defp new_data(data, %{tail: tail} = s) do
    {:ok, %{s | tail: tail <> data}}
  end
end
