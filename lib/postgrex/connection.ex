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
    * `:connect_timeout` - Connect timeout in milliseconds (default: 5000);
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
      |> Dict.put_new(:username, System.get_env("PGUSER") || System.get_env("USER"))
      |> Dict.put_new(:password, System.get_env("PGPASSWORD"))
      |> Dict.put_new(:hostname, System.get_env("PGHOST") || "localhost")
      |> Enum.reject(fn {_k,v} -> is_nil(v) end)
    case GenServer.start_link(__MODULE__, []) do
      {:ok, pid} ->
        timeout = opts[:connect_timeout] || @timeout
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
  Listens to an asynchronous notification channel `channel`.
  A message will be sent to the current process when a notification is
  received.

  This sends a LISTEN query to the server automatically.

  ## Options

    * `:timeout` - Call timeout (default: `#{@timeout}`)
  """
  def listen(pid, channel, opts \\ []) do
    message = {:listen, channel, self(), opts}
    timeout = opts[:timeout] || @timeout
    case GenServer.call(pid, message, timeout) do
      %Postgrex.Result{} -> :ok
      %Postgrex.Error{} = err  -> raise err
    end
  end

  @doc """
  Unlistens a previously-listened notification channel `channel`.

  This sends an UNLISTEN query to the server automatically.

  ## Options

    * `:timeout` - Call timeout (default: `#{@timeout}`)
  """
  def unlisten(pid, channel, opts \\ []) do
    message = {:unlisten, channel, self(), opts}
    timeout = opts[:timeout] || @timeout
    case GenServer.call(pid, message, timeout) do
      %Postgrex.Result{} -> :ok
      %Postgrex.Error{} = err  -> raise err
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

  @doc """
  Starts a transaction. Returns `:ok` or `{:error, %Postgrex.Error{}}` if an
  error occurred. Transactions can be nested with the help of savepoints. A
  transaction won't end until a `rollback/1` or `commit/1` have been issued for
  every `begin/1`.

  ## Options

    * `:timeout` - Call timeout (default: `#{@timeout}`)

  ## Examples

      # Transaction begun
      Postgrex.Connection.begin(pid)
      Postgrex.Connection.query(pid, "INSERT INTO comments (text) VALUES ('first')")

      # Nested subtransaction begun
      Postgrex.Connection.begin(pid)
      Postgrex.Connection.query(pid, "INSERT INTO comments (text) VALUES ('second')")

      # Subtransaction rolled back
      Postgrex.Connection.rollback(pid)

      # Only the first comment will be commited because the second was rolled back
      Postgrex.Connection.commit(pid)
  """
  @spec begin(pid, Keyword.t) :: :ok | {:error, Postgrex.Error.t}
  def begin(pid, opts \\ []) do
    timeout = opts[:timeout] || @timeout
    case GenServer.call(pid, {:begin, opts}, timeout) do
      %Postgrex.Result{} -> :ok
      %Postgrex.Error{} = err -> {:error, err}
    end
  end

  @doc """
  Starts a transaction. Returns `:ok` if it was successful or raises
  `Postgrex.Error` if an error occurred. See `begin/1`.
  """
  @spec begin!(pid, Keyword.t) :: :ok
  def begin!(pid, opts \\ []) do
    timeout = opts[:timeout] || @timeout
    case GenServer.call(pid, {:begin, opts}, timeout) do
      %Postgrex.Result{} -> :ok
      %Postgrex.Error{} = err -> raise err
    end
  end

  @doc """
  Rolls back a transaction. Returns `:ok` or `{:error, %Postgrex.Error{}}` if
  an error occurred. See `begin/1` for more information.

  ## Options

    * `:timeout` - Call timeout (default: `#{@timeout}`)
  """
  @spec rollback(pid, Keyword.t) :: :ok | {:error, Postgrex.Error.t}
  def rollback(pid, opts \\ []) do
    timeout = opts[:timeout] || @timeout
    case GenServer.call(pid, {:rollback, opts}, timeout) do
      :ok -> :ok
      %Postgrex.Result{} -> :ok
      %Postgrex.Error{} = err -> {:error, err}
    end
  end

  @doc """
  Rolls back a transaction. Returns `:ok` if it was successful or raises
  `Postgrex.Error` if an error occurred. See `rollback/1`.
  """
  @spec rollback!(pid, Keyword.t) :: :ok
  def rollback!(pid, opts \\ []) do
    timeout = opts[:timeout] || @timeout
    case GenServer.call(pid, {:rollback, opts}, timeout) do
      :ok -> :ok
      %Postgrex.Result{} -> :ok
      %Postgrex.Error{} = err -> raise err
    end
  end

  @doc """
  Commits a transaction. Returns `:ok` or `{:error, %Postgrex.Error{}}` if an
  error occurred. See `begin/1` for more information.

  ## Options

    * `:timeout` - Call timeout (default: `#{@timeout}`)
  """
  @spec commit(pid, Keyword.t) :: :ok | {:error, Postgrex.Error.t}
  def commit(pid, opts \\ []) do
    timeout = opts[:timeout] || @timeout
    case GenServer.call(pid, {:commit, opts}, timeout) do
      :ok -> :ok
      %Postgrex.Result{} -> :ok
      %Postgrex.Error{} = err -> {:error, err}
    end
  end

  @doc """
  Commits a transaction. Returns `:ok` if it was successful or raises
  `Postgrex.Error` if an error occurred. See `commit/1`.
  """
  @spec commit!(pid, Keyword.t) :: :ok
  def commit!(pid, opts \\ []) do
    timeout = opts[:timeout] || @timeout
    case GenServer.call(pid, {:commit, opts}, timeout) do
      :ok -> :ok
      %Postgrex.Result{} -> :ok
      %Postgrex.Error{} = err -> raise err
    end
  end

  @doc """
  Helper for creating reliable transactions. If an error is raised in the given
  function the transaction is rolled back, otherwise it is commited. A
  transaction can be cancelled with `throw :postgrex_rollback`. If there is a
  connection error `Postgrex.Error` will be raised. Do not use this function in
  conjunction with `begin/1`, `commit/1` and `rollback/1`.

  ## Options

    * `:timeout` - Call timeout (default: `#{@timeout}`). Note that it is not
      the maximum timeout of the entire call but rather the timeout of the
      `commit/2` and `rollback/2` calls that this function makes.
  """
  @spec in_transaction(pid, Keyword.t, (() -> term)) :: term
  def in_transaction(pid, opts \\ [], fun) do
    case begin(pid) do
      :ok ->
        try do
          value = fun.()
          case commit(pid, opts) do
            :ok -> value
            err -> raise err
          end
        catch
          :throw, :postgrex_rollback ->
            case rollback(pid, opts) do
              :ok -> nil
              err -> raise err
            end
          type, term ->
            _ = rollback(pid, opts)
            :erlang.raise(type, term, System.stacktrace)
        end
      err -> raise err
    end
  end

  ### GEN_SERVER CALLBACKS ###

  @doc false
  def init([]) do
    {:ok, %{sock: nil, tail: "", state: :ready, parameters: %{}, backend_key: nil,
            rows: [], statement: nil, portal: nil, bootstrap: false, types: nil,
            transactions: 0, queue: :queue.new, opts: nil, listeners: HashDict.new,
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
    host      = opts[:hostname] || System.get_env("PGHOST")
    host      = if is_binary(host), do: String.to_char_list(host), else: host
    port      = opts[:port] || 5432
    timeout   = opts[:connect_timeout] || @timeout
    sock_opts = [{:active, :once}, {:packet, :raw}, :binary]

    case :gen_tcp.connect(host, port, sock_opts, timeout) do
      {:ok, sock} ->
        queue = :queue.in({{:connect, opts}, from, nil}, queue)
        s = %{s | opts: opts, sock: {:gen_tcp, sock}, queue: queue}
        if opts[:ssl] do
          Protocol.startup_ssl(s)
        else
          Protocol.startup(s)
        end

      {:error, reason} ->
        {:stop, :normal, %Postgrex.Error{message: "tcp connect: #{reason}"}, s}
    end
  end

  def handle_call(:parameters, _from, %{parameters: params} = s) do
    {:reply, params, s}
  end

  def handle_call(command, from, %{state: state, queue: queue} = s) do
    # Assume last element in tuple is the options
    timeout = elem(command, tuple_size(command)-1)[:timeout] || @timeout

    unless timeout == :infinity do
      timer_ref = :erlang.start_timer(timeout, self(), :command)
    end

    queue = :queue.in({command, from, timer_ref}, queue)
    s = %{s | queue: queue}

    if state == :ready do
      case next(s) do
        {:ok, s} -> {:noreply, s}
        {:error, error, s} -> error(error, s)
      end
    else
      {:noreply, s}
    end
  end

  def handle_info({:DOWN, monitor_ref, :process, _, _}, s) do
    case HashDict.fetch(s.listener_monitors, monitor_ref) do
      {:ok, {pid, channel}} ->

        s = update_in(s.listener_monitors, &HashDict.delete(&1, monitor_ref))
        s = update_in(s.listener_pids, &HashDict.delete(&1, {pid, channel}))
        s = update_in(s.listeners[channel], &HashSet.delete(&1, pid))

        if HashSet.size(s.listeners[channel]) == 0 do
          # There are no more listeners for this channel; we can send a
          # UNLISTEN command now.
          queue = :queue.in({:query, {nil, nil}, nil}, s.queue)
          s = %{s | queue: queue}

          {:ok, s} = new_query("UNLISTEN #{channel}", [], s)
        end
      :error ->
    end

    {:noreply, s}
  end

  @doc false
  def handle_info({:timeout, timer_ref, :command}, %{queue: queue} = s) do
    {first, second} = queue

    command = Enum.find(first, &(elem(&1, 2) == timer_ref))
              || Enum.find(second, &(elem(&1, 2) == timer_ref))

    if command do
      {:stop, :normal, s}
    else
      {:noreply, s}
    end
  end

  def handle_info({:tcp, _, data}, %{sock: {:gen_tcp, sock}, opts: opts, state: :ssl} = s) do
    case data do
      <<?S>> ->
        case :ssl.connect(sock, opts[:ssl_opts] || []) do
          {:ok, ssl_sock} ->
            :ssl.setopts(ssl_sock, active: :once)
            Protocol.startup(%{s | sock: {:ssl, ssl_sock}})
          {:error, reason} ->
            reply(%Postgrex.Error{message: "ssl negotiation failed: #{reason}"}, s)
            {:stop, :normal, s}
        end

      <<?N>> ->
        reply(%Postgrex.Error{message: "ssl not available"}, s)
        {:stop, :normal, s}
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
    {{:value, {_command, from, timer}}, queue} = :queue.out(queue)
    queue = :queue.in_r({command, from, timer}, queue)
    command(command, %{s | queue: queue})
  end

  @doc false
  def next(%{queue: queue} = s) do
    case :queue.out(queue) do
      {{:value, {command, _from, _timer}}, _queue} ->
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

  defp command({:begin, _opts}, %{transactions: trans} = s) do
    if trans == 0 do
      s = %{s | transactions: 1}
      new_query("BEGIN", [], s)
    else
      s = %{s | transactions: trans + 1}
      new_query("SAVEPOINT postgrex_#{trans}", [], s)
    end
  end

  defp command({:rollback, _opts}, %{queue: queue, transactions: trans} = s) do
    cond do
      trans == 0 ->
        reply(:ok, s)
        queue = :queue.drop(queue)
        {:ok, %{s | queue: queue}}
      trans == 1 ->
        s = %{s | transactions: 0}
        new_query("ROLLBACK", [], s)
      true ->
        trans = trans - 1
        s = %{s | transactions: trans}
        new_query("ROLLBACK TO SAVEPOINT postgrex_#{trans}", [], s)
    end
  end

  defp command({:commit, _opts}, %{queue: queue, transactions: trans} = s) do
    case trans do
      0 ->
        reply(:ok, s)
        queue = :queue.drop(queue)
        {:ok, %{s | queue: queue}}
      1 ->
        s = %{s | transactions: 0}
        new_query("COMMIT", [], s)
      _ ->
        reply(:ok, s)
        queue = :queue.drop(queue)
        {:ok, %{s | queue: queue, transactions: trans - 1}}
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
