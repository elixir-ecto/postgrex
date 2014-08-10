defmodule Postgrex.Connection do
  @moduledoc """
  Main API for Postgrex. This module handles the connection to postgres.
  """

  use GenServer
  alias Postgrex.Protocol
  alias Postgrex.Types
  import Postgrex.Protocol.Messages
  import Postgrex.BinaryUtils

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
            {:binary | :text, binary}
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
      |> Enum.reject(fn {_k,v} -> nil?(v) end)
    case :gen_server.start_link(__MODULE__, [], []) do
      {:ok, pid} ->
        timeout = opts[:connect_timeout] || @timeout
        case :gen_server.call(pid, {:connect, opts}, timeout) do
          :ok -> {:ok, pid}
          err -> {:error, err}
        end
      err -> err
    end
  end

  @doc """
  Stop the process and disconnect.
  """
  @spec stop(pid) :: :ok
  @spec stop(pid, timeout) :: :ok
  def stop(pid, timeout \\ @timeout) do
    :gen_server.call(pid, :stop, timeout)
  end

  @doc """
  Runs an (extended) query and returns the result as `{:ok, %Postgrex.Result{}}`
  or `{:error, %Postgrex.Error{}}` if there was an error. Parameters can be
  set in the query as `$1` embedded in the query string. Parameters are given as
  a list of elixir values. See the README for information on how Postgrex
  encodes and decodes elixir values by default. See `Postgrex.Result` for the
  result data.
  """
  @spec query(pid, String.t) :: {:ok, Postgrex.Result.t} | {:error, Postgrex.Error.t}
  @spec query(pid, String.t, list) :: {:ok, Postgrex.Result.t} | {:error, Postgrex.Error.t}
  @spec query(pid, String.t, list, timeout) :: {:ok, Postgrex.Result.t} | {:error, Postgrex.Error.t}
  def query(pid, statement, params \\ [], timeout \\ @timeout) do
    case :gen_server.call(pid, {{:query, statement, params}, timeout}, timeout) do
      %Postgrex.Result{} = res -> {:ok, res}
      %Postgrex.Error{} = err  -> {:error, err}
    end
  end

  @doc """
  Runs an (extended) query and returns the result or raises `Postgrex.Error` if
  there was an error. See `query/3`.
  """
  @spec query!(pid, String.t) :: Postgrex.Result.t | no_return
  @spec query!(pid, String.t, list) :: Postgrex.Result.t | no_return
  @spec query!(pid, String.t, list, timeout) :: Postgrex.Result.t | no_return
  def query!(pid, statement, params \\ [], timeout \\ @timeout) do
    case :gen_server.call(pid, {{:query, statement, params}, timeout}, timeout) do
      %Postgrex.Result{} = res -> res
      %Postgrex.Error{} = err  -> raise err
    end
  end


  @doc """
  Returns a cached map of connection parameters.
  """
  @spec parameters(pid) :: map
  @spec parameters(pid, timeout) :: map
  def parameters(pid, timeout \\ @timeout) do
    :gen_server.call(pid, :parameters, timeout)
  end

  @doc """
  Starts a transaction. Returns `:ok` or `{:error, %Postgrex.Error{}}` if an
  error occurred. Transactions can be nested with the help of savepoints. A
  transaction won't end until a `rollback/1` or `commit/1` have been issued for
  every `begin/1`.

  ## Example

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
  @spec begin(pid) :: :ok | {:error, Postgrex.Error.t}
  @spec begin(pid, timeout) :: :ok | {:error, Postgrex.Error.t}
  def begin(pid, timeout \\ @timeout) do
    case :gen_server.call(pid, {:begin, timeout}, timeout) do
      %Postgrex.Result{} -> :ok
      %Postgrex.Error{} = err -> {:error, err}
    end
  end

  @doc """
  Starts a transaction. Returns `:ok` if it was successful or raises
  `Postgrex.Error` if an error occurred. See `begin/1`.
  """
  @spec begin!(pid) :: :ok | no_return
  @spec begin!(pid, timeout) :: :ok | no_return
  def begin!(pid, timeout \\ @timeout) do
    case :gen_server.call(pid, {:begin, timeout}, timeout) do
      %Postgrex.Result{} -> :ok
      %Postgrex.Error{} = err -> raise err
    end
  end

  @doc """
  Rolls back a transaction. Returns `:ok` or `{:error, %Postgrex.Error{}}` if
  an error occurred. See `begin/1` for more information.
  """
  @spec rollback(pid) :: :ok | {:error, Postgrex.Error.t}
  @spec rollback(pid, timeout) :: :ok | {:error, Postgrex.Error.t}
  def rollback(pid, timeout \\ @timeout) do
    case :gen_server.call(pid, {:rollback, timeout}, timeout) do
      :ok -> :ok
      %Postgrex.Result{} -> :ok
      %Postgrex.Error{} = err -> {:error, err}
    end
  end

  @doc """
  Rolls back a transaction. Returns `:ok` if it was successful or raises
  `Postgrex.Error` if an error occurred. See `rollback/1`.
  """
  @spec rollback!(pid) :: :ok | no_return
  @spec rollback!(pid, timeout) :: :ok | no_return
  def rollback!(pid, timeout \\ @timeout) do
    case :gen_server.call(pid, {:rollback, timeout}, timeout) do
      :ok -> :ok
      %Postgrex.Result{} -> :ok
      %Postgrex.Error{} = err -> raise err
    end
  end

  @doc """
  Commits a transaction. Returns `:ok` or `{:error, %Postgrex.Error{}}` if an
  error occurred. See `begin/1` for more information.
  """
  @spec commit(pid) :: :ok | {:error, Postgrex.Error.t}
  @spec commit(pid, timeout) :: :ok | {:error, Postgrex.Error.t}
  def commit(pid, timeout \\ @timeout) do
    case :gen_server.call(pid, {:commit, timeout}, timeout) do
      :ok -> :ok
      %Postgrex.Result{} -> :ok
      %Postgrex.Error{} = err -> {:error, err}
    end
  end

  @doc """
  Commits a transaction. Returns `:ok` if it was successful or raises
  `Postgrex.Error` if an error occurred. See `commit/1`.
  """
  @spec commit!(pid) :: :ok | no_return
  @spec commit!(pid, timeout) :: :ok | no_return
  def commit!(pid, timeout \\ @timeout) do
    case :gen_server.call(pid, {:commit, timeout}, timeout) do
      :ok -> :ok
      %Postgrex.Result{} -> :ok
      %Postgrex.Error{} = err -> raise err
    end
  end

  @doc """
  Helper for creating reliable transactions. If an error is raised in the given
  function the transaction is rolled back, otherwise it is commited. A
  transaction can be cancelled with `throw :postgrex_rollback`. If there is a
  connection error `Postgrex.Error` will be raised.

  NOTE:

  * Do not use this function in conjunction with `begin/1`, `commit/1` and
  `rollback/1`.
  *  The timeout argument is not the maximum timeout of the entire call but
  rather the timeout of the `commit/2` and `rollback/2` calls that this function
  makes.
  """
  @spec in_transaction(pid, (() -> term)) :: term | no_return
  @spec in_transaction(pid, timeout, (() -> term)) :: term | no_return
  def in_transaction(pid, timeout \\ @timeout, fun) do
    case begin(pid) do
      :ok ->
        try do
          value = fun.()
          case commit(pid, timeout) do
            :ok -> value
            err -> raise err
          end
        catch
          :throw, :postgrex_rollback ->
            case rollback(pid, timeout) do
              :ok -> nil
              err -> raise err
            end
          type, term ->
            _ = rollback(pid, timeout)
            :erlang.raise(type, term, System.stacktrace)
        end
      err -> raise err
    end
  end

  defp clean_opts(opts) do
    Keyword.put(opts, :password, :REDACTED)
  end

  ### GEN_SERVER CALLBACKS ###

  @doc false
  def init([]) do
    {:ok, %{sock: nil, tail: "", state: :ready, parameters: %{}, backend_key: nil,
            rows: [], statement: nil, portal: nil, bootstrap: false, types: nil,
            transactions: 0, queue: :queue.new, opts: nil}}
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
    sock_opts = [ {:active, :once}, {:packet, :raw}, :binary ]

    case :gen_tcp.connect(host, port, sock_opts, timeout) do
      {:ok, sock} ->
        queue = :queue.in({{:connect, opts}, from, nil}, queue)
        s = %{s | opts: opts, sock: {:gen_tcp, sock}, queue: queue}
        if opts[:ssl] do
          startup_ssl(s)
        else
          startup(s)
        end

      {:error, reason} ->
        {:stop, :normal, %Postgrex.Error{message: "tcp connect: #{reason}"}, s}
    end
  end

  def handle_call(:parameters, _from, %{parameters: params} = s) do
    {:reply, params, s}
  end

  def handle_call({command, timeout}, from, %{state: state, queue: queue} = s) do
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
            startup(%{s | sock: {:ssl, ssl_sock}})
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

  ### PRIVATE FUNCTIONS ###

  defp next(%{queue: queue} = s) do
    case :queue.out(queue) do
      {{:value, {command, _from, _timer}}, _queue} ->
        command(command, s)
      {:empty, _queue} ->
        {:ok, s}
    end
  end

  defp command({:query, statement, _params}, s) do
    msgs = [
      msg_parse(name: "", query: statement, type_oids: []),
      msg_describe(type: :statement, name: ""),
      msg_sync() ]

    case send_to_result(msgs, s) do
      {:ok, s} ->
        {:ok, %{s | statement: nil, state: :parsing}}
      err ->
        err
    end
  end

  defp command(:begin, %{transactions: trans} = s) do
    if trans == 0 do
      s = %{s | transactions: 1}
      new_query("BEGIN", [], s)
    else
      s = %{s | transactions: trans + 1}
      new_query("SAVEPOINT postgrex_#{trans}", [], s)
    end
  end

  defp command(:rollback, %{queue: queue, transactions: trans} = s) do
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

  defp command(:commit, %{queue: queue, transactions: trans} = s) do
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
        msg = Protocol.parse(type, size, data)
        case message(state, msg, s) do
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

  # possible states: ssl, auth, init, parsing, describing, binding, executing,
  #                  ready

  ### auth state ###

  defp message(:auth, msg_auth(type: :ok), s) do
    {:ok, %{s | state: :init}}
  end

  defp message(:auth, msg_auth(type: :cleartext), %{opts: opts} = s) do
    msg = msg_password(pass: opts[:password])
    send_to_result(msg, s)
  end

  defp message(:auth, msg_auth(type: :md5, data: salt), %{opts: opts} = s) do
    digest = :crypto.hash(:md5, [opts[:password], opts[:username]])
             |> Base.encode16(case: :lower)
    digest = :crypto.hash(:md5, [digest, salt])
             |> Base.encode16(case: :lower)
    msg = msg_password(pass: ["md5", digest])
    send_to_result(msg, s)
  end

  defp message(:auth, msg_error(fields: fields), s) do
    {:error, %Postgrex.Error{postgres: fields}, s}
  end

  ### init state ###

  defp message(:init, msg_backend_key(pid: pid, key: key), s) do
    {:ok, %{s | backend_key: {pid, key}}}
  end

  defp message(:init, msg_ready(), %{opts: opts} = s) do
    opts = clean_opts(opts)
    s = %{s | opts: opts, bootstrap: true}
    new_query(Types.bootstrap_query, [], s)
  end

  defp message(:init, msg_error(fields: fields), s) do
    {:error, %Postgrex.Error{postgres: fields}, s}
  end

  ### parsing state ###

  defp message(:parsing, msg_parse_complete(), s) do
    {:ok, %{s | state: :describing}}
  end

  ### describing state ###

  defp message(:describing, msg_no_data(), s) do
    send_params(s, [])
  end

  defp message(:describing, msg_parameter_desc(type_oids: oids), s) do
    {:ok, %{s | portal: oids}}
  end

  defp message(:describing, msg_row_desc(fields: fields),
               %{types: types, bootstrap: bootstrap, opts: opts} = s) do
    rfs = []
    if not bootstrap do
      {info, rfs, cols} = extract_row_info(fields, types, opts[:decoder], opts[:formatter])
      stat = %{columns: cols, row_info: List.to_tuple(info)}
      s = %{s | statement: stat}
    end

    send_params(s, rfs)
  end

  defp message(:describing, msg_ready(), s) do
    {:ok, %{s | state: :binding}}
  end

  ### binding state ###

  defp message(:binding, msg_bind_complete(), s) do
    {:ok, %{s | state: :executing}}
  end

  ### executing state ###

  defp message(:executing, msg_data_row(values: values), %{rows: rows} = s) do
    {:ok, %{s | rows: [values|rows]}}
  end

  defp message(:executing, msg_command_complete(), %{bootstrap: true, rows: rows} = s) do
    reply(:ok, s)
    types = Types.build_types(rows)
    {:ok, %{s | rows: [], bootstrap: false, types: types}}
  end

  defp message(:executing, msg_command_complete(tag: tag), %{statement: stat} = s) do
    reply =
      if nil?(stat) do
        create_result(tag)
      else
        try do
          result = decode_rows(s)
          %{columns: cols} = stat
          create_result(tag, result, cols)
        catch
          {:postgrex_decode, msg} ->
            %Postgrex.Error{message: msg}
        end
      end

    reply(reply, s)
    {:ok, %{s | rows: [], statement: nil, portal: nil}}
  end

  defp message(:executing, msg_empty_query(), s) do
    reply(%Postgrex.Result{}, s)
    {:ok, s}
  end

  ### asynchronous messages ###

  defp message(_, msg_ready(), %{queue: queue} = s) do
    queue = :queue.drop(queue)
    next(%{s | queue: queue, state: :ready})
  end

  defp message(_, msg_parameter(name: name, value: value), %{parameters: params} = s) do
    params = Map.put(params, name, value)
    {:ok, %{s | parameters: params}}
  end

  defp message(_, msg_error(fields: fields), s) do
    reply(%Postgrex.Error{postgres: fields}, s)
    {:ok, s}
  end

  defp message(_, msg_notice(), s) do
    # TODO: subscribers
    {:ok, s}
  end

  ### helpers ###

  defp error(error, s) do
    if reply(error, s) do
      {:stop, :normal, s}
    else
      {:stop, error, s}
    end
  end

  defp reply(reply, %{queue: queue}) do
    case :queue.out(queue) do
      {{:value, {_command, from, _timer}}, _queue} ->
        :gen_server.reply(from, reply)
        true
      {:empty, _queue} ->
        false
    end
  end

  defp reply(reply, {_, _} = from) do
    :gen_server.reply(from, reply)
    true
  end

  defp startup_ssl(%{sock: sock} = s) do
    case msg_send(msg_ssl_request(), sock) do
      :ok ->
        {:noreply, %{s | state: :ssl}}
      {:error, reason} ->
        {:stop, :normal, %Postgrex.Error{message: "tcp send: #{reason}"}, s}
    end
  end

  defp startup(%{sock: sock, opts: opts} = s) do
    params = opts[:parameters] || []
    msg = msg_startup(params: [user: opts[:username], database: opts[:database]] ++ params)
    case msg_send(msg, sock) do
      :ok ->
        {:noreply, %{s | state: :auth}}
      {:error, reason} ->
        {:stop, :normal, %Postgrex.Error{message: "tcp send: #{reason}"}, s}
    end
  end

  defp new_query(statement, params, %{queue: queue} = s) do
    command = {:query, statement, params}
    {{:value, {_command, from, timer}}, queue} = :queue.out(queue)
    queue = :queue.in_r({command, from, timer}, queue)
    command(command, %{s | queue: queue})
  end

  defp decode_rows(%{statement: %{row_info: info}, rows: rows, opts: opts}) do
    decoder = opts[:decoder]

    Enum.reduce(rows, [], fn values, acc ->
      {_, row} = Enum.reduce(values, {0, []}, fn
        nil, {count, list} ->
          {count + 1, [nil|list]}

        bin, {count, list} ->
          {info, format, default} = elem(info, count)
          decoded = Types.decode_value(info, format, decoder, default, bin)
          {count + 1, [decoded|list]}
      end)

      row = Enum.reverse(row) |> List.to_tuple
      [ row | acc ]
    end)
  end

  defp send_params(s, rfs) do
    {msgs, s} = try do
      {pfs, params} = encode_params(s)

      msgs = [
        msg_bind(name_port: "", name_stat: "", param_formats: pfs, params: params, result_formats: rfs),
        msg_execute(name_port: "", max_rows: 0),
        msg_sync() ]
      {msgs, s}

    catch
      {:postgrex_encode, reason} ->
        reply(%Postgrex.Error{message: reason}, s)
        {[msg_sync], %{s | portal: nil}}
    end

    case send_to_result(msgs, s) do
      {:ok, s} ->
        {:ok, s}
      err ->
        err
    end
  end

  defp encode_params(%{queue: queue, portal: param_oids, types: types, opts: opts}) do
    {{:query, _statement, params}, _from, _timer} = :queue.get(queue)
    zipped = Enum.zip(param_oids, params)
    extra = {types, opts[:encoder], opts[:formatter]}

    Enum.map(zipped, fn
      {_oid, nil} ->
        {:binary, nil}

      {oid, param} ->
        info = Dict.fetch!(types, oid)
        default = &Types.encode(info, extra, &1)
        Types.encode_value(info, extra, default, param)

    end) |> :lists.unzip
  end

  defp extract_row_info(fields, types, decoder, formatter) do
    Enum.map(fields, fn row_field(name: name, type_oid: oid) ->
      info = Dict.fetch!(types, oid)
      format = Types.format(types, oid, formatter)
      extra = {types, decoder}

      default =
        case format do
          :binary -> &Types.decode_binary(info, extra, &1)
          :text   -> &Types.decode_text(info, extra, &1)
        end

      {{info, format, default}, format, name}
    end) |> List.unzip |> List.to_tuple
  end

  defp create_result(tag) do
    create_result(tag, nil, nil)
  end

  defp create_result(tag, rows, cols) do
    {command, nrows} = decode_tag(tag)

    # Fix for PostgreSQL 8.4 (doesn't include number of selected rows in tag)
    if nil?(nrows) and command == :select do
      nrows = length(rows)
    end

    %Postgrex.Result{command: command, num_rows: nrows || 0, rows: rows,
                     columns: cols}
  end

  defp decode_tag(tag) do
    words = :binary.split(tag, " ", [:global])
    words = Enum.map(words, fn word ->
      case Integer.parse(word) do
        {num, ""} -> num
        :error -> word
      end
    end)

    {command, nums} = Enum.split_while(words, &is_binary(&1))
    command = Enum.join(command, "_") |> String.downcase |> String.to_atom
    {command, List.last(nums)}
  end

  defp msg_send(msg, %{sock: sock}), do: msg_send(msg, sock)

  defp msg_send(msgs, {mod, sock}) when is_list(msgs) do
    binaries = Enum.reduce(msgs, [], &[&2 | Protocol.encode_msg(&1)])
    mod.send(sock, binaries)
  end

  defp msg_send(msg, {mod, sock}) do
    data = Protocol.encode_msg(msg)
    mod.send(sock, data)
  end

  defp send_to_result(msg, s) do
    case msg_send(msg, s) do
      :ok ->
        {:ok, s}
      {:error, reason} ->
        {:error, %Postgrex.Error{message: "tcp send: #{reason}"} , s}
    end
  end
end
