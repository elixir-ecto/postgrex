defmodule Postgrex.Connection do
  @moduledoc """
  Main API for Postgrex. This module handles the connection to postgres.
  """

  use GenServer.Behaviour
  use Postgrex.Protocol.Messages
  alias Postgrex.Protocol
  alias Postgrex.Types
  import Postgrex.BinaryUtils

  # possible states: auth, init, parsing, describing, binding, executing, ready

  defrecordp :state, [ :opts, :sock, :tail, :state, :reply_to, :reply,
                       :parameters, :backend_key, :rows, :statement, :portal,
                       :qparams, :bootstrap, :types, :transactions ]

  defrecordp :statement, [:row_info, :columns]
  defrecordp :portal, [:param_oids]

  ### PUBLIC API ###

  @doc """
  Start the connection process and connect to postgres.

  ## Options

    * `:hostname` - Server hostname (required);
    * `:port` - Server port (default: 5432);
    * `:username` - Username (required);
    * `:password` - User password;
    * `:encoder` - Custom encoder function;
    * `:decoder` - Custom decoder function;
    * `:formatter` - Function deciding the format for a type;
    * `:parameters` - Keyword list of connection parameters;

  ## Function signatures

      @spec encoder(type :: atom, sender :: atom, oid :: integer, default :: fun, param :: term) ::
            { :binary | :text, binary }
      @spec decoder(type :: atom, sender :: atom, oid :: integer, default :: fun, bin :: binary) ::
            term
      @spec decode_formatter(type :: atom, sender :: atom, oid :: integer) ::
            :binary | :text
  """
  @spec start_link(Keyword.t) :: { :ok, pid } | { :error, Postgrex.Error.t | term }
  def start_link(opts) do
    case :gen_server.start_link(__MODULE__, [], []) do
      { :ok, pid } ->
        opts = fix_opts(opts)
        case :gen_server.call(pid, { :connect, opts }) do
          :ok -> { :ok, pid }
          err -> { :error, err }
        end
      err -> err
    end
  end

  @doc """
  Stop the process and disconnect.
  """
  @spec stop(pid) :: :ok
  def stop(pid) do
    :gen_server.call(pid, :stop)
  end

  @doc """
  Runs an (extended) query and returns the result as `{ :ok, Postgrex.Result[]
  }` or `{ :error, Postgrex.Error[] }` if there was an error. Parameters can be
  set in the query as `$1` embedded in the query string. Parameters are given as
  a list of elixir values. See the README for information on how Postgrex
  encodes and decodes elixir values by default. See `Postgrex.Result` for the
  result data.
  """
  @spec query(pid, String.t, list) :: { :ok, Postgrex.Result.t } | { :error, Postgrex.Error.t }
  def query(pid, statement, params // []) do
    case :gen_server.call(pid, { :query, statement, params }) do
      Postgrex.Result[] = res -> { :ok, res }
      Postgrex.Error[] = err -> { :error, err }
    end
  end

  @doc """
  Runs an (extended) query and returns the result or raises `Postgrex.Error` if
  there was an error. See `query/3`.
  """
  @spec query!(pid, String.t, list) :: Postgrex.Result.t | no_return
  def query!(pid, statement, params // []) do
    case :gen_server.call(pid, { :query, statement, params }) do
      Postgrex.Result[] = res -> res
      Postgrex.Error[] = err -> raise err
    end
  end


  @doc """
  Returns a cached list dict of connection parameters.
  """
  @spec parameters(pid) :: [{ String.t, String.t }]
  def parameters(pid) do
    :gen_server.call(pid, :parameters)
  end

  @doc """
  Starts a transaction. Returns `:ok` or `{ :error, Postgrex.Error[] }` if an
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
  @spec begin(pid) :: :ok | { :error, Postgrex.Error.t }
  def begin(pid) do
    case :gen_server.call(pid, :begin) do
      Postgrex.Result[] -> :ok
      Postgrex.Error[] = err -> err
    end
  end

  @doc """
  Starts a transaction. Returns `:ok` if it was successful or raises
  `Postgrex.Error` if an error occurred. See `begin/1`.
  """
  @spec begin!(pid) :: :ok | no_return
  def begin!(pid) do
    case :gen_server.call(pid, :begin) do
      Postgrex.Result[] -> :ok
      Postgrex.Error[] = err -> raise err
    end
  end

  @doc """
  Rolls back a transaction. Returns `:ok` or `{ :error, Postgrex.Error[] }` if
  an error occurred. See `begin/1` for more information.
  """
  @spec rollback(pid) :: :ok | { :error, Postgrex.Error.t }
  def rollback(pid) do
    case :gen_server.call(pid, :rollback) do
      :ok -> :ok
      Postgrex.Result[] -> :ok
      Postgrex.Error[] = err -> err
    end
  end

  @doc """
  Rolls back a transaction. Returns `:ok` if it was successful or raises
  `Postgrex.Error` if an error occurred. See `rollback/1`.
  """
  @spec rollback!(pid) :: :ok | no_return
  def rollback!(pid) do
    case :gen_server.call(pid, :rollback) do
      :ok -> :ok
      Postgrex.Result[] -> :ok
      Postgrex.Error[] = err -> raise err
    end
  end

  @doc """
  Commits a transaction. Returns `:ok` or `{ :error, Postgrex.Error[] }` if an
  error occurred. See `begin/1` for more information.
  """
  @spec commit(pid) :: :ok | { :error, Postgrex.Error.t }
  def commit(pid) do
    case :gen_server.call(pid, :commit) do
      :ok -> :ok
      Postgrex.Result[] -> :ok
      Postgrex.Error[] = err -> err
    end
  end

  @doc """
  Commits a transaction. Returns `:ok` if it was successful or raises
  `Postgrex.Error` if an error occurred. See `commit/1`.
  """
  @spec commit!(pid) :: :ok | no_return
  def commit!(pid) do
    case :gen_server.call(pid, :commit) do
      :ok -> :ok
      Postgrex.Result[] -> :ok
      Postgrex.Error[] = err -> raise err
    end
  end

  @doc """
  Helper for creating reliable transactions. If an error is raised in the given
  function the transaction is rolled back, otherwise it is commited. A
  transaction can be cancelled with `throw :postgrex_rollback`. If there is a
  connection error `Postgrex.Error` will be raised.

  NOTE: Do not use this function in conjunction with `begin/1`, `commit/1` and
  `rollback/1`.
  """
  @spec in_transaction(pid, (() -> term)) :: term | no_return
  def in_transaction(pid, fun) do
    case begin(pid) do
      :ok ->
        try do
          value = fun.()
          case commit(pid) do
            :ok -> value
            err -> raise err
          end
        catch
          :throw, :postgrex_rollback ->
            case rollback(pid) do
              :ok -> nil
              err -> raise err
            end
          type, term ->
            rollback(pid)
            :erlang.raise(type, term, System.stacktrace)
        end
      err -> raise err
    end
  end

  defp fix_opts(opts) do
    opts
      |> Keyword.update!(:hostname, &if is_binary(&1), do: String.to_char_list!(&1), else: &1)
      |> Keyword.put_new(:port, 5432)
  end

  ### GEN_SERVER CALLBACKS ###

  @doc false
  def init([]) do
    { :ok, state(state: :ready, tail: "", parameters: [], rows: [],
                 bootstrap: false, transactions: 0) }
  end

  @doc false
  def handle_call(:stop, from, state(state: :ready) = s) do
    { :stop, :normal, state(s, reply_to: from) }
  end

  def handle_call({ :connect, opts }, from, state(state: :ready) = s) do
    sock_opts = [ { :active, :once }, { :packet, :raw }, :binary ]

    case :gen_tcp.connect(opts[:hostname], opts[:port], sock_opts) do
      { :ok, sock } ->
        params = opts[:parameters] || []
        msg = msg_startup(params: [user: opts[:username], database: opts[:database]] ++ params)
        case send(msg, sock) do
          :ok ->
            { :noreply, state(s, opts: opts, sock: sock, reply_to: from, state: :auth) }
          { :error, reason } ->
            { :stop, :normal, Postgrex.Error[reason: "tcp send: #{reason}"], s }
        end

      { :error, reason } ->
        { :stop, :normal, Postgrex.Error[reason: "tcp connect: #{reason}"], s }
    end
  end

  def handle_call({ :query, statement, params }, from, state(state: :ready) = s) do
    case send_query(statement, s) do
      { :ok, s } ->
        { :noreply, state(s, qparams: params, reply_to: from) }
      { :error, reason, s } ->
        { :stop, :normal, { :error, reason }, s }
    end
  end

  def handle_call(:parameters, _from, state(parameters: params, state: :ready) = s) do
    { :reply, params, s }
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
        { :reply, :ok, s }
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
        { :reply, :ok, s }
      1 ->
        s = state(s, transactions: 0)
        handle_call({ :query, "COMMIT", [] }, from, s)
      _ ->
        { :reply, :ok, state(s, transactions: trans - 1) }
    end
  end

  @doc false
  def handle_info({ :tcp, _, data }, state(reply_to: to, sock: sock, tail: tail) = s) do
    case handle_data(tail <> data, state(s, tail: "")) do
      { :ok, s } ->
        :inet.setopts(sock, active: :once)
        { :noreply, s }
      { :error, error, s } ->
        if to do
          :gen_server.reply(to, error)
          { :stop, :normal, s }
        else
          { :stop, error, s }
        end
    end
  end

  def handle_info({ :tcp_closed, _ }, state(reply_to: to) = s) do
    error = Postgrex.Error[reason: "tcp closed"]
    if to do
      :gen_server.reply(to, error)
      { :stop, :normal, s }
    else
      { :stop, error, s }
    end
  end

  def handle_info({ :tcp_error, _, reason }, state(reply_to: to) = s) do
    error = Postgrex.Error[reason: "tcp error: #{reason}"]
    if to do
      :gen_server.reply(to, error)
      { :stop, :normal, s }
    else
      { :stop, error, s }
    end
  end

  @doc false
  def terminate(reason, state(reply_to: to, reply: reply, sock: sock)) do
    if sock do
      send(msg_terminate(), sock)
      :gen_tcp.close(sock)
    end

    if to do
      if reason == :normal do
        :gen_server.reply(to, reply || :ok)
      else
        :gen_server.reply(to, Postgrex.Error[reason: "terminated: #{inspect reason}"])
      end
    end
  end

  ### PRIVATE FUNCTIONS ###

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

  defp message(msg_no_data(), state(state: :describing) = s) do
    send_params(s, [])
  end

  defp message(msg_parameter_desc(type_oids: oids), state(state: :describing) = s) do
    { :ok, state(s, portal: portal(param_oids: oids)) }
  end

  defp message(msg_row_desc(fields: fields), state(types: types, bootstrap: bootstrap, opts: opts, state: :describing) = s) do
    rfs = []
    if not bootstrap do
      { info, rfs, cols } = extract_row_info(fields, types, opts[:decoder], opts[:formatter])
      stat = statement(columns: cols, row_info: list_to_tuple(info))
      s = state(s, statement: stat)
    end

    send_params(s, rfs)
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
    { :ok, state(s, reply: :ok, rows: [], bootstrap: false, types: types) }
  end

  defp message(msg_command_complete(tag: tag), state(statement: stat, state: :executing) = s) do
    reply =
      if nil?(stat) do
        create_result(tag)
      else
        try do
          result = decode_rows(s)
          statement(columns: cols) = stat
          create_result(tag, result, cols)
        catch
          { :postgrex_decode, msg } ->
            Postgrex.Error[reason: msg]
        end
      end
    { :ok, state(s, reply: reply, rows: [], statement: nil, portal: nil) }
  end

  defp message(msg_empty_query(), state(state: :executing) = s) do
    { :ok, state(s, reply: Postgrex.Result[]) }
  end

  ### asynchronous messages ###

  defp message(msg_ready(), state(reply_to: to, reply: reply) = s) do
    if to, do: :gen_server.reply(to, reply)
    { :ok, state(s, reply_to: nil, reply: nil, state: :ready) }
  end

  defp message(msg_parameter(name: name, value: value), state(parameters: params) = s) do
    params = Dict.put(params, name, value)
    { :ok, state(s, parameters: params) }
  end

  defp message(msg_error(fields: fields), s) do
    # TODO: subscribers
    { :ok, state(s, reply: Postgrex.Error[postgres: fields]) }
  end

  defp message(msg_notice(), s) do
    # TODO: subscribers
    { :ok, s }
  end

  ### helpers ###

  defp decode_rows(state(statement: stat, rows: rows, opts: opts)) do
    statement(row_info: info) = stat
    decoder = opts[:decoder]

    Enum.reduce(rows, [], fn values, acc ->
      { _, row } = Enum.reduce(values, { 0, [] }, fn
        nil, { count, list } ->
          { count + 1, [nil|list] }

        bin, { count, list } ->
          { sender, type, oid, format, default } = elem(info, count)
          decoded = Types.decode_value(sender, type, oid, format, decoder, default, bin)
          { count + 1, [decoded|list] }
      end)

      row = Enum.reverse(row) |> list_to_tuple
      [ row | acc ]
    end)
  end

  defp send_params(s, rfs) do
    { msgs, s } = try do
      { pfs, params } = encode_params(s)

      msgs = [
        msg_bind(name_port: "", name_stat: "", param_formats: pfs, params: params, result_formats: rfs),
        msg_execute(name_port: "", max_rows: 0),
        msg_sync() ]
      { msgs, s }

    catch
      { :postgrex_encode, reason } ->
        reply = Postgrex.Error[reason: reason]
        { [msg_sync], state(s, reply: reply, portal: nil) }
    end

    case send_to_result(msgs, s) do
      { :ok, s } ->
        { :ok, state(s, qparams: nil) }
      err ->
        err
    end
  end

  defp encode_params(state(qparams: params, portal: portal, types: types, opts: opts)) do
    param_oids = portal(portal, :param_oids)
    zipped = Enum.zip(param_oids, params)
    extra = { types, opts[:encoder], opts[:formatter] }

    Enum.map(zipped, fn
      { _oid, nil } ->
        { :binary, nil }

      { oid, param } ->
        { sender, type } = Types.oid_to_type(types, oid)
        default = &Types.encode(sender, oid, extra, &1)
        Types.encode_value(sender, type, oid, extra, default, param)

    end) |> :lists.unzip
  end

  defp extract_row_info(fields, types, decoder, formatter) do
    Enum.map(fields, fn row_field(name: name, type_oid: oid) ->
      { sender, type } = Types.oid_to_type(types, oid)
      format = Types.format(types, oid, formatter)
      extra = { types, decoder }
      default = &Types.decode(sender, extra, &1)

      { { sender, type, oid, format, default }, format, name }
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
    create_result(tag, nil, nil)
  end

  defp create_result(tag, rows, cols) do
    { command, nrows } = decode_tag(tag)

    # Fix for PostgreSQL 8.4 (doesn't include number of selected rows in tag)
    if nil?(nrows) and command == :select do
      nrows = length(rows)
    end

    Postgrex.Result[command: command, num_rows: nrows || 0, rows: rows,
                    columns: cols]
  end

  defp decode_tag(tag) do
    words = :binary.split(tag, " ", [:global])
    words = Enum.map(words, fn word ->
      case Integer.parse(word) do
        { num, "" } -> num
        :error -> word
      end
    end)

    { command, nums } = Enum.split_while(words, &is_binary(&1))
    command = Enum.join(command, "_") |> String.downcase |> binary_to_atom
    { command, List.last(nums) }
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
