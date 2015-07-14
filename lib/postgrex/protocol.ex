defmodule Postgrex.Protocol do
  @moduledoc false

  alias Postgrex.Connection
  alias Postgrex.Types
  import Postgrex.Messages
  import Postgrex.Utils
  import Postgrex.BinaryUtils
  require Logger

  def startup_ssl(%{sock: sock} = s) do
    case msg_send(msg_ssl_request(), sock) do
      :ok ->
        {:noreply, %{s | state: :ssl}}
      {:error, reason} ->
        error(%Postgrex.Error{message: "tcp send: #{reason}"}, s)
    end
  end

  def startup(%{sock: sock, opts: opts} = s) do
    params = opts[:parameters] || []
    user = Keyword.fetch!(opts, :username)
    database = Keyword.fetch!(opts, :database)
    msg = msg_startup(params: [user: user, database: database] ++ params)
    case msg_send(msg, sock) do
      :ok ->
        {:noreply, %{s | state: :auth}}
      {:error, reason} ->
        error(%Postgrex.Error{message: "tcp send: #{reason}"}, s)
    end
  end

  def bootstrap(s) do
    case Postgrex.TypeServer.fetch(s.types_key) do
      {:ok, table} ->
        reply(:ok, s)
        queue = :queue.drop(s.queue)
        Connection.next(%{s | queue: queue, state: :ready, types: table})
      {:lock, ref, table} ->
        extensions = Enum.map(s.extensions, &elem(&1, 0))
        extension_opts = Types.prepare_extensions(s.extensions, s.parameters)
        matchers = Types.extension_matchers(extensions, extension_opts)
        version = s.parameters["server_version"] |> parse_version
        query = Types.bootstrap_query(matchers, version)

        s = %{s | bootstrap: {ref, table, extensions, extension_opts}}
        Connection.new_query(query, [], s)
    end
  end

  def send_query(statement, s) do
    msgs = [
      msg_parse(name: "", query: statement, type_oids: []),
      msg_describe(type: :statement, name: ""),
      msg_flush() ]

    case send_to_result(msgs, s) do
      {:ok, s} ->
        {:ok, %{s | statement: nil, state: :parsing}}
      err ->
        err
    end
  end

  # possible states: ssl, auth, init, parsing, describing, binding, executing,
  #                  ready

  ### auth state ###

  def message(:auth, msg_auth(type: :ok), s) do
    {:ok, %{s | state: :init}}
  end

  def message(:auth, msg_auth(type: :cleartext), s) do
    pass = Keyword.fetch!(s.opts, :password)
    msg = msg_password(pass: pass)
    send_to_result(msg, s)
  end

  def message(:auth, msg_auth(type: :md5, data: salt), s) do
    user = Keyword.fetch!(s.opts, :username)
    pass = Keyword.fetch!(s.opts, :password)

    digest = :crypto.hash(:md5, [pass, user])
             |> Base.encode16(case: :lower)
    digest = :crypto.hash(:md5, [digest, salt])
             |> Base.encode16(case: :lower)
    msg = msg_password(pass: ["md5", digest])
    send_to_result(msg, s)
  end

  def message(:auth, msg_error(fields: fields), s) do
    {:error, Postgrex.Error.exception(postgres: fields), s}
  end

  ### init state ###

  def message(:init, msg_backend_key(pid: pid, key: key), s) do
    {:ok, %{s | backend_key: {pid, key}}}
  end

  def message(:init, msg_ready(), s) do
    opts = clean_opts(s.opts)
    bootstrap(%{s | opts: opts})
  end

  def message(:init, msg_error(fields: fields), s) do
    {:error, Postgrex.Error.exception(postgres: fields), s}
  end

  ### parsing state ###

  def message(:parsing, msg_parse_complete(), s) do
    {:ok, %{s | state: :describing}}
  end

  ### describing state ###

  def message(:describing, msg_no_data(), s) do
    send_params(s, [])
  end

  def message(:describing, msg_parameter_desc(type_oids: oids), s) do
    {:ok, %{s | portal: oids}}
  end

  def message(:describing, msg_row_desc(), %{bootstrap: {_, _, _, _}} = s) do
    send_params(s, [])
  end

  def message(:describing, msg_row_desc(fields: fields), s) do
    {col_oids, col_names} = columns(fields)
    try do
      result_formats = result_formats(col_oids, s.types)
      stat = %{columns: col_names, column_oids: col_oids}
      send_params(%{s | statement: stat}, result_formats)
    catch
      kind, reason ->
        reply({:error, kind, reason, System.stacktrace}, s)
        send_to_result([msg_sync], s)
    end
  end

  ### binding state ###

  def message(:binding, msg_bind_complete(), s) do
    {:ok, %{s | state: :executing}}
  end

  ### executing state ###

  def message(:executing, msg_data_row(values: values), s) do
    {:ok, %{s | rows: [values|s.rows]}}
  end

  def message(:executing, msg_command_complete(),
              %{bootstrap: {ref, table, extensions, extension_opts}} = s) do
    reply(:ok, s)
    types = Types.build_types(s.rows)
    Types.associate_extensions_with_types(table, extensions, extension_opts, types)
    Postgrex.TypeServer.unlock(ref)
    {:ok, %{s | rows: [], bootstrap: false, types: table}}
  end

  def message(:executing, msg_command_complete(tag: tag), s) do
    {command, nrows} = decode_tag(tag)

    reply =
      if is_nil(s.statement) do
        %Postgrex.Result{command: command, num_rows: nrows || 0}
      else
        %{statement: %{column_oids: col_oids, columns: cols},
          types: types, rows: rows} = s

        # Fix for PostgreSQL 8.4 (doesn't include number of selected rows in tag)
        if is_nil(nrows) and command == :select do
          nrows = length(rows)
        end

        try do
          decode_rows(rows, col_oids, types)
        catch
          kind, reason ->
            {:error, kind, reason, System.stacktrace}
        else
          decoded ->
            %Postgrex.Result{command: command, num_rows: nrows || 0,
                             rows: decoded, columns: cols}
        end
      end

    reply(reply, s)
    {:ok, %{s | rows: [], statement: nil, portal: nil}}
  end

  def message(:executing, msg_empty_query(), s) do
    reply(%Postgrex.Result{}, s)
    {:ok, s}
  end

  ### asynchronous messages ###

  def message(_, msg_ready(), s) do
    queue = :queue.drop(s.queue)
    Connection.next(%{s | queue: queue, state: :ready})
  end

  def message(_, msg_parameter(name: name, value: value), s) do
    params = Map.put(s.parameters, name, value)
    {:ok, %{s | parameters: params}}
  end

  def message(state, msg_error(fields: fields), s) do
    error = Postgrex.Error.exception(postgres: fields)
    unless reply(error, s) do
      Logger.warn(fn ->
        ["Unhandled Postgres error: ", Postgrex.Error.message(error)]
      end)
    end
    if state in [:parsing, :describing] do
      # Issue a Sync to get back into valid state when error happened before bind/execute/sync.
      send_to_result([msg_sync], s)
    else
      {:ok, s}
    end
  end

  def message(_, msg_notice(), s) do
    # TODO: subscribers
    {:ok, s}
  end

  def message(_, msg_notify(channel: channel, payload: payload), s) do
    refs = HashDict.get(s.listener_channels, channel) || []
    Enum.each(refs, fn ref ->
      {_channel, pid} = s.listeners[ref]
      send(pid, {:notification, self(), ref, channel, payload})
    end)
    {:ok, s}
  end

  ### helpers ###

  defp decode_rows(rows, col_oids, types) do
    col_oids = List.to_tuple(col_oids)

    Enum.reduce(rows, [], fn values, acc ->
      {_, row} =
        Enum.reduce(values, {0, []}, fn
          nil, {count, list} ->
            {count + 1, [nil|list]}
          bin, {count, list} ->
            oid = elem(col_oids, count)
            decoded = Postgrex.Types.decode(oid, bin, types)
            {count + 1, [decoded|list]}
        end)
      [Enum.reverse(row)|acc]
    end)
  end

  defp send_params(s, rfs) do
    {msgs, s} =
      try do
        encode_params(s)
      catch
        kind, reason ->
          reply({:error, kind, reason, System.stacktrace}, s)
          {[msg_sync], %{s | portal: nil}}
      else
        {pfs, params} ->
          msgs = [
            msg_bind(name_port: "", name_stat: "", param_formats: pfs, params: params, result_formats: rfs),
            msg_execute(name_port: "", max_rows: 0),
            msg_sync() ]
          {msgs, %{s | state: :binding}}
      end

    case send_to_result(msgs, s) do
      {:ok, s} ->
        {:ok, s}
      err ->
        err
    end
  end

  defp encode_params(%{bootstrap: {_, _, _, _}}) do
    {[], []}
  end

  defp encode_params(%{queue: queue, portal: param_oids, types: types}) do
    %{command: {:query, _statement, params}} = :queue.get(queue)
    zipped = Enum.zip(param_oids, params)

    Enum.map(zipped, fn
      {_oid, nil} ->
        {:binary, <<-1::int32>>}
      {oid, param} ->
        format = Types.format(oid, types)
        binary = Types.encode(oid, param, types)
        {format, [<<IO.iodata_length(binary)::int32>>, binary]}
    end)
    |> :lists.unzip
  end

  defp columns(fields) do
    Enum.map(fields, fn row_field(type_oid: oid, name: name) ->
      {oid, name}
    end) |> :lists.unzip
  end

  defp result_formats(columns, types) do
    Enum.map(columns, &Types.format(&1, types))
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
    binaries = Enum.reduce(msgs, [], &[&2 | encode_msg(&1)])
    mod.send(sock, binaries)
  end

  defp msg_send(msg, {mod, sock}) do
    data = encode_msg(msg)
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

  defp clean_opts(opts) do
    Keyword.put(opts, :password, :REDACTED)
  end
end
