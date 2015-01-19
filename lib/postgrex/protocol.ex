defmodule Postgrex.Protocol do
  @moduledoc false

  alias Postgrex.Connection
  alias Postgrex.Types
  import Postgrex.Messages
  import Postgrex.Utils

  def startup_ssl(%{sock: sock} = s) do
    case msg_send(msg_ssl_request(), sock) do
      :ok ->
        {:noreply, %{s | state: :ssl}}
      {:error, reason} ->
        {:stop, :normal, %Postgrex.Error{message: "tcp send: #{reason}"}, s}
    end
  end

  def startup(%{sock: sock, opts: opts} = s) do
    params = opts[:parameters] || []
    msg = msg_startup(params: [user: opts[:username], database: opts[:database]] ++ params)
    case msg_send(msg, sock) do
      :ok ->
        {:noreply, %{s | state: :auth}}
      {:error, reason} ->
        {:stop, :normal, %Postgrex.Error{message: "tcp send: #{reason}"}, s}
    end
  end

  def send_query(statement, s) do
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

  def send_hinted_query(statement, param_types, result_types,
                        %{types: {oids, _}, opts: opts} = s) do
    case types_to_oids(param_types, s) do
      {:ok, param_oids} ->
        case types_to_oids(result_types, s) do
          {:ok, result_oids} ->
            {info, rfs} = extract_row_info(result_oids, oids, opts[:decoder], opts[:formatter])

            msgs = [
              msg_parse(name: "", query: statement, type_oids: param_oids),
              msg_describe(type: :statement, name: "") ]

            case send_to_result(msgs, s) do
              {:ok, s} ->
                stat = %{columns: nil, row_info: List.to_tuple(info)}
                send_params(%{s | statement: stat, portal: param_oids, state: :parsing}, rfs)
              err ->
                err
            end
          err ->
            err
        end
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

  def message(:auth, msg_auth(type: :cleartext), %{opts: opts} = s) do
    msg = msg_password(pass: opts[:password])
    send_to_result(msg, s)
  end

  def message(:auth, msg_auth(type: :md5, data: salt), %{opts: opts} = s) do
    digest = :crypto.hash(:md5, [opts[:password], opts[:username]])
             |> Base.encode16(case: :lower)
    digest = :crypto.hash(:md5, [digest, salt])
             |> Base.encode16(case: :lower)
    msg = msg_password(pass: ["md5", digest])
    send_to_result(msg, s)
  end

  def message(:auth, msg_error(fields: fields), s) do
    {:error, %Postgrex.Error{postgres: Enum.into(fields, %{})}, s}
  end

  ### init state ###

  def message(:init, msg_backend_key(pid: pid, key: key), s) do
    {:ok, %{s | backend_key: {pid, key}}}
  end

  def message(:init, msg_ready(), %{opts: opts} = s) do
    opts = clean_opts(opts)
    s = %{s | opts: opts, bootstrap: true}
    Connection.new_query(Types.bootstrap_query, [], s)
  end

  def message(:init, msg_error(fields: fields), s) do
    {:error, %Postgrex.Error{postgres: Enum.into(fields, %{})}, s}
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

  def message(:describing, msg_row_desc(), %{bootstrap: true} = s) do
    send_params(s, [])
  end

  # If statement is nil we have not sent a hinted query
  def message(:describing, msg_row_desc(fields: fields),
               %{types: {oids, _}, statement: nil, opts: opts} = s) do
    {col_oids, col_names} = columns(fields)
    {info, rfs} = extract_row_info(col_oids, oids, opts[:decoder], opts[:formatter])
    stat = %{columns: col_names, row_info: List.to_tuple(info)}

    send_params(%{s | statement: stat}, rfs)
  end

  def message(:describing, msg_row_desc(fields: fields),
               %{statement: stat} = s) do
    {_, col_names} = columns(fields)
    stat = %{stat | columns: col_names}

    {:ok, %{s | statement: stat, state: :binding}}
  end

  def message(:describing, msg_ready(), s) do
    {:ok, %{s | state: :binding}}
  end

  ### binding state ###

  def message(:binding, msg_bind_complete(), s) do
    {:ok, %{s | state: :executing}}
  end

  ### executing state ###

  def message(:executing, msg_data_row(values: values), %{rows: rows} = s) do
    {:ok, %{s | rows: [values|rows]}}
  end

  def message(:executing, msg_command_complete(), %{bootstrap: true, rows: rows} = s) do
    reply(:ok, s)
    types = Types.build_types(rows)
    {:ok, %{s | rows: [], bootstrap: false, types: types}}
  end

  def message(:executing, msg_command_complete(tag: tag), %{statement: stat} = s) do
    reply =
      if is_nil(stat) do
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

  def message(:executing, msg_empty_query(), s) do
    reply(%Postgrex.Result{}, s)
    {:ok, s}
  end

  ### asynchronous messages ###

  def message(_, msg_ready(), %{queue: queue} = s) do
    queue = :queue.drop(queue)
    Connection.next(%{s | queue: queue, state: :ready})
  end

  def message(_, msg_parameter(name: name, value: value), %{parameters: params} = s) do
    params = Map.put(params, name, value)
    {:ok, %{s | parameters: params}}
  end

  def message(_, msg_error(fields: fields), s) do
    reply(%Postgrex.Error{postgres: Enum.into(fields, %{})}, s)
    {:ok, s}
  end

  def message(_, msg_notice(), s) do
    # TODO: subscribers
    {:ok, s}
  end

  def message(_, msg_notify() = notify, %{listeners: listeners} = s) do
    channel_name = msg_notify(notify, :channel)
    if channel_listeners = HashDict.get(listeners, channel_name) do
      Enum.each(channel_listeners, fn pid ->
        send(pid, {:notification, self(), notify})
      end)
    end
    {:ok, s}
  end

  ### helpers ###

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
    {msgs, s} =
      try do
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

  defp encode_params(%{bootstrap: true}) do
    {[], []}
  end

  defp encode_params(%{queue: queue, portal: param_oids, types: {oids, _}, opts: opts}) do
    {{:query, _statement, params, _}, _from} = :queue.get(queue)
    zipped = Enum.zip(param_oids, params)
    extra = {oids, opts[:encoder], opts[:formatter]}

    Enum.map(zipped, fn {oid, param} ->
      info = Dict.fetch!(oids, oid)
      default = &Types.encode(info, extra, &1)
      Types.encode_value(info, extra, default, param)
    end) |> :lists.unzip
  end

  defp columns(fields) do
    Enum.map(fields, fn row_field(type_oid: oid, name: name) ->
      {oid, name}
    end) |> :lists.unzip
  end

  defp extract_row_info(columns, oids, decoder, formatter) do
    Enum.map(columns, fn oid ->
      info = Dict.fetch!(oids, oid)
      format = Types.format(oids, oid, formatter)
      extra = {oids, decoder}

      default =
        case format do
          :binary -> &Types.decode_binary(info, extra, &1)
          :text   -> &Types.decode_text(info, extra, &1)
        end

      {{info, format, default}, format}
    end) |> :lists.unzip
  end

  defp create_result(tag) do
    create_result(tag, nil, nil)
  end

  defp create_result(tag, rows, cols) do
    {command, nrows} = decode_tag(tag)

    # Fix for PostgreSQL 8.4 (doesn't include number of selected rows in tag)
    if is_nil(nrows) and command == :select do
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

  defp types_to_oids(type_names, %{types: {_, types}} = s) do
    result =
      Enum.flat_map_reduce(type_names, nil, fn name, _acc ->
        if type = types[name] do
          {[type.oid], nil}
        else
          {:halt, "no type of name: #{name}"}
        end
      end)

    case result do
      {oids,  nil}    -> {:ok, oids}
      {_oids, reason} -> {:error, %Postgrex.Error{message: reason}, s}
    end
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
