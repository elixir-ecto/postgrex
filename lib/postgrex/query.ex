defmodule Postgrex.Query do
  @moduledoc """
  Query struct returned from a successfully prepared query. Its fields are:

    * `name` - The name of the prepared statement;
    * `statement` - The prepared statement;
    * `param_info` - List of oids, type info and extension for each parameter;
    * `param_formats` - List of formats for each parameters encoded to;
    * `encoders` - List of anonymous functions to encode each parameter;
    * `columns` - The column names;
    * `result_info` - List of oid, type info and extension for each column;
    * `result_formats` - List of formats for each column is decoded from;
    * `decoders` - List of anonymous functions to decode each column;
    * `types` - The type server table to fetch the type information from;
    * `null` - Atom to use as a stand in for postgres' `NULL`;
    * `copy_data` - Whether the query should send the final parameter as data to
    copy to the database;
  """

  @type t :: %__MODULE__{
    name:           iodata,
    statement:      iodata,
    param_info:     [{Postgrex.Types.oid, Postgrex.TypeInfo.t, module | nil}] | nil,
    param_formats:  [:binary | :text] | nil,
    encoders:       [(term -> iodata)] | nil,
    columns:        [String.t] | nil,
    result_info:    [{Postgrex.Types.oid, Postgrex.TypeInfo.t, module | nil}] | nil,
    result_formats: [:binary | :text] | nil,
    decoders:       [(binary -> term)] | nil,
    types:          Postgrex.TypeServer.table | nil,
    null:           atom,
    copy_data:      boolean}

  defstruct [:ref, :name, :statement, :param_info, :param_formats, :encoders,
    :columns, :result_info, :result_formats, :decoders, :types, :null,
    :copy_data]
end

defimpl DBConnection.Query, for: Postgrex.Query do
  import Postgrex.BinaryUtils
  require Postgrex.Messages

  def parse(%{name: name} = query, opts) do
    copy_data? = opts[:copy_data] || false
    # for query table to match names must be equal
    %{query | name: IO.iodata_to_binary(name), copy_data: copy_data?}
  end

  def describe(query, opts) do
    %Postgrex.Query{param_info: param_info, result_info: result_info,
                    types: types, null: conn_null, copy_data: data?} = query
    {pfs, encoders} = encoders(param_info, types)
    encoders = if data?, do: encoders ++ [:copy_data], else: encoders
    {rfs, decoders} = decoders(result_info, types)

    null = case Keyword.fetch(opts, :null) do
      {:ok, q_null} -> q_null
      :error -> conn_null
    end

    %Postgrex.Query{query | param_formats: pfs, encoders: encoders,
                            result_formats: rfs, decoders: decoders,
                            null: null}
  end

  def encode(%Postgrex.Query{types: nil} = query, _params, _) do
    raise ArgumentError, "query #{inspect query} has not been prepared"
  end

  def encode(query, params, _) do
    %Postgrex.Query{encoders: encoders, null: null, copy_data: data?} = query
    case do_encode(params || [], encoders, null, []) do
      :error when data? ->
        raise ArgumentError,
          "parameters must be of length #{length encoders}" <>
          " with copy data as final parameter for query #{inspect query}"
      :error ->
        raise ArgumentError,
          "parameters must be of length #{length encoders} for query #{inspect query}"
      params ->
       params
    end
  end

  def decode(%Postgrex.Query{decoders: nil}, res, opts) do
    case res do
      %Postgrex.Result{command: copy, rows: rows}
          when copy in [:copy, :copy_stream] and rows != nil ->
        %Postgrex.Result{res | rows: decode_copy(rows, opts)}
      _ ->
        res
    end
  end
  def decode(%Postgrex.Query{decoders: decoders, null: null}, res, opts) do
    mapper = opts[:decode_mapper] || fn x -> x end
    %Postgrex.Result{rows: rows} = res
    rows = do_decode(rows, decoders, null, mapper, [])
    %Postgrex.Result{res | rows: rows}
  end

  ## helpers

  defp encoders(oids, types) do
    oids
    |> Enum.map(&Postgrex.Types.encoder(&1, types))
    |> :lists.unzip()
  end

  defp decoders(nil, _) do
    {[], nil}
  end
  defp decoders(oids, types) do
    oids
    |> Enum.map(&Postgrex.Types.decoder(&1, types))
    |> :lists.unzip()
  end

  defp do_encode([copy_data | params], [:copy_data | encoders], null, encoded) do
    try do
      Postgrex.Messages.encode_msg(Postgrex.Messages.msg_copy_data(data: copy_data))
    else
      packet ->
        do_encode(params, encoders, null, [packet | encoded])
    rescue
      ArgumentError ->
        raise ArgumentError,
          "expected iodata to copy to database, got: " <> inspect(copy_data)
    end
  end
  defp do_encode([null | params], [_encoder | encoders], null, encoded) do
    do_encode(params, encoders, null, [<<-1::int32>> | encoded])
  end

  defp do_encode([param | params], [encoder | encoders], null, encoded) do
    param = encoder.(param)
    encoded = [[<<IO.iodata_length(param)::int32>> | param] | encoded]
    do_encode(params, encoders, null, encoded)
  end

  defp do_encode([], [], _, encoded), do: Enum.reverse(encoded)
  defp do_encode(params, _, _, _) when is_list(params), do: :error

  defp do_decode([row | rows], decoders, null, mapper, decoded) do
    decoded = [mapper.(decode_row(row, decoders, null, [])) | decoded]
    do_decode(rows, decoders, null, mapper, decoded)
  end
  defp do_decode([], _, _, _, decoded), do: decoded

  defp decode_row(<<-1 :: int32, rest :: binary>>, [_ | decoders], null, decoded) do
    decode_row(rest, decoders, null, [null | decoded])
  end
  defp decode_row(<<len :: uint32, value :: binary(len), rest :: binary>>, [decode | decoders], null, decoded) do
    decode_row(rest, decoders, null, [decode.(value) | decoded])
  end
  defp decode_row(<<>>, [], _, decoded), do: Enum.reverse(decoded)

  defp decode_copy(data, opts) do
    case opts[:decode_mapper] do
      nil    -> Enum.reverse(data)
      mapper -> decode_copy(data, mapper, [])
    end
  end

  defp decode_copy([row | data], mapper, decoded) do
    decode_copy(data, mapper, [mapper.(row) | decoded])
  end
  defp decode_copy([], _, decoded) do
    decoded
  end
end

defimpl String.Chars, for: Postgrex.Query do
  def to_string(%Postgrex.Query{statement: statement}) do
    IO.iodata_to_binary(statement)
  end
end
