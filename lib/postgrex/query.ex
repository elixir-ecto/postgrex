defmodule Postgrex.Query do
  @moduledoc """
  Query struct returned from a successfully prepared query. Its fields are:

    * `name` - The name of the prepared statement;
    * `statement` - The prepared statement;
    * `param_info` - List of oids, type info, extension and options for each parameter;
    * `param_formats` - List of formats for each parameters encoded to;
    * `columns` - The column names;
    * `result_info` - List of oid, type info, extension and options for each column;
    * `result_formats` - List of formats for each column is decoded from;
    * `types` - The type server table to fetch the type information from;
    * `null` - Atom to use as a stand in for postgres' `NULL`;
  """

  @type t :: %__MODULE__{
    name:           iodata,
    statement:      iodata,
    param_info:     [type_info] | nil,
    param_formats:  [:binary | :text] | nil,
    columns:        [String.t] | nil,
    result_info:    [type_info] | nil,
    result_formats: [:binary | :text] | nil,
    types:          Postgrex.TypeServer.table | nil,
    null:           atom}

  @type type_info ::
    {Postgrex.Types.oid, Postgrex.TypeInfo.t, module | nil} |
    {Postgrex.Types.oid, Postgrex.TypeInfo.t, module, any}

  defstruct [:ref, :name, :statement, :param_info, :param_formats, :columns,
    :result_info, :result_formats, :types, :null]
end

defimpl DBConnection.Query, for: Postgrex.Query do
  import Postgrex.BinaryUtils
  require Postgrex.Messages

  def parse(%{name: name} = query, _) do
    # for query table to match names must be equal
    %{query | name: IO.iodata_to_binary(name)}
  end

  def describe(query, opts) do
    %Postgrex.Query{param_info: param_info, result_info: result_info,
                    types: types, null: conn_null} = query
    {pfs, param_info} = param_opts(param_info, types)
    {rfs, result_info} = result_opts(result_info, types)

    null = case Keyword.fetch(opts, :null) do
      {:ok, q_null} -> q_null
      :error -> conn_null
    end

    %Postgrex.Query{query | param_formats: pfs, param_info: param_info,
                            result_formats: rfs, result_info: result_info,
                            null: null}
  end

  def encode(%Postgrex.Query{types: nil} = query, _params, _) do
    raise ArgumentError, "query #{inspect query} has not been prepared"
  end

  def encode(query, params, _) do
    %Postgrex.Query{param_info: param_info, null: null, types: types} = query
    case do_encode(params || [], param_info, null, types, []) do
      params when is_list(params) ->
        params
      :error ->
        raise ArgumentError,
          "parameters must be of length #{length param_info} for query #{inspect query}"
    end
  end

  def decode(%Postgrex.Query{result_info: nil}, res, opts) do
    case res do
      %Postgrex.Result{command: copy, rows: rows}
          when copy in [:copy, :copy_stream] and rows != nil ->
        %Postgrex.Result{res | rows: decode_copy(rows, opts)}
      _ ->
        res
    end
  end
  def decode(%Postgrex.Query{result_info: result_info, null: null, types: types}, res, opts) do
    mapper = opts[:decode_mapper] || fn x -> x end
    %Postgrex.Result{rows: rows} = res
    rows = do_decode(rows, result_info, null, types, mapper, [])
    %Postgrex.Result{res | rows: rows}
  end

  ## helpers

  defp param_opts(param_info, types) do
    param_info
    |> Enum.map(&Postgrex.Types.param_opts(&1, types))
    |> :lists.unzip()
  end

  defp result_opts(nil, _) do
    {[], nil}
  end
  defp result_opts(result_info, types) do
    result_info
    |> Enum.map(&Postgrex.Types.result_opts(&1, types))
    |> :lists.unzip()
  end

  defp do_encode([null | params], [_ | param_info], null, types, encoded) do
    do_encode(params, param_info, null, types, [<<-1::int32>> | encoded])
  end

  defp do_encode([param | params], [{_, info, ext, opts} | param_info], null, types, encoded) do
    param = apply(ext, :encode, [info, param, types, opts])
    encoded = [[<<IO.iodata_length(param)::int32>> | param] | encoded]
    do_encode(params, param_info, null, types, encoded)
  end

  defp do_encode([], [], _, _, encoded), do: Enum.reverse(encoded)
  defp do_encode(params, _, _, _, _) when is_list(params), do: :error

  defp do_decode([row | rows], result_info, null, types, mapper, decoded) do
    decoded = [mapper.(decode_row(row, result_info, null, types, [])) | decoded]
    do_decode(rows, result_info, null, types, mapper, decoded)
  end
  defp do_decode([], _, _, _, _, decoded), do: decoded

  defp decode_row(<<-1 :: int32, rest :: binary>>, [_ | result_info], null, types, decoded) do
    decode_row(rest, result_info, null, types, [null | decoded])
  end
  defp decode_row(<<len :: uint32, value :: binary(len), rest :: binary>>, [{_, info, ext, opts} | result_info], null, types, decoded) do
    value = apply(ext, :decode, [info, value, types, opts])
    decode_row(rest, result_info, null, types, [value | decoded])
  end
  defp decode_row(<<>>, [], _, _, decoded), do: Enum.reverse(decoded)

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
