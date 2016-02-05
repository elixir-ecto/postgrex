defmodule Postgrex.Query do
  @moduledoc """
  Query struct returned from a successfully prepared query. Its fields are:

    * `name` - The name of the prepared statement;
    * `statement` - The prepared statement;
    * `param_formats` - List of formats for each parameters encoded to;
    * `encoders` - List of anonymous functions to encode each parameter;
    * `columns` - The column names;
    * `result_formats` - List of formats for each column is decoded from;
    * `decoders` - List of anonymous functions to decode each column;
    * `types` - The type server table to fetch the type information from;
    * `null` - Atom to use as a stand in for postgres' `NULL`;
  """

  @type t :: %__MODULE__{
    name:           iodata,
    statement:      iodata,
    param_formats:  [:binary | :text] | nil,
    encoders:       [Postgrex.Types.oid] | [(term -> iodata)] | nil,
    columns:        [String.t] | nil,
    result_formats: [:binary | :text] | nil,
    decoders:       [Postgrex.Types.oid] | [(binary -> term)] | nil,
    types:          Postgrex.TypeServer.table | nil,
    null:           atom}

  defstruct [:name, :statement, :param_formats, :encoders, :columns,
    :result_formats, :decoders, :types, :null]
end

defimpl DBConnection.Query, for: Postgrex.Query do
  import Postgrex.BinaryUtils

  def parse(%{name: name, statement: statement} = query, _) do
    # for query table to match on two identical statements they must be equal
    %{query | name: IO.iodata_to_binary(name),
      statement: IO.iodata_to_binary(statement)}
  end

  def describe(query, opts) do
    %Postgrex.Query{encoders: poids, decoders: roids,
                    types: types, null: conn_null} = query
    {pfs, encoders} = encoders(poids, types)
    {rfs, decoders} = decoders(roids, types)

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

  def encode(%Postgrex.Query{encoders: encoders, null: null} = query, params, _) do
    case do_encode(params || [], encoders, null, []) do
      :error ->
        raise ArgumentError,
        "parameters must be of length #{length encoders} for query #{inspect query}"
      params ->
       params
    end
  end

  def decode(%Postgrex.Query{decoders: nil}, res, _), do: res
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
end

defimpl String.Chars, for: Postgrex.Query do
  def to_string(%Postgrex.Query{statement: statement}) do
    IO.iodata_to_binary(statement)
  end
end
