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
    * `types` - The type serber table to fetch the type information from;
  """

  @type t :: %__MODULE__{
    name:           iodata,
    statement:      iodata,
    param_formats:  [:binary | :text] | nil,
    encoders:       [Postgrex.Types.oid] | [(term -> iodata)] | nil,
    columns:        [String.t] | nil,
    result_formats: [:binary | :text] | nil,
    decoders:       [Postgrex.Types.oid] | [(binary -> term)] | nil,
    types:          Postgrex.TypeServer.table | nil}

  defstruct [:name, :statement, :param_formats, :encoders, :columns,
    :result_formats, :decoders, :types]
end

defimpl DBConnection.Query, for: Postgrex.Query do

  import Postgrex.BinaryUtils

  def parse(query, _), do: query

  def describe(query, _) do
    %Postgrex.Query{encoders: poids, decoders: roids, types: types} = query
    {pfs, encoders} = encoders(poids, types)
    {rfs, decoders} = decoders(roids, types)
    %Postgrex.Query{query | param_formats: pfs, encoders: encoders,
                            result_formats: rfs, decoders: decoders}
  end

  def encode(%Postgrex.Query{types: nil} = query, _params, _mapper) do
    raise ArgumentError, "query #{inspect query} has not been prepared"
  end

  def encode(%Postgrex.Query{encoders: encoders} = query, params, opts) do
    mapper = opts[:encode_mapper] || fn x -> x end
    case encode(params || [], encoders, mapper, []) do
      :error ->
        raise ArgumentError,
        "parameters must be of length #{length encoders} for query #{inspect query}"
      params ->
       params
    end
  end

  def decode(%Postgrex.Query{decoders: nil}, res, _), do: res
  def decode(%Postgrex.Query{decoders: decoders}, res, opts) do
    mapper = opts[:decode_mapper] || fn x -> x end
    %Postgrex.Result{rows: rows} = res
    rows = decode(rows, decoders, mapper, [])
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

 defp encode([param | params], [encoder | encoders], mapper, encoded) do
    case mapper.(param) do
      nil   ->
        encode(params, encoders, mapper, [<<-1::int32>> | encoded])
      param ->
        param = encoder.(param)
        encoded = [[<<IO.iodata_length(param)::int32>> | param] | encoded]
        encode(params, encoders, mapper, encoded)
    end
  end
  defp encode([], [], _, encoded), do: Enum.reverse(encoded)
  defp encode(params, _, _, _) when is_list(params), do: :error

  defp decode([row | rows], decoders, mapper, decoded) do
    decoded = [mapper.(decode_row(row, decoders, [])) | decoded]
    decode(rows, decoders, mapper, decoded)
  end
  defp decode([], _, _, decoded), do: decoded

  defp decode_row([nil | rest], [_ | decoders], decoded) do
    decode_row(rest, decoders, [nil | decoded])
  end
  defp decode_row([elem | rest], [decode | decoders], decoded) do
    decode_row(rest, decoders, [decode.(elem) | decoded])
  end
  defp decode_row([], [], decoded), do: Enum.reverse(decoded)
end

defimpl String.Chars, for: Postgrex.Query do
  def to_string(%Postgrex.Query{statement: statement}) do
    IO.iodata_to_binary(statement)
  end
end
