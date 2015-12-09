defmodule Postgrex.Query do
  @moduledoc """
  Query struct returned from a successfully prepared query. Its fields are:

    * `name` - The name of the prepared statement;
    * `statement` - The prepared statement;
    * `params` - The parameters of the query.
    * `param_formats` - List of formats for each parameters encoded to;
    * `encoders` - List of anonymous functions to encode each parameter;
    * `columns` - The column names;
    * `result_formats` - List of formats for each column is decoded from;
    * `decoders` - List of anonymous functions to decode each column;
    * `types` - The type serber table to fetch the type information from;
  """

  import Postgrex.BinaryUtils

  @type t :: %__MODULE__{
    name:           iodata,
    statement:      iodata,
    params:         [term] | nil,
    param_formats:  [:binary | :text] | nil,
    encoders:       [Postgrex.Types.oid] | [(term -> iodata)] | nil,
    columns:        [String.t] | nil,
    result_formats: [:binary | :text] | nil,
    decoders:       [Postgrex.Types.oid] | [(binary -> term)] | nil,
    types:          Postgrex.TypeServer.table | nil}

  defstruct [:name, :statement, :params, :param_formats, :encoders, :columns,
    :result_formats, :decoders, :types]

  @doc """
  Encodes a prepared query.

  It is a no-op if the parameters are already encoded.

  A mapper function can be given to process each
  parameter before encoding, in no specific order.
  """
  @spec encode(t, (term -> term)) :: t
  def encode(query, mapper \\ fn x -> x end)

  def encode(%Postgrex.Query{param_formats: nil, types: nil} = query, _mapper) do
    raise ArgumentError, "query #{inspect query} has not been prepared"
  end

  def encode(%Postgrex.Query{param_formats: nil} = query, _mapper) do
    raise ArgumentError, "query #{inspect query} has not been described"
  end

  def encode(%Postgrex.Query{encoders: nil} = query, _mapper), do: query

  def encode(query, mapper) do
    %Postgrex.Query{params: params, encoders: encoders} = query
    case encode_params(params || [], encoders, mapper, []) do
      :error ->
        raise ArgumentError, "parameters must be of length #{length encoders} for this query"
      params ->
       %Postgrex.Query{query | params: params, encoders: nil}
    end
  end

  ## helpers

  defp encode_params([param | params], [encoder | encoders], mapper, encoded) do
    case mapper.(param) do
      nil   ->
        encode_params(params, encoders, mapper, [<<-1::int32>> | encoded])
      param ->
        param = encoder.(param)
        encoded = [[<<IO.iodata_length(param)::int32>> | param] | encoded]
        encode_params(params, encoders, mapper, encoded)
    end
  end
  defp encode_params([], [], _, encoded), do: Enum.reverse(encoded)
  defp encode_params(params, _, _, _) when is_list(params), do: :error
end

defimpl DBConnection.Query, for: Postgrex.Query do

  def parse(query, _), do: query

  def describe(%Postgrex.Query{param_formats: nil, types: nil} = query, _) do
    raise ArgumentError, "query #{inspect query} has not been prepared"
  end
  def describe(%Postgrex.Query{encoders: encoders, types: nil} = query, _)
  when is_list(encoders) do
    query
  end
  def describe(query, _) do
    %Postgrex.Query{encoders: poids, decoders: roids, types: types} = query
    {pfs, encoders} = encoders(poids, types)
    {rfs, decoders} = decoders(roids, types)
    %Postgrex.Query{query | param_formats: pfs, encoders: encoders,
                            result_formats: rfs, decoders: decoders,
                            types: nil}
  end

  def encode(query, opts) do
    case opts[:encode] || :auto do
      :auto   -> Postgrex.Query.encode(query)
      :manual -> query
    end
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
end
