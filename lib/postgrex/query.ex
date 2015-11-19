defmodule Postgrex.Query do
  @moduledoc """
  Query struct returned from a successfully parsed query. Its fields are:

    * `name` - The name of the parsed statement;
    * `statement` - The parsed statement;
    * `params` - The parameters of the query.
    * `param_formats` - List of formats for each parameters encoded to;
    * `encoders` - List of anonymous functions to encode each parameter;
    * `columns` - The column names;
    * `result_formats` - List of formats for each column is decoded from;
    * `decoders` - List of anonymous functions to decode each column;
  """

  import Postgrex.BinaryUtils

  @type t :: %__MODULE__{
    name:           iodata,
    statement:      iodata,
    params:         [term] | nil,
    param_formats:  [:binary | :text] | nil,
    encoders:       [(term -> iodata)] | nil,
    columns:        [String.t] | nil,
    result_formats: [:binary | :text] | nil,
    decoders:       [(binary -> term)] | nil}

  defstruct [:name, :statement, :params, :param_formats, :encoders, :columns,
    :result_formats, :decoders]

  @doc """
  Encodes a parsed query..

  It is a no-op if the parameters are already encoded.

  A mapper function can be given to process each
  parameter before encoding, in no specific order.
  """
  @spec encode(t, (term -> term)) :: t
  def encode(query, mapper \\ fn x -> x end)

  def encode(%Postgrex.Query{params: [_|_], param_formats: nil}, _mapper) do
    raise ArgumentError, "parameters must be of length 0 for this query"
  end

  def encode(%Postgrex.Query{encoders: nil} = query, _mapper), do: query

  def encode(%Postgrex.Query{encoders: [_|_] = encoders, params: nil}, _mapper) do
    raise ArgumentError, "parameters must be of length #{length encoders} for this query"
  end

  def encode(query, mapper) do
    %Postgrex.Query{params: params, encoders: encoders} = query
    case encode_params(params, encoders, mapper, []) do
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
