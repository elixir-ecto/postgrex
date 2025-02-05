defmodule Postgrex.Query do
  @moduledoc """
  Query struct returned from a successfully prepared query.

  Its public fields are:

    * `name` - The name of the prepared statement;
    * `statement` - The prepared statement;
    * `columns` - The column names;
    * `ref` - A reference used to identify prepared queries;

  ## Prepared queries

  Once a query is prepared with `Postgrex.prepare/4`, the
  returned query will have its `ref` field set to a reference.
  When `Postgrex.execute/4` is called with the prepared query,
  it always returns a query. If the `ref` field in the query
  given to `execute` and the one returned are the same, it
  means the cached prepared query was used. If the `ref` field
  is not the same, it means the query had to be re-prepared.
  """

  @type t :: %__MODULE__{
          cache: :reference | :statement,
          ref: reference | nil,
          name: iodata,
          statement: iodata,
          param_oids: [Postgrex.Types.oid()] | nil,
          param_formats: [:binary | :text] | nil,
          param_types: [Postgrex.Types.type()] | nil,
          columns: [String.t()] | nil,
          result_oids: [Postgrex.Types.oid()] | nil,
          result_formats: [:binary | :text] | nil,
          result_types: [Postgrex.Types.type()] | nil,
          types: Postgrex.Types.state() | nil
        }

  defstruct [
    :ref,
    :name,
    :statement,
    :param_oids,
    :param_formats,
    :param_types,
    :columns,
    :result_oids,
    :result_formats,
    :result_types,
    :types,
    cache: :reference
  ]
end

defimpl DBConnection.Query, for: Postgrex.Query do
  require Postgrex.Messages

  def parse(%{types: nil, name: name} = query, _) do
    # for query table to match names must be equal
    %{query | name: IO.iodata_to_binary(name)}
  end

  def parse(query, _) do
    raise ArgumentError, "query #{inspect(query)} has already been prepared"
  end

  def describe(query, _), do: query

  def encode(%{types: nil} = query, _params, _) do
    raise ArgumentError, "query #{inspect(query)} has not been prepared"
  end

  def encode(query, params, _) do
    %{param_types: param_types, types: types} = query

    case Postgrex.Types.encode_params(params, param_types, types) do
      encoded when is_list(encoded) ->
        encoded

      :error ->
        raise ArgumentError,
              "parameters must be of length #{length(param_types)} for query #{inspect(query)}"
    end
  end

  def decode(_, %Postgrex.Result{rows: nil} = res, _opts) do
    res
  end

  def decode(_, %Postgrex.Result{rows: rows} = res, opts) do
    %{res | rows: decode_map(rows, opts)}
  end

  def decode(_, %Postgrex.Copy{} = copy, _opts) do
    copy
  end

  ## Helpers

  defp decode_map(data, opts) do
    case opts[:decode_mapper] do
      nil -> Enum.reverse(data)
      mapper -> decode_map(data, mapper, [])
    end
  end

  defp decode_map([row | data], mapper, decoded) do
    decode_map(data, mapper, [mapper.(row) | decoded])
  end

  defp decode_map([], _, decoded) do
    decoded
  end
end

defimpl String.Chars, for: Postgrex.Query do
  def to_string(%Postgrex.Query{statement: statement}) do
    IO.iodata_to_binary(statement)
  end
end
