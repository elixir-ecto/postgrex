defmodule Postgrex.Query do
  @moduledoc """
  Query struct returned from a successfully prepared query.

  Its public fields are:

    * `name` - The name of the prepared statement;
    * `statement` - The prepared statement;
    * `columns` - The column names;
  """

  @type t :: %__MODULE__{
    ref:            reference | nil,
    name:           iodata,
    statement:      iodata,
    param_oids:     [Postgrex.Types.oid] | nil,
    param_formats:  [:binary | :text] | nil,
    param_types:    [Postgrex.Types.type] | nil,
    columns:        [String.t] | nil,
    result_oids:    [Postgrex.Types.oid] | nil,
    result_formats: [:binary | :text] | nil,
    result_types:   [Postgrex.Types.type] | nil,
    types:          Postgrex.Types.state | nil}

  defstruct [:ref, :name, :statement, :param_oids, :param_formats, :param_types,
    :columns, :result_oids, :result_formats, :result_types, :types]
end

defimpl DBConnection.Query, for: Postgrex.Query do
  require Postgrex.Messages

  def parse(%{name: name} = query, _) do
    # for query table to match names must be equal
    %{query | name: IO.iodata_to_binary(name)}
  end

  def describe(query, _), do: query

  def encode(%Postgrex.Query{types: nil} = query, _params, _) do
    raise ArgumentError, "query #{inspect query} has not been prepared"
  end

  def encode(query, params, _) do
    %Postgrex.Query{param_types: param_types, types: types} = query
    case Postgrex.Types.encode_params(params, param_types, types) do
      encoded when is_list(encoded) ->
        encoded
      :error ->
        raise ArgumentError,
          "parameters must be of length #{length param_types} for query #{inspect query}"
    end
  end

  def decode(%Postgrex.Query{result_types: nil}, res, opts) do
    case res do
      %Postgrex.Result{command: copy, rows: rows}
          when copy in [:copy, :copy_stream] and rows != nil ->
        %Postgrex.Result{res | rows: decode_copy(rows, opts)}
      _ ->
        res
    end
  end
  def decode(%Postgrex.Query{result_types: result_types, types: types}, res,
             opts) do
    mapper = opts[:decode_mapper] || fn x -> x end
    %Postgrex.Result{rows: rows} = res
    rows = decode_rows(rows, result_types, types, mapper, [])
    %Postgrex.Result{res | rows: rows}
  end

  ## Helpers

  defp decode_rows([row | rows], result_types, types, mapper, decoded) do
    row =
      row
      |> Postgrex.Types.decode_row(result_types, types)
      |> mapper.()
    decode_rows(rows, result_types, types, mapper, [row | decoded])
  end
  defp decode_rows([], _, _, _, decoded), do: decoded

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
