defmodule Postgrex.Result do
  @moduledoc """
  Result struct returned from any successful query. Its fields are:

    * `command` - An atom of the query command, for example: `:select` or
                  `:insert`;
    * `columns` - The column names;
    * `rows` - The result set. A list of tuples, each tuple corresponding to a
               row, each element in the tuple corresponds to a column;
    * `num_rows` - The number of fetched or affected rows;
    * `decoders` - List of anonymous functions to decode each column;
  """

  @type t :: %__MODULE__{
    command:  atom,
    columns:  [String.t] | nil,
    rows:     [[term] | term] | nil,
    num_rows: integer,
    decoders: [(term -> term)] | nil}

  defstruct [command: nil, columns: nil, rows: nil, num_rows: nil,
             decoders: nil]

  @doc """
  Decodes a result set.

  It is a no-op if the result was already decoded.

  A mapper function can be given to further process
  each row, in no specific order.
  """
  @spec decode(t, ([term] -> term)) :: t
  def decode(result_set, mapper \\ fn x -> x end)

  def decode(%Postgrex.Result{decoders: nil} = res, _mapper), do: res

  def decode(res, mapper) do
    %Postgrex.Result{rows: rows, decoders: decoders} = res
    rows = decode(rows, decoders, mapper, [])
    %Postgrex.Result{res | rows: rows, decoders: nil}
  end

  defp decode([row | rows], decoders, mapper, decoded) do
    decoded = [mapper.(decode_row(row, decoders)) | decoded]
    decode(rows, decoders, mapper, decoded)
  end
  defp decode([], _, _, decoded), do: decoded

  defp decode_row([nil | rest], [_ | decoders]) do
    [nil | decode_row(rest, decoders)]
  end
  defp decode_row([elem | rest], [decode | decoders]) do
    [decode.(elem) | decode_row(rest, decoders)]
  end
  defp decode_row([], []), do: []
end
