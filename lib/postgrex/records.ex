defrecord Postgrex.Result, [:command, :columns, :rows, :num_rows] do
  @moduledoc """
  Result record returned from any successful query. Its fields are:

    * `command` - An atom of the query command, for example: `:select` or
                  `:insert`.
    * `columns` - The column names.
    * `rows` - The result set. A list of tuples, each tuple corresponding to a
               row, each element in the tuple corresponds to a column.
    * `num_rows` - The number of fetched or affected rows.
  """

  record_type [
    command: atom,
    columns: [String.t] | nil,
    rows: [tuple] | nil,
    num_rows: integer ]
end

defexception Postgrex.Error, [:postgres, :reason] do
  def message(Postgrex.Error[postgres: kw]) when is_list(kw) do
    "#{kw[:severity]} (#{kw[:code]}): #{kw[:message]}"
  end

  def message(Postgrex.Error[reason: msg]) do
    msg
  end
end
