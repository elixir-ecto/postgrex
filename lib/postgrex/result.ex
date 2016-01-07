defmodule Postgrex.Result do
  @moduledoc """
  Result struct returned from any successful query. Its fields are:

    * `command` - An atom of the query command, for example: `:select` or
                  `:insert`;
    * `columns` - The column names;
    * `rows` - The result set. A list of tuples, each tuple corresponding to a
               row, each element in the tuple corresponds to a column;
    * `num_rows` - The number of fetched or affected rows;
    * `connection_id` - The OS pid of the Postgres backend that executed the query;
  """

  @type t :: %__MODULE__{
    command:  atom,
    columns:  [String.t] | nil,
    rows:     [[term] | binary] | nil,
    num_rows: integer,
    connection_id: pos_integer}

  defstruct [command: nil, columns: nil, rows: nil, num_rows: nil, connection_id: nil]
end
