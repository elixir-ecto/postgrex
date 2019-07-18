defmodule Postgrex.Result do
  @moduledoc """
  Result struct returned from any successful query. Its fields are:

    * `command` - An atom of the query command, for example: `:select` or `:insert`;
    * `columns` - The column names;
    * `rows` - The result set. A list of lists, each inner list corresponding to a
      row, each element in the inner list corresponds to a column;
    * `num_rows` - The number of fetched or affected rows;
    * `connection_id` - The OS pid of the PostgreSQL backend that executed the query;
    * `messages` - A list of maps of messages, such as hints and notices, sent by the
      the driver during the execution of the query
  """

  @type t :: %__MODULE__{
          command: atom,
          columns: [String.t()] | nil,
          rows: [[term] | binary] | nil,
          num_rows: integer,
          connection_id: pos_integer,
          messages: [map()]
        }

  defstruct [:command, :columns, :rows, :num_rows, :connection_id, messages: nil]
end
