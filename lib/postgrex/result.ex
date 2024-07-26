defmodule Postgrex.Result do
  @moduledoc """
  Result struct returned from any successful query. Its fields are:

    * `command` - An atom or a list of atoms of the query command, for example:
      `:select`, `:insert`, or `[:rollback, :release]`;
    * `columns` - The column names;
    * `rows` - The result set. A list of lists, each inner list corresponding to a
      row, each element in the inner list corresponds to a column;
    * `num_rows` - The number of fetched or affected rows;
    * `connection_id` - The OS pid of the PostgreSQL backend that executed the query;
    * `messages` - A list of maps of messages, such as hints and notices, sent by the
      driver during the execution of the query.
  """

  @type t :: %__MODULE__{
          command: atom | [atom],
          columns: [String.t()] | nil,
          rows: [[term] | binary] | nil,
          num_rows: integer,
          connection_id: pos_integer,
          messages: [map()]
        }

  defstruct [:command, :columns, :rows, :num_rows, :connection_id, messages: nil]
end

if Code.ensure_loaded?(Table.Reader) do
  defimpl Table.Reader, for: Postgrex.Result do
    def init(%{columns: columns}) when columns in [nil, []] do
      {:rows, %{columns: [], count: 0}, []}
    end

    def init(result) do
      {columns, _} =
        Enum.map_reduce(result.columns, %{}, fn column, counts ->
          counts = Map.update(counts, column, 1, &(&1 + 1))

          case counts[column] do
            1 -> {column, counts}
            n -> {"#{column}_#{n}", counts}
          end
        end)

      {:rows, %{columns: columns, count: result.num_rows}, result.rows}
    end
  end
end
