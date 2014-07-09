defmodule Postgrex.Result do
  @moduledoc """
  Result struct returned from any successful query. Its fields are:

    * `command` - An atom of the query command, for example: `:select` or
                  `:insert`;
    * `columns` - The column names;
    * `rows` - The result set. A list of tuples, each tuple corresponding to a
               row, each element in the tuple corresponds to a column;
    * `num_rows` - The number of fetched or affected rows;
  """

  @type t :: %__MODULE__{
    command:  atom,
    columns:  [String.t] | nil,
    rows:     [tuple] | nil,
    num_rows: integer}

  defstruct [:command, :columns, :rows, :num_rows]
end

# TODO: Add "output" function name for easy text decoding?
defmodule Postgrex.TypeInfo do
  @moduledoc """
  The information about a type that is provided to the custom encoder/decoder
  functions.

    * `oid` - The type's [oid](http://www.postgresql.org/docs/9.3/static/datatype-oid.html);
    * `sender` - The name of the "sender" function (the function postgres uses
      to convert the type to binary format) without the "send" postfix. Useful
      to identify types that share a common binary format by a common name;
    * `type` - The type name;
    * `array_elem` - If the type is an array, the array elements' oid;
    * `comp_elems` - If the type is a composite type (record), the tuple
      elements' oid;
  """

  @type t :: %__MODULE__{
    oid:        pos_integer,
    sender:     String.t,
    type:       String.t,
    array_elem: pos_integer,
    comp_elems: [pos_integer]}

  defstruct [:oid, :sender, :type, :array_elem, :comp_elems]
end
