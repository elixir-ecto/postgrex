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

  defstruct [
    command: nil :: atom,
    columns: nil :: [String.t] | nil,
    rows: nil :: [tuple] | nil,
    num_rows: nil :: integer ]
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

  defstruct [
    oid: nil :: pos_integer,
    sender: nil :: String.t,
    type: nil :: String.t,
    array_elem: nil :: pos_integer,
    comp_elems: nil :: [pos_integer] ]
end
