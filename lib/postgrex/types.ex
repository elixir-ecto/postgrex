defmodule Postgrex.Types do
  @moduledoc """
  Encodes and decodes between Postgres' protocol and Elixir values.
  """

  alias Postgrex.TypeInfo
  import Postgrex.BinaryUtils

  @typedoc """
  Postgres internal identifier that maps to a type. See
  http://www.postgresql.org/docs/9.4/static/datatype-oid.html.
  """
  @type oid :: pos_integer

  @typedoc """
  State used by the encoder/decoder functions
  """
  @opaque state :: {module, :ets.tid}

  @typedoc """
  Term used to describe type information
  """
  @opaque type :: module | {module, [oid], [type]} | {module, nil, state}

  ### BOOTSTRAP TYPES AND EXTENSIONS ###

  @doc false
  @spec new(module) :: state
  def new(module) do
    {module, :ets.new(__MODULE__, [:protected, {:read_concurrency, true}])}
  end

  @doc false
  @spec owner(state) :: {:ok, pid} | :error
  def owner({_, table}) do
    case :ets.info(table, :owner) do
      owner when is_pid(owner) ->
        {:ok, owner}
      :undefined ->
        :error
    end
  end

  @doc false
  @spec bootstrap_query({pos_integer, non_neg_integer, non_neg_integer}, state) :: binary
  def bootstrap_query(version, {_, table}) do
    oids = :ets.select(table, [{{:"$1", :_, :_}, [], [:"$1"]}])

    {typelem, join_domain} =
      if version >= {9, 0, 0} do
        {"coalesce(d.typelem, t.typelem)",
         "LEFT JOIN pg_type AS d ON t.typbasetype = d.oid"}
      else
        {"t.typelem", ""}
      end

    {rngsubtype, join_range} =
      if version >= {9, 2, 0} do
        {"coalesce(r.rngsubtype, 0)",
         "LEFT JOIN pg_range AS r ON r.rngtypid = t.oid OR (t.typbasetype <> 0 AND r.rngtypid = t.typbasetype)"}
      else
        {"0", ""}
      end

    filter_oids =
      case oids do
        [] ->
          ""
        _  ->
          # equiv to `WHERE t.oid NOT IN (SELECT unnest(ARRAY[#{Enum.join(oids, ",")}]))`
          # `unnest` is not supported in redshift or postgres version prior to 8.4
          """
          WHERE t.oid NOT IN (
            SELECT (ARRAY[#{Enum.join(oids, ",")}])[i]
            FROM generate_series(1, #{length(oids)}) AS i
          )
          """
      end

    """
    SELECT t.oid, t.typname, t.typsend, t.typreceive, t.typoutput, t.typinput,
           #{typelem}, #{rngsubtype}, ARRAY (
      SELECT a.atttypid
      FROM pg_attribute AS a
      WHERE a.attrelid = t.typrelid AND a.attnum > 0 AND NOT a.attisdropped
      ORDER BY a.attnum
    )
    FROM pg_type AS t
    #{join_domain}
    #{join_range}
    #{filter_oids}
    """
  end

  @doc false
  @spec build_type_info(binary) :: TypeInfo.t
  def build_type_info(row) do
    [oid,
      type,
      send,
      receive,
      output,
      input,
      array_oid,
      base_oid,
      comp_oids] = row_decode(row)
    oid = String.to_integer(oid)
    array_oid = String.to_integer(array_oid)
    base_oid = String.to_integer(base_oid)
    comp_oids = parse_oids(comp_oids)

    %TypeInfo{
      oid: oid,
      type: :binary.copy(type),
      send: :binary.copy(send),
      receive: :binary.copy(receive),
      output: :binary.copy(output),
      input: :binary.copy(input),
      array_elem: array_oid,
      base_type: base_oid,
      comp_elems: comp_oids}
  end

  @doc false
  @spec associate_type_infos([TypeInfo.t], state) :: :ok
  def associate_type_infos(type_infos, {module, table}) do
    _ = for %TypeInfo{oid: oid} = type_info <- type_infos do
      true = :ets.insert_new(table, {oid, type_info, nil})
    end
    _ = for %TypeInfo{oid: oid} = type_info <- type_infos do
      info = find(type_info, :any, module, table)
      true = :ets.update_element(table, oid, {3, info})
    end
    :ok
  end

  defp find(type_info, formats, module, table) do
    case apply(module, :find, [type_info, formats]) do
      {:super_binary, extension, nil} ->
        {:binary, {extension, nil, {module, table}}}
      {:super_binary, extension, sub_oids} when formats == :any ->
        super_find(sub_oids, extension, module, table) ||
          find(type_info, :text, module, table)
      {:super_binary, extension, sub_oids} ->
        super_find(sub_oids, extension, module, table)
      nil ->
        nil
      info ->
        info
    end
  end

  defp super_find(sub_oids, extension, module, table) do
    case sub_find(sub_oids, module, table, []) do
      {:ok, sub_types} ->
        {:binary, {extension, sub_oids, sub_types}}
      :error ->
        nil
    end
  end

  defp sub_find([oid | oids], module, table, acc) do
    case :ets.lookup(table, oid) do
      [{_, _, {:binary, types}}] ->
        sub_find(oids, module, table, [types | acc])
      [{_, type_info, _}] ->
        case find(type_info, :binary, module, table) do
          {:binary, types} ->
            sub_find(oids, module, table, [types | acc])
          nil ->
            :error
        end
      [] ->
        :error
    end
  end
  defp sub_find([], _, _, acc) do
    {:ok, Enum.reverse(acc)}
  end

  defp row_decode(<<>>), do: []
  defp row_decode(<<-1::int32, rest::binary>>) do
    [nil | row_decode(rest)]
  end
  defp row_decode(<<len::uint32, value::binary(len), rest::binary>>) do
    [value | row_decode(rest)]
  end

  defp parse_oids(nil) do
    []
  end

  defp parse_oids("{}") do
    []
  end

  defp parse_oids("{" <> rest) do
    parse_oids(rest, [])
  end

  defp parse_oids(bin, acc) do
    case Integer.parse(bin) do
      {int, "," <> rest} -> parse_oids(rest, [int|acc])
      {int, "}"}         -> Enum.reverse([int|acc])
    end
  end

  ### TYPE ENCODING / DECODING ###

  @doc """
  Defines a type module with custom extensions and options.

  `Postgrex.Types.define/3` must be called on its own file, outside of
  any module and function, as it only needs to be defined once during
  compilation.

  Type modules are given to Postgrex on `start_link` via the `:types`
  option and are used to control how Postgrex encodes and decodes data
  coming from Postgrex.

  For example, to define a new type module with a custom extension
  called `MyExtension` while also changing `Postgrex`'s default
  behaviour regarding binary decoding, you may create a new file
  called "lib/my_app/postgrex_types.ex" with the following:

      Postgrex.Types.define(MyApp.PostgrexTypes, [MyExtension], [decode_binary: :reference])

  The line above will define a new module, called `MyApp.PostgrexTypes`
  which can be passed as `:types` when starting Postgrex. The type module
  works by rewriting and inlining the extensions' encode and decode
  expressions in an optimal fashion for postgrex to encode parameters and
  decode multiple rows at a time.

  ## Extensions

  Extensions is a list of `Postgrex.Extension` modules or a 2-tuple
  containing the module and a keyword list. The keyword, defaulting
  to `[]`, will be passed to the modules `init/1` callback.

  Extensions at the front of the list will take priority over later
  extensions when the `matching/1` callback returns have conflicting
  matches. If an extension is not provided for a type then Postgrex
  will fallback to default encoding/decoding methods where possible.

  See `Postgrex.Extension` for more information on extensions.

  ## Options

    * `:null` - The atom to use as a stand in for postgres' `NULL` in
      encoding and decoding. The module attribute `@null` is registered
      with the value so that extension can access the value if desired
      (default: `nil`);

    * `:decode_binary` - Either `:copy` to copy binary values when decoding
      with default extensions that return binaries or `:reference` to use a
      reference counted binary of the binary received from the socket.
      Referencing a potentially larger binary can be more efficient if the binary
      value is going to be garbaged collected soon because a copy is avoided.
      However the larger binary can not be garbage collected until all references
      are garbage collected (default: `:copy`);

    * `:date` - The default extensions date handling mode: `:elixir` to use
      Elixir date structs or `:postgrex` to use the deprecated `:postgrex`
      structs (default: `:elixir`);

    * `:json` - The JSON module to encode and decode JSON binaries, calls
      `module.encode!/1` to encode and `module.decode!/1` to decode. If `nil`
      then no default JSON handling (default: `nil`);

    * `:bin_opt_info` - Either `true` to enable binary optimisation information,
      or `false` to disable, for more information see `Kernel.SpecialForms.<<>>/1`
      in Elixir (default: `false`);

    * `:debug_defaults` - Generate debug information when building default
      extensions so they point to the proper source. Enabling such option
      will increase the time to compile the type module (default: `false`);

  """
  def define(module, extensions, opts \\ []) do
    Postgrex.TypeModule.define(module, extensions, opts)
  end

  @doc false
  @spec encode_params([term], [type], state) :: iodata | :error
  def encode_params(params, types, {mod, _}) do
    apply(mod, :encode_params, [params, types])
  end

  @doc false
  @spec decode_rows(binary, [type], [row], state) ::
    {:more, iodata, [row], non_neg_integer} | {:ok, [row], binary} when row: var
  def decode_rows(binary, types, rows, {mod, _}) do
    apply(mod, :decode_rows, [binary, types, rows])
  end

  @doc false
  @spec fetch(oid, state) ::
    {:ok, {:binary | :text, type}} | {:error, TypeInfo.t | nil, module}
  def fetch(oid, {mod, table}) do
    try do
      :ets.lookup_element(table, oid, 3)
    rescue
      ArgumentError ->
        {:error, nil, mod}
    else
      {_, _} = info ->
        {:ok, info}
      nil ->
        fetch_type_info(oid, mod, table)
    end
  end

  defp fetch_type_info(oid, mod, table) do
    try do
      :ets.lookup_element(table, oid, 2)
    rescue
      ArgumentError ->
        {:error, nil, mod}
    else
      type_info ->
        {:error, type_info, mod}
    end
  end
end
