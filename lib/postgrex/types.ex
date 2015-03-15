defmodule Postgrex.Types do
  @moduledoc """
  Encodes and decodes between Postgres' protocol and Elixir values.
  """

  alias Postgrex.TypeInfo
  alias Postgrex.Extension
  import Postgrex.BinaryUtils

  @typedoc """
  Postgres internal identifier that maps to a type. See
  http://www.postgresql.org/docs/9.4/static/datatype-oid.html.
  """
  @type oid :: pos_integer

  @typedoc """
  State used by the encoder/decoder functions
  """
  @opaque state :: {HashDict.t, HashDict.t}

  @higher_types ["array_send", "range_send", "record_send"]

  ### BOOTSTRAP TYPES AND EXTENSIONS ###

  @doc false
  def bootstrap_query(m, version) do
    if version >= 90_200 do
      rngsubtype = "coalesce(r.rngsubtype, 0)"
      join_range = "LEFT JOIN pg_range AS r ON r.rngtypid = t.oid"
    else
      rngsubtype = "0"
      join_range = ""
    end

    """
    SELECT t.oid, t.typname, t.typsend, t.typreceive, t.typoutput, t.typinput,
           t.typelem, #{rngsubtype}, ARRAY (
      SELECT a.atttypid
      FROM pg_attribute AS a
      WHERE a.attrelid = t.typrelid AND a.attnum > 0 AND NOT a.attisdropped
      ORDER BY a.attnum
    )
    FROM pg_type AS t
    #{join_range}
    WHERE
      t.typname::text = ANY ((#{sql_array(m.type)})::text[]) OR
      t.typsend::text = ANY ((#{sql_array(m.send)})::text[]) OR
      t.typreceive::text = ANY ((#{sql_array(m.receive)})::text[]) OR
      t.typoutput::text = ANY ((#{sql_array(m.output)})::text[]) OR
      t.typinput::text = ANY ((#{sql_array(m.input)})::text[])
    """
  end

  @doc false
  def prepare_extensions(extensions, parameters) do
    parameters = Map.put(parameters, "server_version", "8.4.0")
    Enum.into(extensions, HashDict.new, fn {extension, opts} ->
      {extension, extension.init(parameters, opts)}
    end)
  end

  @doc false
  def extension_matchers(extensions, extension_opts) do
    map = %{type: [], send: [], receive: [], output: [], input: []}
    map =
      Enum.reduce(extensions, map, fn extension, map ->
        opts = HashDict.fetch!(extension_opts, extension)
        Enum.reduce(extension.matching(opts), map, fn {key, value}, map ->
          Map.update!(map, key, &[value|&1])
        end)
      end)
    Map.update!(map, :send, &(@higher_types ++ &1))
  end

  @doc false
  def build_types(rows) do
    Enum.map(rows, fn row ->
      [<<_::int32, oid::binary>>,
       <<_::int32, type::binary>>,
       <<_::int32, send::binary>>,
       <<_::int32, receive::binary>>,
       <<_::int32, output::binary>>,
       <<_::int32, input::binary>>,
       <<_::int32, array_oid::binary>>,
       <<_::int32, base_oid::binary>>,
       <<_::int32, comp_oids::binary>>] = row
      oid = String.to_integer(oid)
      array_oid = String.to_integer(array_oid)
      base_oid = String.to_integer(base_oid)
      comp_oids = parse_oids(comp_oids)

      %TypeInfo{
        oid: oid,
        type: type,
        send: send,
        receive: receive,
        output: output,
        input: input,
        array_elem: array_oid,
        base_type: base_oid,
        comp_elems: comp_oids}
    end)
  end

  @doc false
  def associate_extensions_with_types(extensions, extension_opts, types) do
    oid_types = Enum.into(types, HashDict.new, &{&1.oid, &1})

    Enum.reduce(types, HashDict.new, fn type_info, dict ->
      extension = find_extension(type_info, extensions, extension_opts, oid_types)

      if extension do
        HashDict.put(dict, type_info.oid, {type_info, extension})
      else
        dict
      end
    end)
  end

  defp find_extension(nil, _extensions, _extension_opts, _types) do
    nil
  end

  defp find_extension(type_info, extensions, extension_opts, types) do
    Enum.find(extensions, fn extension ->
      opts = HashDict.fetch!(extension_opts, extension)
      match_extension_against_type(extension, opts, type_info)
    end)
    || find_superextension_for_type(type_info, extensions, extension_opts, types)
  end

  defp match_extension_against_type(extension, opts, type_info) do
    matching = extension.matching(opts)
    Enum.any?(matching, &match_type(&1, type_info))
  end

  defp match_type({field, value}, type_info) do
    case Map.fetch(type_info, field) do
      {:ok, ^value} -> true
      _ -> false
    end
  end

  defp find_superextension_for_type(type_info, extensions, extension_opts, types) do
    case type_info.send do
      "array_send" ->
        oid = type_info.array_elem
        find_format_extension(oid, extensions, extension_opts, types)

      "range_send" ->
        oid = type_info.base_type
        find_format_extension(oid, extensions, extension_opts, types)

      "record_send" ->
        oids = type_info.comp_elems
        find_format_extension(oids, extensions, extension_opts, types)

      _ ->
        nil
    end
  end

  defp find_format_extension(oid, extensions, extension_opts, types) when is_integer(oid) do
    # TODO: Support text
    if extension = find_extension(types[oid], extensions, extension_opts, types) do
      opts = HashDict.fetch!(extension_opts, extension)
      if extension.format(opts) == :binary do
        Postgrex.Extensions.Binary
      end
    end
  end

  defp find_format_extension(oids, extensions, extension_opts, types) when is_list(oids) do
    # TODO: Support text
    # All record elements need to be able to be encoded/decoded with the
    # same format. For now we only support binary.

    all_binary? =
      oids
      |> Enum.map(&find_extension(types[&1], extensions, extension_opts, types))
      |> Enum.all?(fn extension ->
           if extension do
             opts = HashDict.fetch!(extension_opts, extension)
             extension.format(opts) == :binary
           end
         end)

    if all_binary? do
      Postgrex.Extensions.Binary
    end
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

  defp sql_array(list) do
    list = Enum.uniq(list)
    "ARRAY[" <> Enum.map_join(list, ", ", &("'" <> &1 <> "'")) <> "]"
  end

  ### TYPE FORMAT ###

  @doc false
  def format(oid, state) do
    {info, extension} = fetch!(state, oid)

    case info.send do
      "array_send" ->
        format(info.array_elem, state)
      "range_send" ->
        format(info.base_type, state)
      "record_send" ->
        if info.comp_elems == [] do
          # Empty record should use binary format
          :binary
        else
          format(hd(info.comp_elems), state)
        end
      _ ->
        opts = fetch_opts(state, extension)
        extension.format(opts)
    end
  end

  ### TYPE ENCODING / DECODING ###

  @doc """
  Encodes an Elixir term to a binary for the given type.
  """
  @spec encode(oid, term, state) :: binary
  def encode(oid, value, state) do
    {info, extension} = fetch!(state, oid)
    opts = fetch_opts(state, extension)
    extension.encode(info, value, state, opts)
  end

  @doc """
  Encodes an Elixir term with the extension for the given type.
  """
  @spec encode(Extension.t, oid, term, state) :: binary
  def encode(extension, oid, value, state) do
    {info, _extension} = fetch!(state, oid)
    opts = fetch_opts(state, extension)
    extension.encode(info, value, state, opts)
  end

  @doc """
  Decodes a binary to an Elixir value for the given type.
  """
  @spec decode(oid, binary, state) :: term
  def decode(oid, binary, state) do
    {info, extension} = fetch!(state, oid)
    opts = fetch_opts(state, extension)
    extension.decode(info, binary, state, opts)
  end

  @doc """
  Decodes a binary with the extension for the given type.
  """
  @spec decode(Extension.t, oid, binary, state) :: term
  def decode(extension, oid, binary, state) do
    {info, _extension} = fetch!(state, oid)
    opts = fetch_opts(state, extension)
    extension.decode(info, binary, state, opts)
  end

  defp fetch!({types, _extensions}, oid) do
    case HashDict.fetch(types, oid) do
      {:ok, value} ->
        value
      :error ->
        raise ArgumentError, message: "no extension found for oid `#{oid}`"
    end
  end

  defp fetch_opts({_types, extensions}, extension) do
    HashDict.fetch!(extensions, extension)
  end
end
