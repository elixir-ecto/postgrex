defmodule Postgrex.Types do
  @moduledoc """
  Encodes and decodes between Postgres' protocol and Elixir values.
  """

  alias Postgrex.TypeInfo
  alias Postgrex.Extension

  @typedoc """
  Postgres internal identifier that maps to a type. See
  http://www.postgresql.org/docs/9.4/static/datatype-oid.html.
  """
  @type oid :: pos_integer

  @typedoc """
  State used by the encoder/decoder functions
  """
  @opaque state :: :ets.tab

  ### BOOTSTRAP TYPES AND EXTENSIONS ###

  @doc false
  def bootstrap_query(version, oids) do
    {rngsubtype, join_range} =
      if version >= {9, 2, 0} do
        {"coalesce(r.rngsubtype, 0)",
         "LEFT JOIN pg_range AS r ON r.rngtypid = t.oid"}
      else
        {"0", ""}
      end

    filter_oids =
      case oids do
        [] ->
          ""
        _  ->
          "WHERE t.oid NOT IN (SELECT unnest(ARRAY[#{Enum.join(oids, ",")}]))"
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
    #{filter_oids}
    """
  end

  @doc false
  def configure(extensions, parameters) do
    Enum.into(extensions, Map.new, fn {extension, opts} ->
      opts     = extension.init(parameters, opts)
      matching = extension.matching(opts)
      format   = extension.format(opts)
      {extension, {opts, matching, format}}
    end)
  end

  @doc false
  def build_type_infos(rows) do
    Enum.map(rows, fn row ->
      [oid,
       type,
       send,
       receive,
       output,
       input,
       array_oid,
       base_oid,
       comp_oids] = row
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
    end)
  end

  @doc false
  def associate_type_infos(type_infos, extensions, config) do
    oids = Enum.into(type_infos, %{}, &{&1.oid, &1})
    formats = [:binary, :text, :super_binary]
    for type_info <- type_infos do
      {type_info, find(extensions, type_info, formats, config, oids)}
    end
  end

  defp find(_extensions, nil, _formats, _config, _oids) do
    nil
  end

  defp find(extensions, type_info, formats, config, oids) do
    find(extensions, type_info, formats, extensions, config, oids)
  end

  defp find([extension | rest], type_info, formats, extensions, config, oids) do
    case match(extension, type_info, formats, extensions, config, oids) do
      {format, type} ->
        {format, type}
      nil ->
        find(rest, type_info, formats, extensions, config, oids)
    end
  end
  defp find([], _type_info, _formats, _extensions, _config, _oids) do
    nil
  end

  defp match(extension, type_info, formats, extensions, config, oids) do
    if match_extension_against_type(extension, config, type_info) do
      match_format(extension, type_info, formats, extensions, config, oids)
    end
  end

  defp match_extension_against_type(extension, config, type_info) do
    {_opts, matching, _format} = Map.fetch!(config, extension)
    Enum.any?(matching, &match_type(&1, type_info))
  end

  defp match_type({field, value}, type_info) do
    case Map.fetch(type_info, field) do
      {:ok, ^value} -> true
      _ -> false
    end
  end

   # TODO: Support text
   # All record elements need to be able to be encoded/decoded with the
   # same format. For now we only support binary.
  defp match_format(extension, type_info, formats, extensions, config, oids) do
    {opts, _matching, format} = Map.fetch!(config, extension)
    cond do
      not format in formats ->
        nil
      format == :super_binary ->
        sub_oids = extension.oids(type_info, opts)
        formats = formats--[:text]
        case super_fetch(sub_oids, formats, extensions, config, oids) do
          {:ok, sub_types} ->
            {:binary, {extension, sub_oids, sub_types}}
          :error ->
            nil
        end
      true ->
        {format, extension}
    end
  end

  defp super_fetch(sub_oids, acc \\ [], formats, extensions, config, oids)

  defp super_fetch([oid | sub_oids], acc, formats, extensions, config, oids) do
    type_info = Map.get(oids, oid)
    case find(extensions, type_info, formats, config, oids) do
      {_format, type} ->
        super_fetch(sub_oids, [type | acc], formats, extensions, config, oids)
      nil ->
        :error
    end
  end
  defp super_fetch([], acc, _formats, _extension, _config, _oids) do
    {:ok, Enum.reverse(acc)}
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

  @doc false
  def oids(table) do
    :ets.select(table,[{{:"$1", :_, :_}, [], [:"$1"]}])
  end

  @doc false
  def delete_unhandled_oids(table) do
    :ets.match_delete(table, {:_, :_, nil})
  end

  ### TYPE FORMAT ###

  @doc false
  def inline_opts(oid, state) do
    {_, info, extension} = fetch!(state, oid)
    format = format(oid, state)
    {extension, format, info, fetch_opts(state, extension)}
  end

  @doc false
  def param_opts({oid, info, nil}, _state) do
    raise ArgumentError, "no extension found for oid `#{oid}`: " <> inspect(info)
  end
  def param_opts({oid, info, extension}, state) do
    opts = fetch_opts(state, extension)
    {format(info, extension, state), {oid, info, extension, opts}}
  end

  @doc false
  def result_opts({oid, info, nil}, _state) do
    raise ArgumentError, "no extension found for oid `#{oid}`: " <> inspect(info)
  end
  def result_opts({oid, info, extension}, state) do
    opts = fetch_opts(state, extension)
    {format(info, extension, state), {oid, info, extension, opts}}
  end

  defp format(oid, state) do
    {_, info, extension} = fetch!(state, oid)
    format(info, extension, state)
  end

  defp format(info, extension, state) do
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
    {_oid, info, extension} = fetch!(state, oid)
    opts = fetch_opts(state, extension)
    extension.encode(info, value, state, opts)
  end

  @doc """
  Encodes an Elixir term with the extension for the given type.
  """
  @spec encode(Extension.t, oid, term, state) :: binary
  def encode(extension, oid, value, state) do
    {_oid, info, _extension} = fetch!(state, oid)
    opts = fetch_opts(state, extension)
    extension.encode(info, value, state, opts)
  end

  @doc """
  Decodes a binary to an Elixir value for the given type.
  """
  @spec decode(oid, binary, state) :: term
  def decode(oid, binary, state) do
    {_oid, info, extension} = fetch!(state, oid)
    opts = fetch_opts(state, extension)
    extension.decode(info, binary, state, opts)
  end

  @doc """
  Decodes a binary with the extension for the given type.
  """
  @spec decode(Extension.t, oid, binary, state) :: term
  def decode(extension, oid, binary, state) do
    {_oid, info, _extension} = fetch!(state, oid)
    opts = fetch_opts(state, extension)
    extension.decode(info, binary, state, opts)
  end

  @doc false
  def fetch(table, oid) do
    case :ets.lookup(table, oid) do
      [value] ->
        {:ok, value}
      [] ->
        :error
    end
  end

  defp fetch!(table, oid) do
    case :ets.lookup(table, oid) do
      [{_, info, nil}] ->
        raise ArgumentError, "no extension found for oid `#{oid}`: " <> inspect(info)
      [value] ->
        value
      [] ->
        raise ArgumentError, "no extension found for oid `#{oid}`"
    end
  end

  defp fetch_opts(table, extension) do
    :ets.lookup_element(table, extension, 2)
  end
end
