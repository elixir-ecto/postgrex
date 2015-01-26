defmodule Postgrex.Types do
  @moduledoc false

  alias Postgrex.TypeInfo
  import Postgrex.BinaryUtils

  ### BOOTSTRAP TYPES AND EXTENSIONS ###

  def bootstrap_query(m) do
    """
    SELECT t.oid, t.typname, t.typsend, t.typreceive, t.typoutput, t.typinput, t.typelem, ARRAY (
      SELECT a.atttypid
      FROM pg_attribute AS a
      WHERE a.attrelid = t.typrelid AND a.attnum > 0 AND NOT a.attisdropped
      ORDER BY a.attnum
    )
    FROM pg_type AS t
    WHERE
      t.typname::text = ANY ((#{sql_array(m.type)})::text[]) OR
      t.typsend::text = ANY ((#{sql_array(m.send)})::text[]) OR
      t.typreceive::text = ANY ((#{sql_array(m.receive)})::text[]) OR
      t.typoutput::text = ANY ((#{sql_array(m.output)})::text[]) OR
      t.typinput::text = ANY ((#{sql_array(m.input)})::text[])
    """
  end

  def extension_matchers(extensions) do
    map = %{type: [], send: [], receive: [], output: [], input: []}
    Enum.reduce(extensions, map, fn extension, map ->
      Enum.reduce(extension.matching(), map, fn {key, value}, map ->
        Map.update!(map, key, &[value|&1])
      end)
    end)
  end

  def build_types(rows) do
    Enum.map(rows, fn row ->
      [<<_::int32, oid::binary>>,
       <<_::int32, type::binary>>,
       <<_::int32, send::binary>>,
       <<_::int32, receive::binary>>,
       <<_::int32, output::binary>>,
       <<_::int32, input::binary>>,
       <<_::int32, array_oid::binary>>,
       <<_::int32, comp_oids::binary>>] = row
      oid = String.to_integer(oid)
      array_oid = String.to_integer(array_oid)
      comp_oids = parse_oids(comp_oids)

      %TypeInfo{
        oid: oid,
        type: type,
        send: send,
        receive: receive,
        output: output,
        input: input,
        array_elem: array_oid,
        comp_elems: comp_oids}
    end)
  end

  def associate_extensions_with_types(extensions, types) do
    Enum.reduce(types, HashDict.new, fn type_info, dict ->
      extension = Enum.find(extensions, &match_extension_against_type(&1, type_info))
      if extension do
        HashDict.put(dict, type_info.oid, {type_info, extension})
      else
        dict
      end
    end)
  end

  defp match_extension_against_type(extension, type_info) do
    matching = extension.matching()
    Enum.any?(matching, &match_type(&1, type_info))
  end

  defp match_type({field, value}, type_info) do
    case Map.fetch(type_info, field) do
      {:ok, ^value} -> true
      _ -> false
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

  def format(oid, types) do
    {info, extension} = fetch!(types, oid)
    cond do
      info.send == "array_send" and format(info.array_elem, types) == :binary ->
        :binary
      info.send == "record_send" and info.type != "record" and
      Enum.all?(info.comp_elems, &(format(&1, types) == :binary)) ->
        :binary
      true ->
        extension.format()
    end
  end

  ### TYPE ENCODING ###

  def encode(_extension, oid, nil, types) do
    fetch!(types, oid)
    <<-1 :: int32>>
  end

  def encode(extension, oid, value, types) do
    {info, _extension} = fetch!(types, oid)
    binary = extension.encode(info, value, types)
    [<<IO.iodata_length(binary) :: int32>>, binary]
  end

  def encode(oid, nil, types) do
    fetch!(types, oid)
    <<-1 :: int32>>
  end

  def encode(oid, value, types) do
    {info, extension} = fetch!(types, oid)
    binary = extension.encode(info, value, types)
    [<<IO.iodata_length(binary) :: int32>>, binary]
  end

  ### TYPE DECODING ###

  def decode(_extension, oid, <<-1 :: int32>>, types) do
    fetch!(types, oid)
    nil
  end

  def decode(extension, oid, <<size :: int32, binary :: binary(size)>>, types) do
    {info, _extension} = fetch!(types, oid)
    extension.decode(info, binary, types)
  end

  def decode(oid, <<-1 :: int32>>, types) do
    fetch!(types, oid)
    nil
  end

  def decode(oid, <<size :: int32, binary :: binary(size)>>, types) do
    {info, extension} = fetch!(types, oid)
    extension.decode(info, binary, types)
  end

  defp fetch!(types, oid) do
    case HashDict.fetch(types, oid) do
      {:ok, value} ->
        value
      :error ->
        raise ArgumentError, message: "no extension found for oid `#{oid}`"
    end
  end
end
