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
      [oid, type, send, receive, output, input, array_oid, comp_oids] = row
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
      extensions = Enum.filter(extensions, &match_extension_against_type(&1, type_info))
      if extensions != [] do
        HashDict.put(dict, type_info.oid, {type_info, extensions})
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
    case HashDict.fetch(types, oid) do
      {:ok, {%TypeInfo{send: send, type: type, array_elem: array_oid, comp_elems: comp_oids}, [extension|_]}} ->
        cond do
          send == "array_send" and format(array_oid, types) == :binary ->
            :binary
          send == "record_send" and type != "record" and
          Enum.all?(comp_oids, &(format(&1, types) == :binary)) ->
            :binary
          true ->
            extension.format()
        end

      :error ->
        # TODO: Handle this throw
        # TODO: Or should we default to :text? If so, make sure to handle it in
        #       encode and decode functions
        throw {:postgrex_format, "unable to decide format, no extension found for oid `#{oid}`"}
    end
  end

  ### TYPE ENCODING ###

  def encode(_oid, nil, _types) do
    <<-1 :: int32>>
  end

  def encode(oid, value, types) do
    case HashDict.fetch(types, oid) do
      {:ok, {info, [extension|_]}} ->
        # TODO: Fix this?
        # types = HashDict.put(types, oid, {info, rest_extensions})
        default = &encode(&1, &2, types)
        extension.encode(info, value, default) |> encode_param
      :error ->
        # TODO: Handle this throw
        throw {:postgrex_encode, "unable to encode value `#{inspect value}`, no extension found for oid `#{oid}`"}
    end
  end

  defp encode_param(<<-1 :: int32>>),
    do: <<-1 :: int32>>
  defp encode_param(param),
    do: [<<IO.iodata_length(param) :: int32>>, param]

  ### TYPE DECODING ###

  def decode(oid, binary, types) do
    case HashDict.fetch(types, oid) do
      {:ok, {info, [extension|_]}} ->
        default = &decode(&1, &2, types)
        extension.decode(info, binary, default)
      :error ->
        # TODO: Handle this throw
        throw {:postgrex_decode, "unable to decode binary `#{inspect binary}`, no extension found for oid `#{oid}`"}
    end
  end
end
