defmodule Postgrex.TypeModule do
  alias Postgrex.Types

  @superextension [Postgrex.Extensions.Array,
                   Postgrex.Extensions.Range,
                   Postgrex.Extensions.Record]

  def define(module, parameters, type_infos, opts \\ []) do
    ^module = :ets.new(module, [:named_table])
    try do
      associate(module, parameters, type_infos, opts)
      prepare_inline(module)
    else
      {oids, types} ->
        define_inline(module, oids, types)
    after
      :ets.delete(module)
    end
  end

  def write(file, module, parameters, types, opts \\ []) do
    File.write(file, generate(module, parameters, types, opts))
  end

  ## Helpers

  defp directives(types) do
    requires =
      for {_, extension, _, _} <- types do
        quote do: require unquote(extension)
      end

    quote do
      import Postgrex.BinaryUtils, [warn: false]

      unquote(requires)
    end
  end

  defp fetch(oids) do
    fetches =
      for {oid, type, format} <- oids do
        quote do
          def fetch(unquote(oid)) do
            {:ok, {unquote(format), unquote(type)}}
          end
        end
      end

    quote do
      unquote(fetches)
      def fetch(_), do: :error
    end
  end

  defp get_location([{:->, meta, _} | _]) do
    {meta[:file] || "nofile", meta[:line] || 1}
  end

  defp encode(types) do
    encodes =
      for {type, _, encode, _} <- types do
        location = get_location(encode)
        clauses = (quote do: (^null -> <<-1::int32>>)) ++ encode
        quote do
          @file unquote(location)
          defp encode([param | params], [unquote(type) | types], null, acc) do
            encoded =
              case param do
                unquote(clauses)
              end
            encode(params, types, null, [encoded | acc])
          end
        end
      end

    quote do
      def encode(params, types, null) do
        encode(params, types, null, [])
      end

      unquote(encodes)

      defp encode([], [], _, encoded), do: Enum.reverse(encoded)
      defp encode(params, _, _, _) when is_list(params), do: :error
    end
  end

  defp decode(types) do
    decodes =
      for {type, _, _, decode} <- types do
        location = get_location(decode)
        clauses = for clause <- decode, do: decode_type(type, clause, location)
        quote do
          @file unquote(location)
          defp decode(<<rest::binary>>, [unquote(type) | types], null, acc) do
            unquote(type)(rest, types, null, acc)
          end

          unquote(clauses)

          @file unquote(location)
          defp unquote(type)(<<-1::int32, rest::binary>>, types, null, acc) do
            decode(rest, types, null, [null | acc])
          end
        end
      end

    quote do
      def decode(row, types, null) do
        decode(row, types, null, [])
      end

      unquote(decodes)

      defp decode(<<>>, [], _, decoded), do: Enum.reverse(decoded)
      defp decode(<<_::binary>>, _, _, _), do: :error
    end
  end

  defp decode_type(type, clause, location) do
    case split_decode(clause) do
      {pattern, guard, body} ->
        quote do
          @file unquote(location)
          defp unquote(type)(<<unquote(pattern), rest::binary>>, types, null, acc)
              when unquote(guard) do
            decoded = unquote(body)
            decode(rest, types, null, [decoded | acc])
          end
        end
      {pattern, body} ->
        quote do
          @file unquote(location)
          defp unquote(type)(<<unquote(pattern), rest::binary>>, types, null, acc) do
            decoded = unquote(body)
            decode(rest, types, null, [decoded | acc])
          end
        end
    end
  end

  defp split_decode({:->, _, [head, body]}) do
    case head do
      [{:when, _, [pattern, guard]}] ->
        {pattern, guard, body}
      [pattern] ->
        {pattern, body}
    end
  end

  defp associate(module, parameters, types, opts) do
    opts = Keyword.put_new(opts, :decode_binary, :copy)
    extensions = Postgrex.Utils.default_extensions(opts)
    extension_keys = Enum.map(extensions, &elem(&1, 0))
    extension_opts = Types.prepare_extensions(extensions, parameters)
    Types.associate_extensions_with_types(module, extension_keys, extension_opts, types)
    Types.delete_unhandled_oids(module)
  end

  defp prepare_inline(module) do
    oids = :ets.new(:oids, [:set])
    types = :ets.new(:handlers, [:set])
    try do
      prepare_inline(module, oids, types)
    after
      :ets.delete(oids)
      :ets.delete(types)
    end
  end

  defp prepare_inline(module, oids, types) do
    _ =
      for oid <- Types.oids(module),
          {extension, format, info, opts} <- [Types.inline_opts(oid, module)],
          not (extension in @superextension) do
        {type, encode, decode} = extension.inline(info, module, opts)
        :ets.insert_new(types, {type, extension, encode, decode})
        :ets.insert(oids, {oid, type, format})
      end
    {:ets.tab2list(oids), :ets.tab2list(types)}
  end

  defp define_inline(module, oids, types) do
    quoted = [directives(types), fetch(oids), encode(types), decode(types)]
    Module.create(module, quoted, Macro.Env.location(__ENV__))
  end

  defp generate(module, parameters, types, opts) do
    ["parameters =\n",
     gen_inspect(parameters), ?\n,
     "types =\n",
     gen_inspect(types), ?\n,
     gen_inspect(__MODULE__),
      ".define(#{gen_inspect(module)}, parameters, types, ",
      gen_inspect(opts), ")\n"]
  end

  defp gen_inspect(term) do
    inspect(term, [limit: :infinity, width: 80, pretty: true])
  end
end
