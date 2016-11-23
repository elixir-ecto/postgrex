defmodule Postgrex.TypeModule do
  alias Postgrex.Types

  @superextension [Postgrex.Extensions.Array,
                   Postgrex.Extensions.Range,
                   Postgrex.Extensions.Record]

  def define(module, parameters, type_infos, opts \\ []) do
    opts =
      opts
      |> Keyword.put_new(:decode_binary, :copy)
      |> Keyword.put_new(:null, nil)
    ^module = :ets.new(module, [:named_table])
    try do
      associate(module, parameters, type_infos, opts)
      prepare_inline(module)
    else
      {oids, types} ->
        null = Keyword.fetch!(opts, :null)
        define_inline(module, oids, types, null)
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

  defp rewrite(ast, [{:->, meta, _} | _original]) do
    location = [file: meta[:file] || "nofile", line: meta[:keep] || 1]

    Macro.prewalk(ast, fn
      {left, meta, right} ->
        {left, location ++ meta, right}
      other ->
        other
    end)
  end

  defp encode(types, null) do
    encodes =
      for {type, _, encode, _} <- types do
        null_clause =
          quote do
            defp unquote(type)(unquote(null)), do: unquote(null)
          end

        clauses = for clause <- encode, do: encode_type(type, clause)

        clauses = [null_clause | clauses]

        quote do
          defp encode_params([param | params], [unquote(type) | types], acc) do
            encoded = unquote(type)(param)
            encode_params(params, types, [encoded | acc])
          end

          @compile {:inline, [{unquote(type), 1}]}

          unquote(clauses |> rewrite(encode))
        end
      end

    quote do
      def encode_params(params, types) do
        encode_params(params, types, [])
      end

      unquote(encodes)

      defp encode_params([], [], encoded), do: Enum.reverse(encoded)
      defp encode_params(params, _, _) when is_list(params), do: :error
    end
  end

  defp encode_type(type, clause) do
    case split_decode(clause) do
      {pattern, guard, body} ->
        encode_type(type, pattern, guard, body)
      {pattern, body} ->
        encode_type(type, pattern, body)
    end
  end

  defp encode_type(type, pattern, guard, body) do
    quote do
      defp unquote(type)(unquote(pattern)) when unquote(guard) do
        unquote(body)
      end
    end
  end

  defp encode_type(type, pattern, body) do
    quote do
      defp unquote(type)(unquote(pattern)) do
        unquote(body)
      end
    end
  end

  defp decode(types, null) do
    rest = quote do: rest
    acc = quote do: acc

    dispatch =
      for {type, _, _, _} <- types do
        [clause] =
          quote do
            [unquote(type) | types] ->
              unquote(type)(unquote(rest), types, unquote(acc))
          end
        clause
      end

    decoded  = (quote do: ([] -> decoded_row(unquote(rest), unquote(acc))))
    dispatch = dispatch ++ decoded

    decodes =
      for {type, _, _, decode} <- types do
        clauses =
          for clause <- decode do
            decode_type(type, clause, dispatch, rest, acc)
          end

        quote do
          unquote(clauses |> rewrite(decode))

          defp unquote(type)(<<-1::int32, unquote(rest)::binary>>,
                             types, acc) do
            unquote(acc) = [unquote(null) | acc]
            case types do
              unquote(dispatch)
            end
          end
        end
      end

    quote do
      def decode_row(<<unquote(rest)::binary>>, types) do
        unquote(acc) = []
        case types do
          unquote(dispatch)
        end
      end

      unquote(decodes)

      defp decoded_row(<<>>, acc), do: Enum.reverse(acc)
      defp decoded_row(<<_::binary>>, _), do: :error
    end
  end

  defp decode_type(type, clause, dispatch, rest, acc) do
    case split_decode(clause) do
      {pattern, guard, body} ->
        decode_type(type, pattern, guard, body, dispatch, rest, acc)
      {pattern, body} ->
        decode_type(type, pattern, body, dispatch, rest, acc)
    end
  end

  defp decode_type(type, pattern, guard, body, dispatch, rest, acc) do
    quote do
      defp unquote(type)(<<unquote(pattern), unquote(rest)::binary>>,
                         types, acc) when unquote(guard) do
        unquote(acc) = [unquote(body) | acc]
        case types do
          unquote(dispatch)
        end
      end
    end
  end

  defp decode_type(type, pattern, body, dispatch, rest, acc) do
    quote do
      defp unquote(type)(<<unquote(pattern), unquote(rest)::binary>>,
                         types, acc) do
        unquote(acc) = [unquote(body) | acc]
        case types do
          unquote(dispatch)
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

  defp define_inline(module, oids, types, null) do
    quoted = [directives(types), fetch(oids),
              encode(types, null), decode(types, null)]
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
