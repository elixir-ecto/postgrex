defmodule Postgrex.TypeModule do
  alias Postgrex.TypeInfo

  def define(module, extensions, opts \\ []) do
    opts =
      opts
      |> Keyword.put_new(:decode_binary, :copy)
      |> Keyword.put_new(:null, nil)
      |> Keyword.put_new(:date, :postgrex)
    config = configure(extensions, opts)
    null = Keyword.get(opts, :null)
    define_inline(module, config, null)
  end

  ## Helpers

  defp directives(config) do
    requires =
      for {extension, _} <- config do
        quote do: require unquote(extension)
      end

    quote do
      import Postgrex.BinaryUtils, [warn: false]
      require unquote(__MODULE__), [warn: false]
      unquote(requires)
    end
  end

  defp attributes(null) do
    quote do
      #@compile :bin_opt_info
      @compile {:inline, [encode_value: 2]}
      @null unquote(Macro.escape(null))
    end
  end

  defp find(config) do
    clauses = Enum.flat_map(config, &find_clauses/1)
    clauses = clauses ++ quote do: (_ -> nil)
    quote generated: true do
      def find(type_info, formats) do
        case {type_info, formats} do
          unquote(clauses)
        end
      end
    end
  end

  defp find_clauses({extension, {opts, matching, format}}) do
    for {key, value} <- matching do
      [clause] = find_clause(extension, opts, key, value, format)
      clause
    end
  end

  defp find_clause(extension, opts, key, value, :super_binary) do
    quote do
      {%{unquote(key) => unquote(value)} = type_info, formats}
          when formats in [:any, :binary] ->
      oids = unquote(extension).oids(type_info, unquote(opts))
      {:super_binary, unquote(extension), oids}
    end
  end
  defp find_clause(extension, _opts, key, value, format) do
    quote do
      {%{unquote(key) => unquote(value)}, formats}
          when formats in [:any, unquote(format)] ->
        {unquote(format), unquote(extension)}
    end
  end

  defp maybe_rewrite(ast, clauses) do
    if Application.get_env(:postgrex, :debug_extensions) do
      rewrite(ast, clauses)
    else
      ast
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

  defp encode(config) do
    encodes =
      for {extension, {opts, [_|_], format}} <- config do
        encode = extension.encode(opts)

        clauses =
          for clause <- encode do
            encode_type(extension, format, clause)
          end

        clauses = [encode_null(extension, format) | clauses]

        quote do
          unquote(encode_value(extension, format))

          unquote(encode_inline(extension, format))

          unquote(clauses |> maybe_rewrite(encode))
        end
      end

    quote do
      unquote(encodes)

      def encode_params(params, types) do
        encode_params(params, types, [])
      end

      defp encode_params([param | params], [type | types], encoded) do
        encode_params(params, types, [encode_value(param, type) | encoded])
      end
      defp encode_params([], [], encoded), do: Enum.reverse(encoded)
      defp encode_params(params, _, _) when is_list(params), do: :error

      def encode_tuple(tuple, oids, types) do
        encode_tuple(tuple, 1, oids, types, [])
      end
      defp encode_tuple(tuple, n, [oid | oids], [type | types], acc) do
        param = :erlang.element(n, tuple)
        acc = [acc, <<oid::uint32>> | encode_value(param, type)]
        encode_tuple(tuple, n+1, oids, types, acc)
      end
      defp encode_tuple(tuple, n, [], [], acc) when tuple_size(tuple) < n do
        acc
      end
      defp encode_tuple(tuple, _, [], [], _) when is_tuple(tuple), do: :error

      def encode_list(list, type) do
        encode_list(list, type, [])
      end

      defp encode_list([value | rest], type, acc) do
        encode_list(rest, type, [acc | encode_value(value, type)])
      end
      defp encode_list([], _, acc) do
        acc
      end
    end
  end

  defp encode_type(extension, :super_binary, clause) do
    encode_super(extension, clause)
  end
  defp encode_type(extension, _, clause) do
    encode_extension(extension, clause)
  end

  defp encode_extension(extension, clause) do
    case split_extension(clause) do
      {pattern, guard, body} ->
        encode_extension(extension, pattern, guard, body)
      {pattern, body} ->
        encode_extension(extension, pattern, body)
    end
  end

  defp encode_extension(extension, pattern, guard, body) do
    quote do
      defp unquote(extension)(unquote(pattern)) when unquote(guard) do
        unquote(body)
      end
    end
  end

  defp encode_extension(extension, pattern, body) do
    quote do
      defp unquote(extension)(unquote(pattern)) do
        unquote(body)
      end
    end
  end

  defp encode_super(extension, clause) do
    case split_super(clause) do
      {pattern, sub_oids, sub_types, guard, body} ->
        encode_super(extension, pattern, sub_oids, sub_types, guard, body)
      {pattern, sub_oids, sub_types, body} ->
        encode_super(extension, pattern, sub_oids, sub_types, body)
    end
  end

  defp encode_super(extension, pattern, sub_oids, sub_types, guard, body) do
    quote do
      defp unquote(extension)(unquote(pattern), unquote(sub_oids),
                              unquote(sub_types)) when unquote(guard) do
        unquote(body)
      end
    end
  end

  defp encode_super(extension, pattern, sub_oids, sub_types, body) do
    quote do
      defp unquote(extension)(unquote(pattern),
                              unquote(sub_oids), unquote(sub_types)) do
        unquote(body)
      end
    end
  end

  defp encode_inline(extension, :super_binary) do
    quote do
      @compile {:inline, [{unquote(extension), 3}]}
    end
  end
  defp encode_inline(extension, _) do
    quote do
      @compile {:inline, [{unquote(extension), 1}]}
    end
  end

  defp encode_null(extension, :super_binary) do
    quote do
      defp unquote(extension)(@null, _sub_oids, _sub_types), do: <<-1::int32>>
    end
  end
  defp encode_null(extension, _) do
    quote do
      defp unquote(extension)(@null), do: <<-1::int32>>
    end
  end

  defp encode_value(extension, :super_binary) do
    quote do
      def encode_value(value, {unquote(extension), sub_oids, sub_types}) do
        unquote(extension)(value, sub_oids, sub_types)
      end
    end
  end
  defp encode_value(extension, _) do
    quote do
      def encode_value(value, unquote(extension)) do
        unquote(extension)(value)
      end
    end
  end

  defp decode(config) do
    decodes =
      for {extension, {opts, [_|_], format}} <- config do
        decode = extension.decode(opts)

        clauses =
          for clause <- decode do
            decode_type(extension, format, clause)
          end
        quote do
          unquote(clauses |> maybe_rewrite(decode))

          unquote(decode_null(extension, format))
        end
      end

    quote do
      unquote(decode_row(config))

      unquote(decode_list(config))

      unquote(decode_tuple(config))

      unquote(decodes)
    end
  end

  defp decode_row(config) do
    rest  = quote do: rest
    types = quote do: types
    acc   = quote do: acc

    dispatch =
      for {extension, {_, [_|_], format}} <- config do
        decode_row_dispatch(extension, format, rest, types, acc)
      end

    quote do
      def decode_row(binary, types) do
        decode_row(binary, types, [])
      end

      defp decode_row(<<-1::int32, rest::binary>>, [_ | types], acc) do
        decode_row(rest, types, [@null | acc])
      end
      defp decode_row(<<rest::binary>>, [type | types], acc) do
        case type do
          unquote(dispatch)
        end
      end
      defp decode_row(<<_::binary-size(0)>>, [], acc) do
        acc
      end
    end
  end

  defp decode_row_dispatch(extension, :super_binary, rest, types, acc) do
    [clause] =
      quote do
        {unquote(extension), sub_oids, sub_types} ->
          unquote(extension)(unquote(rest), sub_oids, sub_types,
                             unquote(types), unquote(acc))
      end
    clause
  end
  defp decode_row_dispatch(extension, _, rest, types, acc) do
    [clause] =
      quote do
        unquote(extension) ->
          unquote(extension)(unquote(rest), unquote(types), unquote(acc))
      end
    clause
  end

  defp decode_list(config) do
    rest = quote do: rest

    dispatch =
       for {extension, {_, [_|_], format}} <- config do
         decode_list_dispatch(extension, format, rest)
       end

    quote do
      def decode_list(<<unquote(rest)::binary>>, type) do
        case type do
          unquote(dispatch)
        end
      end
    end
  end

  defp decode_list_dispatch(extension, :super_binary, rest) do
    [clause] =
      quote do
        {unquote(extension), sub_oids, sub_types} ->
          unquote(extension)(unquote(rest), sub_oids, sub_types, [])
      end
    clause
  end
  defp decode_list_dispatch(extension, _, rest) do
    [clause] =
      quote do
        unquote(extension) ->
          unquote(extension)(unquote(rest), [])
      end
    clause
  end

  defp decode_tuple(config) do
    rest = quote do: rest
    oids = quote do: oids
    n    = quote do: n
    acc  = quote do: acc

    dispatch =
       for {extension, {_, [_|_], format}} <- config do
         decode_tuple_dispatch(extension, format, rest, oids, n, acc)
       end

    quote do
      def decode_tuple(<<rest::binary>>, count, types) when is_integer(count) do
        decode_tuple(rest, count, types, count, [])
      end
      def decode_tuple(<<rest::binary>>, oids, types) do
        decode_tuple(rest, oids, types, 0, [])
      end

      defp decode_tuple(<<oid::int32, unquote(rest)::binary>>,
                        [oid | unquote(oids)], types,
                        unquote(n), unquote(acc)) do
        case types do
          unquote(dispatch)
        end
      end
      defp decode_tuple(<<>>, [], [], n, acc) do
        :erlang.make_tuple(n, @null, acc)
      end
      defp decode_tuple(<<oid::int32, unquote(rest)::binary>>,
                        rem, types,
                        unquote(n), unquote(acc)) when rem > 0 do
        case Postgrex.Types.fetch(oid, types) do
          {:ok, {:binary, type}} ->
            unquote(oids) = rem - 1
            case [type | types] do
              unquote(dispatch)
            end
          {:ok, {:text, _}} ->
            msg = "oid `#{oid}` was bootstrapped in text format and can not " <>
                  "be decoded inside an anonymous record"
            raise RuntimeError, msg
          {:error, %TypeInfo{type: pg_type}} ->
            msg = "type `#{pg_type}` can not be handled by the configured " <>
                  "extensions"
            raise RuntimeError, msg
          {:error, nil} ->
            msg = "oid `#{oid}` was not bootstrapped and lacks type information"
            raise RuntimeError, msg
        end
      end
      defp decode_tuple(<<>>, 0, _types, n, acc) do
        :erlang.make_tuple(n, @null, acc)
      end
    end
  end

  defp decode_tuple_dispatch(extension, :super_binary, rest, oids, n, acc) do
    [clause] =
      quote do
        [{unquote(extension), sub_oids, sub_types} | types] ->
          unquote(extension)(unquote(rest), sub_oids, sub_types,
                             unquote(oids), types, unquote(n)+1, unquote(acc))
      end
    clause
  end
  defp decode_tuple_dispatch(extension, _, rest, oids, n, acc) do
    [clause] =
      quote do
        [unquote(extension) | types] ->
          unquote(extension)(unquote(rest), unquote(oids), types,
                             unquote(n)+1, unquote(acc))
      end
    clause
  end

  defp decode_type(extension, :super_binary, clause) do
    decode_super(extension, clause)
  end
  defp decode_type(extension, _, clause) do
    decode_extension(extension, clause)
  end

  defp decode_null(extension, :super_binary) do
    decode_super_null(extension)
  end
  defp decode_null(extension, _) do
    decode_extension_null(extension)
  end

  defp decode_extension(extension, clause) do
    case split_extension(clause) do
      {pattern, guard, body} ->
        decode_extension(extension, pattern, guard, body)
      {pattern, body} ->
        decode_extension(extension, pattern, body)
    end
  end

  defp decode_extension(extension, pattern, guard, body) do
    quote do
      defp unquote(extension)(<<unquote(pattern), rest::binary>>,
                              types, acc) when unquote(guard) do
        decode_row(rest, types, [unquote(body) | acc])
      end

      defp unquote(extension)(<<unquote(pattern), rest::binary>>, acc)
                              when unquote(guard) do
        unquote(extension)(rest, [unquote(body) | acc])
      end

      defp unquote(extension)(<<unquote(pattern), rest::binary>>,
                              oids, types, n, acc) when unquote(guard) do
        decode_tuple(rest, oids, types, n, [{n, unquote(body)} | acc])
      end
    end
  end

  defp decode_extension(extension, pattern, body) do
    quote do
      defp unquote(extension)(<<unquote(pattern), rest::binary>>, types, acc) do
        decode_row(rest, types, [unquote(body) | acc])
      end

      defp unquote(extension)(<<unquote(pattern), rest::binary>>, acc) do
        decoded = unquote(body)
        unquote(extension)(rest, [decoded | acc])
      end

      defp unquote(extension)(<<unquote(pattern), rest::binary>>,
                              oids, types, n, acc) do
        decode_tuple(rest, oids, types, n, [{n, unquote(body)} | acc])
      end
    end
  end

  defp decode_extension_null(extension) do
    quote do

      defp unquote(extension)(<<-1::int32, rest::binary>>, acc) do
        unquote(extension)(rest, [@null | acc])
      end

      defp unquote(extension)(<<>>, acc) do
        acc
      end

      defp unquote(extension)(<<-1::int32, rest::binary>>,
                              oids, types, n, acc) do
        decode_tuple(rest, oids, types, n, acc)
      end
    end
  end

  defp split_extension({:->, _, [head, body]}) do
    case head do
      [{:when, _, [pattern, guard]}] ->
        {pattern, guard, body}
      [pattern] ->
        {pattern, body}
    end
  end

  defp decode_super(extension, clause) do
    case split_super(clause) do
      {pattern, oids, types, guard, body} ->
        decode_super(extension, pattern, oids, types, guard, body)
      {pattern, oids, types, body} ->
        decode_super(extension, pattern, oids, types, body)
    end
  end

  defp decode_super(extension, pattern, sub_oids, sub_types, guard, body) do
    quote do
      defp unquote(extension)(<<unquote(pattern), rest::binary>>,
                              unquote(sub_oids), unquote(sub_types),
                              types, acc) when unquote(guard) do
        decode_row(rest, types, [unquote(body) | acc])
      end

      defp unquote(extension)(<<unquote(pattern), rest::binary>>,
                              unquote(sub_oids), unquote(sub_types), acc)
           when unquote(guard) do
        acc = [unquote(body) | acc]
        unquote(extension)(rest, unquote(sub_oids), unquote(sub_types), acc)
      end

      defp unquote(extension)(<<unquote(pattern), rest::binary>>,
                              unquote(sub_oids), unquote(sub_types),
                              oids, types, n, acc) when unquote(guard) do
        decode_tuple(rest, oids, types, n, [{n, unquote(body)} | acc])
      end
    end
  end

  defp decode_super(extension, pattern, sub_oids, sub_types, body) do
    quote do
      defp unquote(extension)(<<unquote(pattern), rest::binary>>,
                              unquote(sub_oids), unquote(sub_types),
                              types, acc) do
        decode_row(rest, types, [unquote(body) | acc])
      end

      defp unquote(extension)(<<unquote(pattern), rest::binary>>,
                              unquote(sub_oids), unquote(sub_types), acc) do
        acc = [unquote(body) | acc]
        unquote(extension)(rest, unquote(sub_oids), unquote(sub_types), acc)
      end

      defp unquote(extension)(<<unquote(pattern), rest::binary>>,
                              unquote(sub_oids), unquote(sub_types),
                              oids, types, n, acc) do
        acc = [{n, unquote(body)} | acc]
        decode_tuple(rest, oids, types, n, acc)
      end
    end
  end

  defp decode_super_null(extension) do
    quote do
      defp unquote(extension)(<<-1::int32, rest::binary>>,
                              sub_oids, sub_types, acc) do
        unquote(extension)(rest, sub_oids, sub_types, [@null | acc])
      end

      defp unquote(extension)(<<>>, _sub_oid, _sub_types, acc) do
        acc
      end

      defp unquote(extension)(<<-1::int32, rest::binary>>,
                              _sub_oids, _sub_types,
                              oids, types, n, acc) do
        decode_tuple(rest, oids, types, n, acc)
      end
    end
  end

  defp split_super({:->, _, [head, body]}) do
    case head do
      [{:when, _, [pattern, sub_oids, sub_types, guard]}] ->
        {pattern, sub_oids, sub_types, guard, body}
      [pattern, sub_oids, sub_types] ->
        {pattern, sub_oids, sub_types, body}
    end
  end

  defp configure(extensions, opts) do
    defaults = Postgrex.Utils.default_extensions(opts)
    for {extension, arg} <- extensions ++ defaults do
      opts     = extension.init(arg)
      matching = extension.matching(opts)
      format   = extension.format(opts)
      {extension, {opts, matching, format}}
    end
  end

  defp define_inline(module, config, null) do
    quoted = [directives(config), attributes(null), find(config),
              encode(config), decode(config)]
    Module.create(module, quoted, Macro.Env.location(__ENV__))
  end
end
