defmodule Postgrex.TypeModule do
  @moduledoc false

  alias Postgrex.TypeInfo

  def define(module, extensions, opts) do
    opts =
      opts
      |> Keyword.put_new(:decode_binary, :copy)

    config = configure(extensions, opts)
    define_inline(module, config, opts)
  end

  ## Helpers

  defp directives(config, opts) do
    requires =
      for {extension, _} <- config do
        quote do: require(unquote(extension))
      end

    preludes =
      for {extension, {state, _, _}} <- config,
          function_exported?(extension, :prelude, 1),
          do: extension.prelude(state)

    null = Keyword.get(opts, :null)

    moduledoc = Keyword.get(opts, :moduledoc, false)

    quote do
      @moduledoc unquote(moduledoc)
      import Postgrex.BinaryUtils
      require unquote(__MODULE__)
      unquote(requires)
      unquote(preludes)
      unquote(bin_opt_info(opts))
      @compile {:inline, [encode_value: 2]}
      @dialyzer {:no_opaque, [decode_tuple: 5]}
      @null unquote(Macro.escape(null))
    end
  end

  defp bin_opt_info(opts) do
    if Keyword.get(opts, :bin_opt_info) do
      quote do: @compile(:bin_opt_info)
    else
      []
    end
  end

  @anno [generated: true]

  defp find(config) do
    clauses = Enum.flat_map(config, &find_clauses/1)
    clauses = clauses ++ quote do: (_ -> nil)

    quote @anno do
      @doc false
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

  defp maybe_rewrite(ast, extension, cases, opts) do
    if Postgrex.Utils.default_extension?(extension) and
         not Keyword.get(opts, :debug_defaults, false) do
      ast
    else
      rewrite(ast, cases)
    end
  end

  defp rewrite(ast, [{:->, clause_meta, _} | _original]) do
    Macro.prewalk(ast, fn
      {kind, meta, [{fun, _, args}, block]} when kind in [:def, :defp] and is_list(args) ->
        {kind, meta, [{fun, clause_meta, args}, block]}

      other ->
        other
    end)
  end

  defp encode(config, define_opts) do
    encodes =
      for {extension, {opts, [_ | _], format}} <- config do
        encode = extension.encode(opts)

        clauses =
          for clause <- encode do
            encode_type(extension, format, clause)
          end

        clauses = [encode_null(extension, format) | clauses]

        quote do
          unquote(encode_value(extension, format))

          unquote(encode_inline(extension, format))

          unquote(clauses |> maybe_rewrite(extension, encode, define_opts))
        end
      end

    quote location: :keep do
      unquote(encodes)

      @doc false
      def encode_params(params, types) do
        encode_params(params, types, [])
      end

      defp encode_params([param | params], [type | types], encoded) do
        encode_params(params, types, [encode_value(param, type) | encoded])
      end

      defp encode_params([], [], encoded), do: Enum.reverse(encoded)
      defp encode_params(params, _, _) when is_list(params), do: :error

      @doc false
      def encode_tuple(tuple, nil, _types) do
        raise DBConnection.EncodeError, """
        cannot encode anonymous tuple #{inspect(tuple)}. \
        Please define a custom Postgrex extension that matches on its underlying type:

            use Postgrex.BinaryExtension, type: "typeinthedb"
        """
      end

      def encode_tuple(tuple, oids, types) do
        encode_tuple(tuple, 1, oids, types, [])
      end

      defp encode_tuple(tuple, n, [oid | oids], [type | types], acc) do
        param = :erlang.element(n, tuple)
        acc = [acc, <<oid::uint32()>> | encode_value(param, type)]
        encode_tuple(tuple, n + 1, oids, types, acc)
      end

      defp encode_tuple(tuple, n, [], [], acc) when tuple_size(tuple) < n do
        acc
      end

      defp encode_tuple(tuple, n, [], [], _) when is_tuple(tuple) do
        raise DBConnection.EncodeError,
              "expected a tuple of size #{n - 1}, got: #{inspect(tuple)}"
      end

      @doc false
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
      defp unquote(extension)(unquote(pattern), unquote(sub_oids), unquote(sub_types))
           when unquote(guard) do
        unquote(body)
      end
    end
  end

  defp encode_super(extension, pattern, sub_oids, sub_types, body) do
    quote do
      defp unquote(extension)(unquote(pattern), unquote(sub_oids), unquote(sub_types)) do
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
      defp unquote(extension)(@null, _sub_oids, _sub_types), do: <<-1::int32()>>
    end
  end

  defp encode_null(extension, _) do
    quote do
      defp unquote(extension)(@null), do: <<-1::int32()>>
    end
  end

  defp encode_value(extension, :super_binary) do
    quote do
      @doc false
      def encode_value(value, {unquote(extension), sub_oids, sub_types}) do
        unquote(extension)(value, sub_oids, sub_types)
      end
    end
  end

  defp encode_value(extension, _) do
    quote do
      @doc false
      def encode_value(value, unquote(extension)) do
        unquote(extension)(value)
      end
    end
  end

  defp decode(config, define_opts) do
    rest = quote do: rest
    acc = quote do: acc
    rem = quote do: rem
    full = quote do: full
    rows = quote do: rows

    row_dispatch =
      for {extension, {_, [_ | _], format}} <- config do
        decode_row_dispatch(extension, format, rest, acc, rem, full, rows)
      end

    next_dispatch = decode_rows_dispatch(rest, acc, rem, full, rows)
    row_dispatch = row_dispatch ++ next_dispatch

    decodes =
      for {extension, {opts, [_ | _], format}} <- config do
        decode = extension.decode(opts)

        clauses =
          for clause <- decode do
            decode_type(extension, format, clause, row_dispatch, rest, acc, rem, full, rows)
          end

        null_clauses = decode_null(extension, format, row_dispatch, rest, acc, rem, full, rows)

        quote location: :keep do
          unquote(clauses |> maybe_rewrite(extension, decode, define_opts))

          unquote(null_clauses)
        end
      end

    quote location: :keep do
      unquote(decode_rows(row_dispatch, rest, acc, rem, full, rows))

      unquote(decode_simple())

      unquote(decode_list(config))

      unquote(decode_tuple(config))

      unquote(decodes)
    end
  end

  defp decode_rows(dispatch, rest, acc, rem, full, rows) do
    quote location: :keep, generated: true do
      @doc false
      def decode_rows(binary, types, rows) do
        decode_rows(binary, byte_size(binary), types, rows)
      end

      defp decode_rows(
             <<?D, size::int32(), _::int16(), unquote(rest)::binary>>,
             rem,
             unquote(full),
             unquote(rows)
           )
           when rem > size do
        unquote(rem) = rem - (1 + size)
        unquote(acc) = []

        case unquote(full) do
          unquote(dispatch)
        end
      end

      defp decode_rows(<<?D, size::int32(), rest::binary>>, rem, _, rows) do
        more = size + 1 - rem
        {:more, [?D, <<size::int32()>> | rest], rows, more}
      end

      defp decode_rows(<<?D, rest::binary>>, _, _, rows) do
        {:more, [?D | rest], rows, 0}
      end

      defp decode_rows(<<rest::binary-size(0)>>, _, _, rows) do
        {:more, [], rows, 0}
      end

      defp decode_rows(<<rest::binary>>, _, _, rows) do
        {:ok, rows, rest}
      end
    end
  end

  defp decode_row_dispatch(extension, :super_binary, rest, acc, rem, full, rows) do
    [clause] =
      quote do
        [{unquote(extension), sub_oids, sub_types} | types] ->
          unquote(extension)(
            unquote(rest),
            sub_oids,
            sub_types,
            types,
            unquote(acc),
            unquote(rem),
            unquote(full),
            unquote(rows)
          )
      end

    clause
  end

  defp decode_row_dispatch(extension, _, rest, acc, rem, full, rows) do
    [clause] =
      quote do
        [unquote(extension) | types2] ->
          unquote(extension)(
            unquote(rest),
            types2,
            unquote(acc),
            unquote(rem),
            unquote(full),
            unquote(rows)
          )
      end

    clause
  end

  defp decode_rows_dispatch(rest, acc, rem, full, rows) do
    quote do
      [] ->
        rows = [Enum.reverse(unquote(acc)) | unquote(rows)]
        decode_rows(unquote(rest), unquote(rem), unquote(full), rows)
    end
  end

  defp decode_simple() do
    rest = quote do: rest
    acc = quote do: acc

    dispatch = decode_simple_dispatch(Postgrex.Extensions.Raw, rest, acc)

    quote do
      @doc false
      def decode_simple(binary) do
        decode_simple(binary, [])
      end

      defp decode_simple(<<>>, unquote(acc)), do: Enum.reverse(acc)
      defp decode_simple(<<unquote(rest)::binary>>, unquote(acc)), do: unquote(dispatch)
    end
  end

  defp decode_simple_dispatch(extension, rest, acc) do
    quote do
      unquote(extension)(unquote(rest), unquote(acc), &decode_simple/2)
    end
  end

  defp decode_list(config) do
    rest = quote do: rest

    dispatch =
      for {extension, {_, [_ | _], format}} <- config do
        decode_list_dispatch(extension, format, rest)
      end

    quote do
      @doc false
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
    n = quote do: n
    acc = quote do: acc

    dispatch =
      for {extension, {_, [_ | _], format}} <- config do
        decode_tuple_dispatch(extension, format, rest, oids, n, acc)
      end

    quote generated: true do
      @doc false
      def decode_tuple(<<rest::binary>>, count, types) when is_integer(count) do
        decode_tuple(rest, count, types, 0, [])
      end

      def decode_tuple(<<rest::binary>>, oids, types) do
        decode_tuple(rest, oids, types, 0, [])
      end

      defp decode_tuple(
             <<oid::int32(), unquote(rest)::binary>>,
             [oid | unquote(oids)],
             types,
             unquote(n),
             unquote(acc)
           ) do
        case types do
          unquote(dispatch)
        end
      end

      defp decode_tuple(<<>>, [], [], n, acc) do
        :erlang.make_tuple(n, @null, acc)
      end

      defp decode_tuple(
             <<oid::int32(), unquote(rest)::binary>>,
             rem,
             types,
             unquote(n),
             unquote(acc)
           )
           when rem > 0 do
        case Postgrex.Types.fetch(oid, types) do
          {:ok, {:binary, type}} ->
            unquote(oids) = rem - 1

            case [type | types] do
              unquote(dispatch)
            end

          {:ok, {:text, _}} ->
            msg =
              "oid `#{oid}` was bootstrapped in text format and can not " <>
                "be decoded inside an anonymous record"

            raise RuntimeError, msg

          {:error, %TypeInfo{type: pg_type}, _mod} ->
            msg = "type `#{pg_type}` can not be handled by the configured extensions"
            raise RuntimeError, msg

          {:error, nil, _mod} ->
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
          unquote(extension)(
            unquote(rest),
            sub_oids,
            sub_types,
            unquote(oids),
            types,
            unquote(n) + 1,
            unquote(acc)
          )
      end

    clause
  end

  defp decode_tuple_dispatch(extension, _, rest, oids, n, acc) do
    [clause] =
      quote do
        [unquote(extension) | types] ->
          unquote(extension)(unquote(rest), unquote(oids), types, unquote(n) + 1, unquote(acc))
      end

    clause
  end

  defp decode_type(extension, :super_binary, clause, dispatch, rest, acc, rem, full, rows) do
    decode_super(extension, clause, dispatch, rest, acc, rem, full, rows)
  end

  defp decode_type(extension, _, clause, dispatch, rest, acc, rem, full, rows) do
    decode_extension(extension, clause, dispatch, rest, acc, rem, full, rows)
  end

  defp decode_null(extension, :super_binary, dispatch, rest, acc, rem, full, rows) do
    decode_super_null(extension, dispatch, rest, acc, rem, full, rows)
  end

  defp decode_null(extension, _, dispatch, rest, acc, rem, full, rows) do
    decode_extension_null(extension, dispatch, rest, acc, rem, full, rows)
  end

  defp decode_extension(extension, clause, dispatch, rest, acc, rem, full, rows) do
    case split_extension(clause) do
      {pattern, guard, body} ->
        decode_extension(extension, pattern, guard, body, dispatch, rest, acc, rem, full, rows)

      {pattern, body} ->
        decode_extension(extension, pattern, body, dispatch, rest, acc, rem, full, rows)
    end
  end

  defp decode_extension(extension, pattern, guard, body, dispatch, rest, acc, rem, full, rows) do
    quote do
      defp unquote(extension)(
             <<unquote(pattern), unquote(rest)::binary>>,
             types,
             acc,
             unquote(rem),
             unquote(full),
             unquote(rows)
           )
           when unquote(guard) do
        unquote(acc) = [unquote(body) | acc]

        case types do
          unquote(dispatch)
        end
      end

      defp unquote(extension)(<<unquote(pattern), rest::binary>>, acc)
           when unquote(guard) do
        unquote(extension)(rest, [unquote(body) | acc])
      end

      defp unquote(extension)(<<unquote(pattern), rest::binary>>, acc, callback)
           when unquote(guard) do
        unquote(extension)(rest, [unquote(body) | acc], callback)
      end

      defp unquote(extension)(<<unquote(pattern), rest::binary>>, oids, types, n, acc)
           when unquote(guard) do
        decode_tuple(rest, oids, types, n, [{n, unquote(body)} | acc])
      end
    end
  end

  defp decode_extension(extension, pattern, body, dispatch, rest, acc, rem, full, rows) do
    quote do
      defp unquote(extension)(
             <<unquote(pattern), unquote(rest)::binary>>,
             types,
             acc,
             unquote(rem),
             unquote(full),
             unquote(rows)
           ) do
        unquote(acc) = [unquote(body) | acc]

        case types do
          unquote(dispatch)
        end
      end

      defp unquote(extension)(<<unquote(pattern), rest::binary>>, acc) do
        decoded = unquote(body)
        unquote(extension)(rest, [decoded | acc])
      end

      defp unquote(extension)(<<unquote(pattern), rest::binary>>, acc, callback) do
        decoded = unquote(body)
        unquote(extension)(rest, [decoded | acc], callback)
      end

      defp unquote(extension)(<<unquote(pattern), rest::binary>>, oids, types, n, acc) do
        decode_tuple(rest, oids, types, n, [{n, unquote(body)} | acc])
      end
    end
  end

  defp decode_extension_null(extension, dispatch, rest, acc, rem, full, rows) do
    quote do
      defp unquote(extension)(
             <<-1::int32(), unquote(rest)::binary>>,
             types,
             acc,
             unquote(rem),
             unquote(full),
             unquote(rows)
           ) do
        unquote(acc) = [@null | acc]

        case types do
          unquote(dispatch)
        end
      end

      defp unquote(extension)(<<-1::int32(), rest::binary>>, acc) do
        unquote(extension)(rest, [@null | acc])
      end

      defp unquote(extension)(<<>>, acc) do
        acc
      end

      defp unquote(extension)(<<-1::int32(), rest::binary>>, acc, callback) do
        unquote(extension)(rest, [@null | acc], callback)
      end

      defp unquote(extension)(<<rest::binary-size(0)>>, acc, callback) do
        callback.(rest, acc)
      end

      defp unquote(extension)(<<-1::int32(), rest::binary>>, oids, types, n, acc) do
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

  defp decode_super(extension, clause, dispatch, rest, acc, rem, full, rows) do
    case split_super(clause) do
      {pattern, oids, types, guard, body} ->
        decode_super(
          extension,
          pattern,
          oids,
          types,
          guard,
          body,
          dispatch,
          rest,
          acc,
          rem,
          full,
          rows
        )

      {pattern, oids, types, body} ->
        decode_super(extension, pattern, oids, types, body, dispatch, rest, acc, rem, full, rows)
    end
  end

  defp decode_super(
         extension,
         pattern,
         sub_oids,
         sub_types,
         guard,
         body,
         dispatch,
         rest,
         acc,
         rem,
         full,
         rows
       ) do
    quote do
      defp unquote(extension)(
             <<unquote(pattern), unquote(rest)::binary>>,
             unquote(sub_oids),
             unquote(sub_types),
             types,
             acc,
             unquote(rem),
             unquote(full),
             unquote(rows)
           )
           when unquote(guard) do
        unquote(acc) = [unquote(body) | acc]

        case types do
          unquote(dispatch)
        end
      end

      defp unquote(extension)(
             <<unquote(pattern), rest::binary>>,
             unquote(sub_oids),
             unquote(sub_types),
             acc
           )
           when unquote(guard) do
        acc = [unquote(body) | acc]
        unquote(extension)(rest, unquote(sub_oids), unquote(sub_types), acc)
      end

      defp unquote(extension)(
             <<unquote(pattern), rest::binary>>,
             unquote(sub_oids),
             unquote(sub_types),
             oids,
             types,
             n,
             acc
           )
           when unquote(guard) do
        decode_tuple(rest, oids, types, n, [{n, unquote(body)} | acc])
      end
    end
  end

  defp decode_super(
         extension,
         pattern,
         sub_oids,
         sub_types,
         body,
         dispatch,
         rest,
         acc,
         rem,
         full,
         rows
       ) do
    quote do
      defp unquote(extension)(
             <<unquote(pattern), unquote(rest)::binary>>,
             unquote(sub_oids),
             unquote(sub_types),
             types,
             acc,
             unquote(rem),
             unquote(full),
             unquote(rows)
           ) do
        unquote(acc) = [unquote(body) | acc]

        case types do
          unquote(dispatch)
        end
      end

      defp unquote(extension)(
             <<unquote(pattern), rest::binary>>,
             unquote(sub_oids),
             unquote(sub_types),
             acc
           ) do
        acc = [unquote(body) | acc]
        unquote(extension)(rest, unquote(sub_oids), unquote(sub_types), acc)
      end

      defp unquote(extension)(
             <<unquote(pattern), rest::binary>>,
             unquote(sub_oids),
             unquote(sub_types),
             oids,
             types,
             n,
             acc
           ) do
        acc = [{n, unquote(body)} | acc]
        decode_tuple(rest, oids, types, n, acc)
      end
    end
  end

  defp decode_super_null(extension, dispatch, rest, acc, rem, full, rows) do
    quote do
      defp unquote(extension)(
             <<-1::int32(), unquote(rest)::binary>>,
             _sub_oids,
             _sub_types,
             types,
             acc,
             unquote(rem),
             unquote(full),
             unquote(rows)
           ) do
        unquote(acc) = [@null | acc]

        case types do
          unquote(dispatch)
        end
      end

      defp unquote(extension)(<<-1::int32(), rest::binary>>, sub_oids, sub_types, acc) do
        unquote(extension)(rest, sub_oids, sub_types, [@null | acc])
      end

      defp unquote(extension)(<<>>, _sub_oid, _sub_types, acc) do
        acc
      end

      defp unquote(extension)(
             <<-1::int32(), rest::binary>>,
             _sub_oids,
             _sub_types,
             oids,
             types,
             n,
             acc
           ) do
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
    Enum.map(extensions ++ defaults, &configure/1)
  end

  defp configure({extension, opts}) do
    state = extension.init(opts)
    matching = extension.matching(state)
    format = extension.format(state)
    {extension, {state, matching, format}}
  end

  defp configure(extension) do
    configure({extension, []})
  end

  defp define_inline(module, config, opts) do
    quoted = [
      directives(config, opts),
      find(config),
      encode(config, opts),
      decode(config, opts)
    ]

    Module.create(module, quoted, Macro.Env.location(__ENV__))
  end
end
