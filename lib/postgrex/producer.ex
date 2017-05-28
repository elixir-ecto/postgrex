defmodule Postgrex.Producer do
  @moduledoc """
  A `GenStage` producer that streams the result of a query.
  """

  @pool_timeout 5000
  @timeout 15_000
  @max_rows 500

  @doc """
  Start and link to a `GenStage` producer that streams the result of a query.

  ### Options

    * `:pool_timeout` - Time to wait in the queue for the connection
    (default: `#{@pool_timeout}`)
    * `:queue` - Whether to wait for connection in a queue (default: `true`);
    * `:timeout` - Execute request timeout (default: `#{@timeout}`);
    * `:max_rows` - Maximum numbers of rows in a result (default to `#{@max_rows}`)
    * `:stream_mapper` - A function to flat map the results of the query, either
    a 2-arity fun, `{module, function, args}` with `DBConnection.t` and
    `Postgrex.Result.t` prepended to `args` (default:
    `fn(_conn, %Postgrex.Result{rows: rows}) -> rows || [] end`);
    * `:decode_mapper` - Fun to map each row in the result to a term after
    decoding, (default: `fn x -> x end`);
    * `:pool` - The pool module to use, must match that set on
    `DBConnection.start_link/1`, see `DBConnection`
    * `:mode` - set to `:savepoint` to use a savepoint to rollback to before the
    execute on error, otherwise set to `:transaction` (default: `:transaction`);

  The pool may support other options.
  """
  @spec start_link(GenServer.server, iodata | Postgrex.Query.t, list, Keyword.t) ::
    GenServer.on_start
  def start_link(pool, query, params, opts \\ []) do
    opts =
      opts
      |> Keyword.put_new(:timeout, @timeout)
      |> Keyword.put_new(:max_rows, @max_rows)
      |> Keyword.put(:stage_transaction, true)
      |> ensure_stream_map()
    case query do
      %Postgrex.Query{} ->
        opts = [stage_prepare: false] ++ opts
        DBConnection.Producer.start_link(pool, query, params, opts)
      statement ->
        query_struct = %Postgrex.Query{name: "", statement: statement}
        opts = [function: :prepare_declare, stage_prepare: true] ++ opts
        DBConnection.Producer.start_link(pool, query_struct, params, opts)
    end
  end

  @doc false
  def stream_mapper(_, %Postgrex.Result{rows: rows}), do: rows || []

  ## Helpers

  defp ensure_stream_map(opts) do
    case Keyword.get(opts, :stream_mapper) do
      nil ->
        Keyword.put(opts, :stream_mapper, {__MODULE__, :stream_mapper, []})
      _ ->
        opts
    end
  end
end
