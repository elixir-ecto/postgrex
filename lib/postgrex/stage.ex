defmodule Postgrex.Stage do
  @timeout 15_000
  @max_rows 500

  @spec copy(GenServer.server, iodata | Postgrex.Query.t, list, Keyword.t) ::
    GenServer.on_start
  def copy(pool, query, params, opts \\ []) do
    start = &copy_start(&1, query, params, opts)
    handle_events = &copy_handle(&1, &2, &3, opts)
    stop = &copy_stop(&1, &2, &3, opts)
    consumer(pool, start, handle_events, stop, opts)
  end

  @spec stream(GenServer.server, iodata | Postgrex.Query.t, list, Keyword.t) ::
    GenServer.on_start
  def stream(pool, query, params, opts \\ [])  do
    opts =
      opts
      |> defaults()
      |> Keyword.put_new(:max_rows, @max_rows)
    case query do
      %Postgrex.Query{} ->
        DBConnection.Stage.stream(pool, query, params, opts)
      statement ->
        query_struct = %Postgrex.Query{name: "", statement: statement}
        opts = Keyword.put(opts, :functions, :prepare_declare)
        DBConnection.Stage.prepare_stream(pool, query_struct, params, opts)
    end
  end

  @spec producer(GenServer.server, ((DBConnection.t) -> acc),
    ((DBConnection.t, demand :: pos_integer, acc) -> {[any], acc}),
    ((DBConnection.t, reason :: any, acc) -> any), Keyword.t) ::
    GenServer.on_start when acc: var
  def producer(pool, start, handle_demand, stop, opts \\ []) do
    opts = defaults(opts)
    DBConnection.Stage.producer(pool, start, handle_demand, stop, opts)
  end

  @spec producer_consumer(GenServer.server, ((DBConnection.t) -> acc),
    ((DBConnection.t, events_in :: [any], acc) -> {events_out :: [any], acc}),
    ((DBConnection.t, reason :: any, acc) -> any), Keyword.t) ::
    GenServer.on_start when acc: var
  def producer_consumer(pool, start, handle_events, stop, opts \\ []) do
    opts = defaults(opts)
    DBConnection.Stage.producer_consumer(pool, start, handle_events, stop, opts)
  end

  @spec consumer(GenServer.server, ((DBConnection.t) -> acc),
    ((DBConnection.t, events_in :: [any], acc) -> {[], acc}),
    ((DBConnection.t, reason :: any, acc) -> any), Keyword.t) ::
    GenServer.on_start when acc: var
  def consumer(pool, start, handle_events, stop, opts \\ []) do
    opts = defaults(opts)
    DBConnection.Stage.consumer(pool, start, handle_events, stop, opts)
  end

  ## Helpers

  defp defaults(opts) do
    Keyword.put_new(opts, :timeout, @timeout)
  end

  defp copy_start(conn, query, params, opts) do
    stream = %Postgrex.Stream{conn: conn, query: query, params: params,
                              options: opts}
    case query do
      %Postgrex.Query{} ->
        DBConnection.execute!(conn, stream, params, opts)
      statement ->
        query_struct = %Postgrex.Query{name: "", statement: statement}
        stream = %Postgrex.Stream{stream | query: query_struct}
        opts = Keyword.put(opts, :function, :prepare_into)
        {_, copy} = DBConnection.prepare_execute!(conn, stream, params, opts)
        copy
    end
  end

  defp copy_handle(conn, data, %Postgrex.Copy{ref: ref} = copy, opts) do
    copy_data = %Postgrex.CopyData{ref: ref, data: data}
    _ = DBConnection.execute!(conn, copy, copy_data, opts)
    {[], copy}
  end

  defp copy_stop(conn, reason, %Postgrex.Copy{ref: ref} = copy, opts) do
    copy_done = %Postgrex.CopyDone{ref: ref}
    _ = DBConnection.execute!(conn, copy, copy_done, opts)
    case reason do
      :normal -> :ok
      reason  -> DBConnection.rollback(conn, reason)
    end
  end
end
