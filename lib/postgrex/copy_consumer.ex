defmodule Postgrex.CopyConsumer do
  @moduledoc """
  A `GenStage` consumer that copies data with a query.
  """

  alias __MODULE__, as: CopyConsumer
  use GenStage

  @pool_timeout 5000
  @timeout 15_000

  @enforce_keys [:conn, :copy, :opts, :done?, :producers]
  defstruct [:conn, :copy, :opts, :done?, :producers]

  @start_opts [:name, :spawn_opt, :debug]
  @stage_opts [:subscribe_to]

  @doc """
  Start and link to a `GenStage` consumer that executes a query and copies
  data.

  ## Examples

      statement = "COPY posts FROM STDIN"
      {:ok, pid} = Postgrex.CopyConsumer.start_link(pool, statement, [])
      "posts"
      |> File.stream!()
      |> Flow.from_enumerable()
      |> Flow.into_stages([pid])

  ### Options

    * `:pool_timeout` - Time to wait in the queue for the connection
    (default: `#{@pool_timeout}`)
    * `:queue` - Whether to wait for connection in a queue (default: `true`);
    * `:timeout` - Execute request timeout (default: `#{@timeout}`);
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
    start_opts = Keyword.take(opts, @start_opts)
    args = {pool, query, params, opts}
    GenStage.start_link(__MODULE__, args, start_opts)
  end

  @doc false
  def init({pool, query, params, opts}) do
    stage_opts = Keyword.take(opts, @stage_opts)
    {:consumer, init(pool, query, params, opts), stage_opts}
  end

  @doc false
  def handle_subscribe(:producer, _, {pid, ref}, consumer) do
    case consumer do
      %CopyConsumer{done?: true} = consumer ->
        GenStage.cancel({pid, ref}, :normal, [:noconnect])
        {:manual, consumer}
      %CopyConsumer{done?: false, producers: producers} = consumer ->
        new_producers = Map.put(producers, ref, pid)
        {:automatic, %CopyConsumer{consumer | producers: new_producers}}
    end
  end

  @doc false
  def handle_cancel(_, {_, ref}, consumer) do
    %CopyConsumer{producers: producers} = consumer
    case Map.delete(producers, ref) do
      new_producers when new_producers == %{} and producers != %{} ->
        GenStage.async_info(self(), :stop)
        {:noreply, [], %CopyConsumer{consumer | done?: true, producers: %{}}}
      new_producers ->
        {:noreply, [], %CopyConsumer{consumer | producers: new_producers}}
    end
  end

  @doc false
  def handle_info(:stop, state) do
    {:stop, :normal, state}
  end
  def handle_info(_msg, state) do
    {:noreply, [], state}
  end

  @doc false
  def handle_events(data, _, consumer) do
    %CopyConsumer{conn: conn, copy: copy, opts: opts} = consumer
    copy_data = &copy_data(&1, data, copy, opts)
    case DBConnection.transaction(conn, copy_data, opts) do
      {:ok, _} ->
        {:noreply, [], consumer}
      {:error, reason} ->
        exit(reason)
    end
  end

  @doc false
  def terminate(reason, consumer) do
    %CopyConsumer{conn: conn, copy: copy, opts: opts} = consumer
    copy_stop = &copy_stop(&1, reason, copy, opts)
    case DBConnection.transaction(conn, copy_stop, opts) do
      {:ok, :normal} ->
        DBConnection.commit_checkin(conn, opts)
      {:ok, reason} ->
        DBConnection.rollback_checkin(conn, reason, opts)
      {:error, :rollback} ->
        :ok
    end
  end

  ## Helpers

  defp init(pool, query, params, opts) do
    conn = DBConnection.checkout_begin(pool, opts)
    bind = &bind(&1, query, params, opts)
    case DBConnection.transaction(conn, bind, opts) do
      {:ok, copy} ->
        %CopyConsumer{conn: conn, copy: copy, opts: opts, producers: %{},
                      done?: false}
      {:error, reason} ->
        exit(reason)
    end
  end

  defp bind(conn, %Postgrex.Query{} = query, params, opts) do
    stream = %Postgrex.Stream{conn: conn, query: query, params: params,
                              options: opts}
    DBConnection.execute!(conn, stream, params, opts)
  end
  defp bind(conn, statement, params, opts) do
    query = %Postgrex.Query{name: "", statement: statement}
    stream = %Postgrex.Stream{conn: conn, query: query, params: params,
                              options: opts}
    opts = Keyword.put(opts, :function, :prepare_into)
    {_, copy} = DBConnection.prepare_execute!(conn, stream, params, opts)
    copy
  end

  defp copy_data(conn, data, %Postgrex.Copy{ref: ref} = copy, opts) do
    try do
      copy_data = %Postgrex.CopyData{ref: ref, data: data}
      DBConnection.execute!(conn, copy, copy_data, opts)
    catch
      kind, reason ->
        stack = System.stacktrace()
        copy_stop = &copy_stop(&1, :raise, copy, opts)
        DBConnection.transaction(conn, copy_stop, opts)
        :erlang.raise(kind, reason, stack)
    end
  end

  defp copy_stop(conn, reason, %Postgrex.Copy{ref: ref} = copy, opts) do
    copy_done = %Postgrex.CopyDone{ref: ref}
    _ = DBConnection.execute!(conn, copy, copy_done, opts)
    reason
  end
end
