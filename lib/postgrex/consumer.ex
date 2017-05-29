defmodule Postgrex.Consumer do
  @moduledoc """
  A `GenStage` consumer that runs a transaction for every batch of events.
  """
  @pool_timeout 5000
  @timeout 15_000

  @doc """
  Start and link to a `GenStage` consumer that runs a transaction for every
  batch of events.

  ### Options

    * `:pool_timeout` - Time to wait in the queue for the connection
    (default: `#{@pool_timeout}`)
    * `:queue` - Whether to wait for connection in a queue (default: `true`);
    * `:timeout` - Execute request timeout (default: `#{@timeout}`);
    * `:pool` - The pool module to use, must match that set on
    `DBConnection.start_link/1`, see `DBConnection`
    * `:mode` - set to `:savepoint` to use a savepoint to rollback to before the
    execute on error, otherwise set to `:transaction` (default: `:transaction`);

  The pool may support other options.

  ## Examples

      copy = fn(conn, data) ->
        Enum.into(data, Postgrex.stream!(conn, "COPY posts FROM STDIN", []))
      end
      {:ok, pid} = Postgrex.Consumer.start_link(pool, copy, [])
      "posts"
      |> File.stream!()
      |> Flow.from_enumerable()
      |> Flow.into_stages([pid])
  """
  @spec start_link(GenServer.server, (DBConnection.t, list -> term), Keyword.t) ::
    GenServer.on_start
  def start_link(pool, fun, opts \\ []) do
    opts = [stage_transaction: true] ++ opts
    DBConnection.Consumer.start_link(pool, fun, opts)
  end
end
