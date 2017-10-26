defmodule Postgrex.Stream do
  defstruct [:conn, :query, :params, :options, max_rows: 500]
  @type t :: %Postgrex.Stream{}
end
defmodule Postgrex.Cursor do
  defstruct [:portal, :ref, :connection_id, :max_rows]
  @type t :: %Postgrex.Cursor{}
end
defmodule Postgrex.Copy do
  defstruct [:portal, :ref, :connection_id, :query]
  @type t :: %Postgrex.Copy{}
end
defmodule Postgrex.CopyData do
  defstruct [:data, :ref]
  @type t :: %Postgrex.CopyData{}
end
defmodule Postgrex.CopyDone do
  defstruct [:ref]
  @type t :: %Postgrex.CopyDone{}
end

defimpl Enumerable, for: Postgrex.Stream do
  alias Postgrex.Query
  def reduce(%Postgrex.Stream{query: %Query{} = query} = stream, acc, fun) do
    %Postgrex.Stream{conn: conn, params: params, options: opts} = stream
    stream = %DBConnection.Stream{conn: conn, query: query, params: params,
                                  opts: opts}
    DBConnection.reduce(stream, acc, fun)
  end
  def reduce(%Postgrex.Stream{query: statement} = stream, acc, fun) do
    %Postgrex.Stream{conn: conn, params: params, options: opts} = stream
    query = %Query{name: "" , statement: statement}
    opts = Keyword.put(opts, :function, :prepare_open)
    stream = %DBConnection.PrepareStream{conn: conn, query: query,
                                         params: params, opts: opts}
    DBConnection.reduce(stream, acc, fun)
  end

  def member?(_, _) do
    {:error, __MODULE__}
  end

  def count(_) do
    {:error, __MODULE__}
  end

  def slice(_) do
    {:error, __MODULE__}
  end
end

defimpl Collectable, for: Postgrex.Stream do
  alias Postgrex.Stream
  alias Postgrex.Query

  def into(%Stream{conn: %DBConnection{}} = stream) do
    %Stream{conn: conn, query: query, params: params, options: opts} = stream
    case query do
      %Query{} ->
        copy = DBConnection.execute!(conn, stream, params, opts)
        {:ok, make_into(conn, stream, copy, opts)}
      query ->
        internal = %Stream{stream | query: %Query{name: "", statement: query}}
        opts = Keyword.put(opts, :function, :prepare_into)
        {_, copy} = DBConnection.prepare_execute!(conn, internal, params, opts)
        {:ok, make_into(conn, stream, copy, opts)}
    end
  end
  def into(_) do
    msg = "data can only be copied to database inside a transaction"
    raise ArgumentError, msg
  end

  defp make_into(conn, stream, %Postgrex.Copy{ref: ref} = copy, opts) do
    fn
      :ok, {:cont, data} ->
        copy_data = %Postgrex.CopyData{ref: ref, data: data}
        _ = DBConnection.execute!(conn, copy, copy_data, opts)
        :ok
      :ok, close when close in [:done, :halt] ->
        copy_done = %Postgrex.CopyDone{ref: ref}
        _ = DBConnection.execute!(conn, copy, copy_done, opts)
        stream
    end
  end
end

defimpl DBConnection.Query, for: Postgrex.Stream do
  alias Postgrex.Stream

  def parse(%Stream{query: query} = stream, opts) do
    %Stream{stream | query: DBConnection.Query.parse(query, opts)}
  end

  def describe(%Stream{query: query} = stream, opts) do
    %Stream{stream | query: DBConnection.Query.describe(query, opts)}
  end

  def encode(%Stream{query: query}, params, opts) do
    DBConnection.Query.encode(query, params, opts)
  end

  def decode(_, copy, _), do: copy
end

defimpl DBConnection.Query, for: Postgrex.Copy do
  alias Postgrex.Copy
  import Postgrex.Messages

  def parse(copy, _) do
    raise "can not prepare #{inspect copy}"
  end

  def describe(copy, _) do
    raise "can not describe #{inspect copy}"
  end

  def encode(%Copy{ref: ref}, %Postgrex.CopyData{data: data, ref: ref}, _) do
    try do
      encode_msg(msg_copy_data(data: data))
    rescue
      ArgumentError ->
        raise ArgumentError,
          "expected iodata to copy to database, got: " <> inspect(data)
    else
      iodata ->
        {:copy_data, iodata}
    end
  end

  def encode(%Copy{ref: ref}, %Postgrex.CopyDone{ref: ref}, _) do
    :copy_done
  end

  def decode(%Copy{query: query}, result, opts) do
    case result do
      %Postgrex.Result{command: :copy_stream} ->
        result
      %Postgrex.Result{command: :close} ->
        result
      _ ->
        DBConnection.Query.decode(query, result, opts)
    end
  end
end

defimpl String.Chars, for: Postgrex.Stream do
  def to_string(%Postgrex.Stream{query: query}) do
    String.Chars.to_string(query)
  end
end

defimpl String.Chars, for: Postgrex.Copy do
  def to_string(%Postgrex.Copy{query: query}) do
    String.Chars.to_string(query)
  end
end
