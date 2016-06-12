defmodule Postgrex.Stream do
  defstruct [:conn, :options, :params, :portal, :query, :ref, state: :bind, max_rows: 500]
end

defmodule Postgrex.CopyData do
  defstruct [:data]
end

defimpl Enumerable, for: Postgrex.Stream do
  def reduce(stream, acc, fun) do
    Stream.resource(fn() -> start(stream) end, &next/1, &close/1).(acc, fun)
  end

  def member?(_, _) do
    {:error, __MODULE__}
  end

  def count(_) do
    {:error, __MODULE__}
  end

  defp start(stream) do
    %Postgrex.Stream{conn: conn, params: params, options: options} = stream
    stream = maybe_generate_portal(stream)
    _ = Postgrex.execute!(conn, stream, params, options)
    %Postgrex.Stream{stream | state: :execute}
  end

  defp next(%Postgrex.Stream{state: :done} = stream) do
    {:halt, stream}
  end
  defp next(stream) do
    %Postgrex.Stream{conn: conn, params: params, options: options,
                     state: state} = stream
    case Postgrex.execute!(conn, stream, params, options) do
      %Postgrex.Result{command: :stream} = result when state == :execute ->
        {[result], %Postgrex.Stream{stream | state: :suspended}}
      %Postgrex.Result{command: :stream} = result when state == :suspended ->
        {[result], stream}
      %Postgrex.Result{command: :copy_stream} = result when state == :execute ->
        {[result], %Postgrex.Stream{stream | state: :copy_out}}
      %Postgrex.Result{command: :copy_stream} = result when state == :copy_out ->
        {[result], stream}
      %Postgrex.Result{rows: [_|_]} = result ->
        {[result], %Postgrex.Stream{stream | state: :done}}
      %Postgrex.Result{} ->
        {:halt, %Postgrex.Stream{stream | state: :done}}
    end
  end

  defp close(%Postgrex.Stream{conn: conn, options: options} = stream) do
    DBConnection.close(conn, stream, options)
  end

  defp maybe_generate_portal(%Postgrex.Stream{portal: nil} = stream) do
    ref = make_ref()
    %Postgrex.Stream{stream | portal: inspect(ref), ref: ref}
  end
  defp maybe_generate_portal(stream) do
    %Postgrex.Stream{stream | ref: make_ref()}
  end
end

defimpl Collectable, for: Postgrex.Stream do
  def into(stream) do
   {:ok, make_into(stream)}
  end

  defp make_into(stream) do
    %Postgrex.Stream{conn: conn, params: params, options: options} = stream
    copy_stream = %Postgrex.Stream{stream | state: :copy_in}
    fn
      :ok, {:cont, data} ->
        data = %Postgrex.CopyData{data: data}
        _ = Postgrex.execute!(conn, copy_stream, [data | params], options)
        :ok
      :ok, :done ->
        stream
      :ok, :halt ->
        :ok
    end
  end
end

defimpl DBConnection.Query, for: Postgrex.Stream do
  def parse(stream, _) do
    raise "can not prepare #{inspect stream}"
  end

  def describe(stream, _) do
    raise "can not describe #{inspect stream}"
  end

  def encode(%Postgrex.Stream{query: query, state: :bind}, params, opts) do
    DBConnection.Query.encode(query, params, opts)
  end

  def encode(%Postgrex.Stream{state: state}, params, _)
      when state in [:execute, :suspended, :copy_out] do
    params
  end

  def encode(%Postgrex.Stream{query: query, state: :copy_in}, [data | params], opts) do
    %Postgrex.CopyData{data: data} = data
    try do
      IO.iodata_length(data)
    rescue
      ArgumentError ->
        raise "expected to iodata to copy to database, got: " <> inspect(data)
    else
      _ ->
        [data | DBConnection.Query.encode(query, params, opts)]
    end
  end

  def decode(%Postgrex.Stream{state: :bind}, result, _) do
    result
  end
  def decode(%Postgrex.Stream{query: query}, result, opts) do
    DBConnection.Query.decode(query, result, opts)
  end
end

defimpl String.Chars, for: Postgrex.Stream do
  def to_string(%Postgrex.Stream{query: query}) do
    String.Chars.to_string(query)
  end
end
