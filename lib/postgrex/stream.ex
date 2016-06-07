defmodule Postgrex.Stream do
  defstruct [:conn, :options, :params, :portal, :query, :state, :result, max_rows: 500]
end

defimpl Enumerable, for: Postgrex.Stream do
  def reduce(stream, acc, fun) do
    start = fn -> maybe_generate_portal(stream) end
    Stream.resource(start, &next/1, &close/1).(acc, fun)
  end

  def member?(_, _) do
    {:error, __MODULE__}
  end

  def count(_) do
    {:error, __MODULE__}
  end

  defp next(%Postgrex.Stream{state: :done} = stream) do
    {:halt, stream}
  end
  defp next(%Postgrex.Stream{conn: conn, params: params, options: options} = stream) do
    %Postgrex.Stream{result: result} = stream = Postgrex.execute!(conn, stream, params, options)
    emit_or_next(stream, result)
  end
  defp close(%Postgrex.Stream{conn: conn} = stream) do
    DBConnection.close(conn, stream)
  end

  defp emit_or_next(stream, %Postgrex.Result{rows: []}), do: next(stream)
  defp emit_or_next(stream, result),                     do: {[result], stream}

  defp maybe_generate_portal(%Postgrex.Stream{portal: nil} = stream),
    do: %Postgrex.Stream{stream | portal: :erlang.ref_to_list(make_ref())}
  defp maybe_generate_portal(stream),
    do: stream
end

defimpl DBConnection.Query, for: Postgrex.Stream do
  def parse(_query, _opts) do
    raise "can not prepare stream"
  end

  def describe(%Postgrex.Stream{query: query} = stream, opts) do
    %Postgrex.Stream{stream | query: DBConnection.Query.describe(query, opts)}
  end

  def encode(%Postgrex.Stream{query: query}, params, opts) do
    DBConnection.Query.encode(query, params, opts)
  end

  def decode(%Postgrex.Stream{query: query}, %Postgrex.Stream{result: result} = stream, opts) do
    result = DBConnection.Query.decode(query, result, opts)
    %Postgrex.Stream{stream | result: result}
  end
end

defimpl String.Chars, for: Postgrex.Stream do
  def to_string(%Postgrex.Stream{query: query}) do
    String.Chars.to_string(query)
  end
end
