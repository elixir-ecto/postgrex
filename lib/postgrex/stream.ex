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
  defp next(stream) do
    %Postgrex.Stream{conn: conn, params: params, options: options,
                     state: state} = stream
    case Postgrex.execute!(conn, stream, params, options) do
      %Postgrex.Result{command: :stream} = result when state == nil ->
        {[result], %Postgrex.Stream{stream | state: :suspended}}
      %Postgrex.Result{command: :stream} = result when state == :suspended ->
        {[result], stream}
      %Postgrex.Result{rows: []} ->
        {:halt, %Postgrex.Stream{stream | state: :done}}
      %Postgrex.Result{} = result ->
        {[result], %Postgrex.Stream{stream | state: :done}}
    end
  end

  defp close(%Postgrex.Stream{conn: conn, options: options} = stream) do
    DBConnection.close(conn, stream, options)
  end

  defp maybe_generate_portal(%Postgrex.Stream{portal: nil} = stream),
    do: %Postgrex.Stream{stream | portal: :erlang.ref_to_list(make_ref())}
  defp maybe_generate_portal(stream),
    do: stream
end

defimpl DBConnection.Query, for: Postgrex.Stream do
  def parse(stream, _) do
    raise "can not prepare #{inspect stream}"
  end

  def describe(stream, _) do
    raise "can not describe #{inspect stream}"
  end

  def encode(%Postgrex.Stream{query: query, state: nil}, params, opts) do
    DBConnection.Query.encode(query, params, opts)
  end

  def encode(%Postgrex.Stream{state: :suspended}, params, _) do
    params
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
