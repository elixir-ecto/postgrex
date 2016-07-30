defmodule Postgrex.Stream do
  defstruct [:conn, :options, :params, :portal, :query, :ref, state: :bind, num_rows: 0, max_rows: 500]
  @type t :: %Postgrex.Stream{}
end

defmodule Postgrex.CopyData do
  defstruct [:query, :params, :ref]
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
    %Postgrex.Stream{stream | state: :out}
  end

  defp next(%Postgrex.Stream{state: :done} = stream) do
    {:halt, stream}
  end
  defp next(stream) do
    %Postgrex.Stream{conn: conn, params: params, options: options,
                     state: state, num_rows: num_rows} = stream
    case Postgrex.execute!(conn, stream, params, options) do
      %Postgrex.Result{command: :stream, rows: rows} = result
          when state in [:out, :suspended] ->
        stream =  %Postgrex.Stream{stream | state: :suspended,
                                            num_rows: num_rows + length(rows)}
        {[result], stream}
      %Postgrex.Result{command: :copy_stream} = result when state == :out ->
        {[result], %Postgrex.Stream{stream | state: :copy_out}}
      %Postgrex.Result{command: :copy_stream} = result when state == :copy_out ->
        {[result], stream}
      %Postgrex.Result{} = result ->
        {[result], %Postgrex.Stream{stream | state: :done}}
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
    %Postgrex.Stream{conn: conn, params: params, options: options} = stream
    copy_stream = %Postgrex.Stream{stream | state: :copy_in, ref: make_ref()}
    _ = Postgrex.execute!(conn, copy_stream, params, options)
    {:ok, make_into(copy_stream, stream)}
  end

  defp make_into(copy_stream, stream) do
    %Postgrex.Stream{conn: conn, query: query, params: params, ref: ref,
                     options: options} = copy_stream
    copy = %Postgrex.CopyData{query: query, params: params, ref: ref}
    fn
      :ok, {:cont, data} ->
        _ = Postgrex.execute!(conn, copy, data, options)
        :ok
      :ok, :done ->
        done_stream = %Postgrex.Stream{copy_stream | state: :copy_done}
        Postgrex.execute!(conn, done_stream, params, options)
        stream
      :ok, :halt ->
        fail_stream = %Postgrex.Stream{copy_stream | state: :copy_fail}
        Postgrex.execute(conn, fail_stream, params, options)
    end
  end
end

defimpl DBConnection.Query, for: Postgrex.Stream do
  require Postgrex.Messages

  def parse(stream, _) do
    raise "can not prepare #{inspect stream}"
  end

  def describe(stream, _) do
    raise "can not describe #{inspect stream}"
  end

  def encode(%Postgrex.Stream{query: %Postgrex.Query{types: nil} = query}, _, _) do
    raise ArgumentError, "query #{inspect query} has not been prepared"
  end

  def encode(%Postgrex.Stream{query: query, state: :bind}, params, opts) do
    case query do
      %Postgrex.Query{encoders: [_|_] = encoders, copy_data: true} ->
        {encoders, [:copy_data]} = Enum.split(encoders, -1)
        {params, _} = Enum.split(params, -1)
        query = %Postgrex.Query{query | encoders: encoders}
        DBConnection.Query.encode(query, params, opts)
      %Postgrex.Query{} = query ->
        DBConnection.Query.encode(query, params, opts)
    end
  end

  def encode(%Postgrex.Stream{state: :out, query: query}, params, _) do
    case query do
      %Postgrex.Query{copy_data: true} ->
        {_, [copy_data]} = Enum.split(params, -1)
        try do
          Postgrex.Messages.encode_msg(Postgrex.Messages.msg_copy_data(data: copy_data))
        rescue
          ArgumentError ->
            raise ArgumentError,
            "expected iodata to copy to database, got: " <> inspect(copy_data)
        end
      _ ->
        []
    end
  end

  def encode(%Postgrex.Stream{query: query, state: :copy_in}, params, opts) do
    case query do
      %Postgrex.Query{encoders: [_|_] = encoders, copy_data: true} ->
        {encoders, [:copy_data]} = Enum.split(encoders, -1)
        query = %Postgrex.Query{query | encoders: encoders}
        DBConnection.Query.encode(query, params, opts)
      %Postgrex.Query{} = query ->
        raise ArgumentError, "query #{inspect query} has not enabled copy data"
    end
  end

  def encode(%Postgrex.Stream{state: state}, _, _)
      when state in [:suspended, :copy_out, :copy_done, :copy_fail] do
    []
  end

  def decode(%Postgrex.Stream{state: state}, result, _)
      when state in [:bind, :copy_in] do
    result
  end
  def decode(%Postgrex.Stream{query: query}, result, opts) do
    DBConnection.Query.decode(query, result, opts)
  end
end

defimpl DBConnection.Query, for: Postgrex.CopyData do
  require Postgrex.Messages

  def parse(copy_data, _) do
    raise "can not prepare #{inspect copy_data}"
  end

  def describe(copy_data, _) do
    raise "can not describe #{inspect copy_data}"
  end

  def encode(_, data, _) do
    try do
      Postgrex.Messages.encode_msg(Postgrex.Messages.msg_copy_data(data: data))
    rescue
      ArgumentError ->
        raise ArgumentError,
          "expected iodata to copy to database, got: " <> inspect(data)
    end
  end

  def decode(_, result, _) do
    result
  end
end

defimpl String.Chars, for: Postgrex.Stream do
  def to_string(%Postgrex.Stream{query: query}) do
    String.Chars.to_string(query)
  end
end

defimpl String.Chars, for: Postgrex.CopyData do
  def to_string(%Postgrex.CopyData{query: query}) do
    String.Chars.to_string(query)
  end
end
