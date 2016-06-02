defmodule Benchmark do

  def log(e) do
    times = [ :pool_time, :connection_time, :decode_time ]
      |> Enum.map(&(System.convert_time_unit(Map.get(e, &1) || 0, :native, :micro_seconds)))
    [ e.call | times ]
      |> Enum.join(" ")
      |> IO.puts
  end

  def timestamp?(%Postgrex.Timestamp{}), do: true
  def timestamp?(_), do: false

  def csv(rows, file, f) do
    Enum.each(rows, fn (row) ->
      line = row
        |> Enum.reject(fn (col) -> timestamp?(col) end)
        |> Enum.join(",")
      f.(file, line <> "\n")
    end)
  end

  def execute(pid, q, options \\ []) do
    micro_seconds(fn ->
      File.open("/tmp/execute.csv", [ :write, :raw, :delayed_write ], fn (f) ->
        {:ok, %{rows: rows}}= Postgrex.execute(pid, q, [], options)
        csv(rows, f, &IO.binwrite/2)
      end)
    end)
  end

  def stream(pid, q, options \\ []) do
    micro_seconds(fn ->
      File.open("/tmp/stream.csv", [ :write, :raw, :delayed_write ], fn (f) ->
        Postgrex.transaction(pid, fn(pid) ->
          Postgrex.stream(pid, q, [], options) |>
            Enum.each(fn (%{rows: rows }) -> csv(rows, f, &IO.binwrite/2) end)
          end, timeout: :infinity)
      end)
    end)
  end

	defp micro_seconds(f) do
    {time, _ }= :timer.tc(f)
    time
	end

end
