defmodule Postgrex.Error do
  defexception [:message, :postgres, :connection_id, :query]

  @type t :: %Postgrex.Error{}

  @metadata [:table, :column, :constraint, :hint]

  def exception(opts) do
    postgres =
      if fields = Keyword.get(opts, :postgres) do
        code = fields.code

        fields
        |> Map.put(:pg_code, code)
        |> Map.put(:code, Postgrex.ErrorCode.code_to_name(code))
      end

    message = Keyword.get(opts, :message)
    connection_id = Keyword.get(opts, :connection_id)
    %Postgrex.Error{postgres: postgres, message: message, connection_id: connection_id}
  end

  def message(e) do
    if map = e.postgres do
      IO.iodata_to_binary([
        map.severity,
        ?\s,
        map.pg_code,
        ?\s,
        [?(, Atom.to_string(map.code), ?)],
        ?\s,
        map.message,
        build_query(e.query),
        build_metadata(map),
        build_detail(map)
      ])
    else
      e.message
    end
  end

  defp build_query(nil), do: []
  defp build_query(query), do: ["\n\n    query: ", query]

  defp build_metadata(map) do
    metadata = for k <- @metadata, v = map[k], do: "\n    #{k}: #{v}"

    case metadata do
      [] -> []
      _  -> ["\n" | metadata]
    end
  end

  defp build_detail(%{detail: detail}) when is_binary(detail), do: ["\n\n" | detail]
  defp build_detail(_), do: []
end

defmodule Postgrex.QueryError do
  defexception [:message]
end