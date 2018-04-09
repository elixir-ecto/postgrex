defmodule Postgrex.Error do
  defexception [:message, :postgres, :connection_id, :query]

  @type t :: %Postgrex.Error{}

  @metadata [:table, :column, :constraint, :hint]

  def exception(opts) do
    postgres =
      if fields = Keyword.get(opts, :postgres) do
        code = fields[:code]

        fields
        |> Map.new()
        |> Map.put(:pg_code, code)
        |> Map.put(:code, Postgrex.ErrorCode.code_to_name(code))
      end

    message = Keyword.get(opts, :message)
    connection_id = Keyword.get(opts, :connection_id)
    %Postgrex.Error{postgres: postgres, message: message, connection_id: connection_id}
  end

  def message(e) do
    if kw = e.postgres do
      IO.iodata_to_binary([
        kw[:severity],
        ?\s,
        kw[:pg_code],
        ?\s,
        [?(, Atom.to_string(kw[:code]), ?)],
        ?\s,
        kw[:message],
        build_query(e.query),
        build_metadata(kw),
        build_detail(kw)
      ])
    else
      e.message
    end
  end

  defp build_query(nil), do: []
  defp build_query(query), do: ["\n\n    query: ", query]

  defp build_metadata(kw) do
    metadata = for k <- @metadata, v = kw[k], do: "\n    #{k}: #{v}"

    case metadata do
      [] -> []
      _  -> ["\n" | metadata]
    end
  end

  defp build_detail(kw) do
    if v = kw[:detail], do: ["\n\n" | v], else: []
  end
end
