defmodule Postgrex.Error do
  defexception [:message, :postgres, :connection_id]

  @nonposix_errors [:closed, :timeout]
  @metadata [:table, :column, :constraint]

  def message(e) do
    if kw = e.postgres do
      "#{kw[:severity]} (#{kw[:code]}): #{kw[:message]}"
      <> build_metadata(kw)
      <> build_detail(kw)
    else
      e.message
    end
  end

  defp build_metadata(kw) do
    metadata = for k <- @metadata, v = kw[k], do: "\n    #{k}: #{v}"
    case metadata do
      [] -> ""
      _  -> "\n" <> Enum.join(metadata)
    end
  end

  defp build_detail(kw) do
    if v = kw[:detail], do: "\n\n" <> v, else: ""
  end

  def exception([postgres: fields]) do
    fields = Enum.into(fields, %{})
             |> Map.put(:pg_code, fields[:code])
             |> Map.update!(:code, &Postgrex.ErrorCode.code_to_name/1)

    %Postgrex.Error{postgres: fields}
  end

  def exception([tag: :ssl, action: action, reason: :timeout]) do
    %Postgrex.Error{message: "ssl #{action}: timeout"}
  end

  def exception([tag: :ssl, action: action, reason: reason]) do
    formatted_reason = :ssl.format_error(reason)
    %Postgrex.Error{message: "ssl #{action}: #{formatted_reason} - #{inspect(reason)}"}
  end

  def exception([tag: :tcp, action: action, reason: reason]) when not reason in @nonposix_errors do
    formatted_reason = :inet.format_error(reason)
    %Postgrex.Error{message: "tcp #{action}: #{formatted_reason} - #{inspect(reason)}"}
  end

  def exception([tag: :tcp, action: action, reason: reason]) do
    %Postgrex.Error{message: "tcp #{action}: #{reason}"}
  end

  def exception(arg) do
    super(arg)
  end
end
