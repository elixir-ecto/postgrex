defmodule Postgrex.Error do
  defexception [:message, :postgres]

  def message(e) do
    if kw = e.postgres do
      msg = "#{kw[:severity]} (#{kw[:code]}): #{kw[:message]}"
    end

    msg || e.message
  end

  def exception([postgres: fields]) do
    fields = Enum.into(fields, %{})
             |> Map.put(:pg_code, fields[:code])
             |> Map.update!(:code, &Postgrex.ErrorCode.code_to_name/1)

    %Postgrex.Error{postgres: fields}
  end

  def exception([tag: :ssl, action: action, reason: reason]) do
    reason = :ssl.format_error(reason)
    %Postgrex.Error{message: "ssl #{action}: #{reason}"}
  end

  def exception([tag: :tcp, action: action, reason: reason]) do
    reason = :inet.format_error(reason)
    %Postgrex.Error{message: "tcp #{action}: #{reason}"}
  end

  def exception(arg) do
    super(arg)
  end
end
