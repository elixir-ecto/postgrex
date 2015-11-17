defmodule Postgrex.Error do
  defexception [:message, :postgres]

  @nonposix_errors [:closed, :timeout]

  def message(e) do
    if kw = e.postgres do
      "#{kw[:severity]} (#{kw[:code]}): #{kw[:message]}"
    else
      e.message
    end
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
