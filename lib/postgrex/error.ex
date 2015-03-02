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

  def exception(arg) do
    super(arg)
  end
end
