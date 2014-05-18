defmodule Postgrex.Error do
  defexception [:message, :postgres]

  def exception(opts) do
    if kw = opts[:postgres] do
      msg = "#{kw[:severity]} (#{kw[:code]}): #{kw[:message]}"
    end

    %Postgrex.Error{message: msg || opts[:message], postgres: opts[:postgres]}
  end
end
