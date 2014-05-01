defexception Postgrex.Error, [:postgres, :reason] do
  def message(Postgrex.Error[postgres: kw]) when is_list(kw) do
    "#{kw[:severity]} (#{kw[:code]}): #{kw[:message]}"
  end

  def message(Postgrex.Error[reason: msg]) do
    msg
  end
end
