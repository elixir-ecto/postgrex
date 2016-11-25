defmodule Postgrex.SuperExtension do
  @moduledoc false

  @type opts :: term

  @callback init(Map.t, term) :: opts

  @callback matching(opts) :: [type: String.t,
                                 send: String.t,
                                 receive: String.t,
                                 input: String.t,
                                 output: String.t]

  @callback format(opts) :: :super_binary

  @callback oids(Postgrex.TypeInfo.t, opts) :: [Postgrex.Types.oid]

  @callback encode(opts) :: Macro.expr

  @callback decode(opts) :: Macro.expr
end
