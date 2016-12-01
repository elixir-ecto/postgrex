defmodule Postgrex.SuperExtension do
  @moduledoc false

  @type opts :: term

  @callback init(term) :: opts

  @callback matching(opts) :: [type: String.t,
                                 send: String.t,
                                 receive: String.t,
                                 input: String.t,
                                 output: String.t]

  @callback format(opts) :: :super_binary

  @callback oids(Postgrex.TypeInfo.t, opts) :: nil | [Postgrex.Types.oid]

  @callback encode(opts) :: Macro.expr

  @callback decode(opts) :: Macro.expr
end
