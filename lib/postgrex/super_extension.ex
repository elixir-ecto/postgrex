defmodule Postgrex.SuperExtension do
  @moduledoc false

  @type state :: term

  @callback init(Keyword.t) :: state

  @callback matching(state) :: [type: String.t,
                                 send: String.t,
                                 receive: String.t,
                                 input: String.t,
                                 output: String.t]

  @callback format(state) :: :super_binary

  @callback oids(Postgrex.TypeInfo.t, state) :: nil | [Postgrex.Types.oid]

  @callback encode(state) :: Macro.t

  @callback decode(state) :: Macro.t
end
