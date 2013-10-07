defmodule Postgrex do
  alias Postgrex.Connection

  defmacrop is_port_number(port) do
    quote do
      is_integer(unquote(port)) and unquote(port) in 0..0xffff
    end
  end

  defmacrop is_address(addr) do
    quote do
      is_binary(unquote(addr)) or elem(unquote(addr), 3)
    end
  end

  def connect(address, port // 5432, username, password, database, parameters) when
      is_address(address) and is_port_number(port) and is_binary(username) and
      is_binary(password) and is_binary(database) do
    opts = [ address: address, port: port, username: username,
             password: password, database: database ]
    Connection.start_link(opts, parameters)
  end

  def disconnect(pid) do
    Connection.stop(pid)
  end

  def query(pid, statement, params // []) do
    Connection.query(pid, statement, params)
  end

  def parameters(pid) do
    Connection.parameters(pid)
  end

  def in_transaction(pid, fun) do
    case Postgrex.begin(pid) do
      :ok ->
        try do
          value = fun.()
          case Postgrex.commit(pid) do
            :ok -> { :ok, value }
            err -> err
          end
        catch
          :throw, :postgrex_rollback ->
            Postgrex.rollback(pid)
            { :ok, :rollback }
          type, term ->
            Postgrex.rollback(pid)
            :erlang.raise(type, term, System.stacktrace)
        end
      err -> err
    end
  end

  def begin(pid) do
    Postgrex.Connection.begin(pid)
  end

  def commit(pid) do
    Postgrex.Connection.commit(pid)
  end

  def rollback(pid) do
    Postgrex.Connection.rollback(pid)
  end
end
