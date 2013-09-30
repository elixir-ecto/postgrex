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
    { :ok, pid } = Connection.start_link()
    case Connection.connect(pid, opts, parameters) do
      :ok -> { :ok, pid }
      err -> err
    end
  end

  def disconnect(pid) do
    Connection.stop(pid)
  end

  def query(pid, statement) do
    Connection.query(pid, statement)
  end

  def parameters(pid) do
    Connection.parameters(pid)
  end
end
