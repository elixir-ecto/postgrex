defmodule Postgrex do
  alias Postgrex.Connection

  def connect(host, port // 5432, username, password, database) do
    opts = [ host: host, port: port, username: username,
             password: password, database: database ]
    { :ok, pid } = Connection.start_link()
    case Connection.connect(pid, opts) do
      :ok -> { :ok, pid }
      err -> err
    end
  end
end
