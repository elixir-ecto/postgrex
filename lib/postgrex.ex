defmodule Postgrex do
  alias Postgrex.Connection

  defmacrop is_port_number(port) do
    quote do
      is_integer(unquote(port)) and unquote(port) in 0..0xffff
    end
  end

  defmacrop is_hostname(host) do
    quote do
      is_binary(unquote(host)) or elem(unquote(host), 3)
    end
  end

  def connect(hostname, port // 5432, username, password, database, parameters) when
      is_hostname(hostname) and is_port_number(port) and is_binary(username) and
      is_binary(password) and is_binary(database) do
    opts = [ hostname: hostname, port: port, username: username,
             password: password, database: database ]
    Connection.start_link(opts, parameters)
  end

  def disconnect(pid) do
    Connection.stop(pid)
  end

  def query(pid, statement, params // []) do
    case Connection.query(pid, statement, params) do
      Postgrex.Result[] = res -> { :ok, res }
      Postgrex.Error[] = err -> { :error, err }
    end
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
            :ok -> value
            err -> raise err
          end
        catch
          :throw, :postgrex_rollback ->
            case Postgrex.rollback(pid) do
              :ok ->
              err -> raise err
          type, term ->
            Postgrex.rollback(pid)
            :erlang.raise(type, term, System.stacktrace)
        end
      err -> raise err
    end
  end

  def begin(pid) do
    case Postgrex.Connection.begin(pid) do
      Postgrex.Result[] -> :ok
      err -> err
    end
  end

  def commit(pid) do
    case Postgrex.Connection.commit(pid) do
      Postgrex.Result[] -> :ok
      err -> err
    end
  end

  def rollback(pid) do
    case Postgrex.Connection.rollback(pid) do
      Postgrex.Result[] -> :ok
      err -> err
    end
  end
end
