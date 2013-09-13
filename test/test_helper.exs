ExUnit.start

run_cmd = fn cmd ->
  key = :ecto_setup_cmd_output
  Process.put(key, "")
  status = Mix.Shell.cmd(cmd, fn(data) ->
    current = Process.get(key)
    Process.put(key, current <> data)
  end)
  output = Process.get(key)
  Process.put(key, "")
  { status, output }
end

sql = """
DROP ROLE IF EXISTS postgrex_cleartext_pw;
DROP ROLE IF EXISTS postgrex_md5_pw;

CREATE USER postgrex_cleartext_pw WITH PASSWORD 'postgrex_cleartext_pw';
CREATE USER postgrex_md5_pw WITH PASSWORD 'postgrex_md5_pw';
"""

cmds = [
  %s(psql -U postgres -c "#{sql}"),
  %s(psql -U postgres -c "DROP DATABASE IF EXISTS postgrex_test;"),
  %s(psql -U postgres -c "CREATE DATABASE postgrex_test ENCODING='UTF8' LC_COLLATE='en_US.UTF-8' LC_CTYPE='en_US.UTF-8';")
]

Enum.each(cmds, fn cmd ->
  { status, output } = run_cmd.(cmd)

  if status != 0 do
    IO.puts """
    Test setup command error'd with:

    #{output}

    Please verify the user "postgres" exists and it has permissions to
    create databases and users. If not, you can create a new user with:

    $ createuser postgres --no-password -d
    """
    System.halt(1)
  end
end)
