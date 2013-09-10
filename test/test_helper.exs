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

cmd = %s(psql -U postgres -c "#{sql}")
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
