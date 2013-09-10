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

pg_hba = """
local   all             postgres                127.0.0.1/32    trust
host    all             postgres                127.0.0.1/32    trust
host    all             postgrex_md5_pw         127.0.0.1/32    md5
host    all             postgrex_cleartext_pw   127.0.0.1/32    password
"""

cmd = %s(psql -U postgres -c "SHOW hba_file")
{ 0, output } = run_cmd.(cmd)
lines = String.split(output, "\n")
path = Enum.at(lines, 2) |> String.strip

File.write!(path, pg_hba)
