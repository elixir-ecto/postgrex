additional_files = ["README.md"]

:os.cmd 'git clone --branch gh-pages `git config --get remote.origin.url` docs'

Mix.Task.run "docs"
Enum.each(additional_files, &File.cp!(&1, Path.join("docs", &1)))

File.cd! "docs", fn ->
  :os.cmd 'git add -A .'
  :os.cmd 'git commit -m "Updated docs"'
  :os.cmd 'git push origin gh-pages'
end

File.rm_rf! "docs"

IO.puts IO.ANSI.escape("%{green}Updated docs pushed to origin/gh-pages")
