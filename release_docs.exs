additional_files = ["README.md"]

System.cmd "git clone --branch gh-pages `git config --get remote.origin.url` docs"

Mix.Task.run "docs"
Enum.each(additional_files, &File.cp!(&1, Path.join("docs", &1)))

File.cd! "docs", fn ->
  System.cmd "git add -A ."
  System.cmd "git commit -m \"Updated docs\""
  System.cmd "git push origin gh-pages"
end

File.rm_rf! "docs"

IO.puts IO.ANSI.escape("%{green}Updated docs pushed to origin/gh-pages")
