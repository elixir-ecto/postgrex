# Run with mix run release_docs.exs

additional_files = [
  "README.html"
]

System.cmd "git stash"

Mix.Task.run "docs"
files = Path.wildcard("docs/**") |> Enum.map(&Path.relative_to(&1, "docs"))
files = files ++ additional_files

IO.puts System.cmd "git checkout gh-pages"

System.cmd "git reset"

old_files = System.cmd("git ls-files") |> String.split("\n")
old_files = old_files -- additional_files
IO.puts System.cmd "git rm " <> Enum.join(old_files, " ")

File.cp_r "docs/.", "./"
File.rm_rf! "docs"

IO.puts System.cmd "git add " <> Enum.join(files, " ")
IO.puts System.cmd "git commit -m \"Update docs\""
IO.puts System.cmd "git push"

Enum.each(files, &File.rm_rf!/1)
IO.puts System.cmd "git checkout master --force"
System.cmd "git stash pop"
