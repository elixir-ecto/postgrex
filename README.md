# Postgrex

PostgreSQL driver for Elixir.

[![Build Status](https://travis-ci.org/ericmj/postgrex.png?branch=master)](https://travis-ci.org/ericmj/postgrex)

## Usage

Add Postgrex as a dependency in your `mix.exs` file.

```elixir
def deps do
  [ { :postgrex, github: "ericmj/postgrex" } ]
end
```

After you are done, run `mix deps.get` in your shell to fetch and compile Postgrex. Start an interactive Elixir shell with `iex -S mix`.

```elixir
iex(1)> { :ok, pid } = Postgrex.Connection.start_link([hostname: "localhost", username: "postgres", password: "postgres", database: "postgres"])
{:ok, #PID<0.69.0>}
iex(2)> Postgrex.Connection.query(pid, "SELECT user_id, text FROM comments")
{:ok,
 Postgrex.Result[command: :select, empty?: false, columns: ["user_id", "text"],
  rows: [{3,"hey"},{4,"there"}], size: 2]}
iex(3)> Postgrex.Connection.query(pid, "INSERT INTO comments (user_id, text) VALUES (10, 'heya')")
{:ok,
 Postgrex.Result[command: :insert, columns: nil, rows: nil, num_rows: 1]}

```

## Features

  * Automatic decoding and encoding of Elixir values to and from PostgreSQL's binary format
  * User specified custom encoders and decoders
  * Nested transactions
  * Supports PostgreSQL 8.4, 9.0, 9.1, 9.2 and 9.3

## Data representation

    PostgreSQL     Elixir
    ----------     ------
    NULL           nil
    bool           true | false
    char           "é"
    int            42
    float          42.0
    text           "eric"
    bytea          << 42 >>
    date           { 2013, 10, 12 }
    time           { 0, 37, 14 }
    timestamp(tz)  { { 2013, 10, 12 }, { 0, 37, 14 } }
    interval       { 14, 40, 10920 } *
    array          [ 1, 2, 3 ]

\* interval is encoded as `{ months, days, seconds }`

## TODO

  * Callbacks for asynchronous events
  * Encoding/decoding of composite types, numeric, money
  * Text format decoding of arrays of unknown types
