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

```iex
iex> { :ok, pid } = Postgrex.Connection.start_link([hostname: "localhost", username: "postgres", password: "postgres", database: "postgres"])
{:ok, #PID<0.69.0>}
iex> Postgrex.Connection.query(pid, "SELECT user_id, text FROM comments")
{:ok,
 Postgrex.Result[command: :select, empty?: false, columns: ["user_id", "text"],
  rows: [{3,"hey"},{4,"there"}], size: 2]}
iex> Postgrex.Connection.query(pid, "INSERT INTO comments (user_id, text) VALUES (10, 'heya')")
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
    numeric        42 | 42.5 *
    date           { 2013, 10, 12 }
    time           { 0, 37, 14 }
    timestamp(tz)  { { 2013, 10, 12 }, { 0, 37, 14 } }
    interval       { 14, 40, 10920 } **
    array          [ 1, 2, 3 ]

\* numeric is only decoded as float when it is a non-integer value, this is to not lose precision when it is an integer value (elixir's integers are of arbitrary precision). NOTE: floating point encoding and decoding is lossy, use with caution!
\*\* interval is encoded as `{ months, days, seconds }`.

## Custom encoder and decoder example

Encoding and decoding from and to JSON:

```elixir
def decoder(:json, _sender, _oid, _default, binary) do
  JSON.decode(binary)
end

def encoder(:json, _sender, _oid, _default, value) do
  { :text, JSON.encode(param) }
end
```

## TODO

  * Callbacks for asynchronous events
  * Encoding/decoding of composite types, money
  * Text format decoding of arrays of unknown types
  * Lossless numeric encoding/decoding with future arbitrary precision decimal type

## License

   Copyright 2013 Eric Meadows-Jönsson

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
