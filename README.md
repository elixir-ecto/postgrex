# Postgrex

[![Build Status](https://travis-ci.org/ericmj/postgrex.png?branch=master)](https://travis-ci.org/ericmj/postgrex)

PostgreSQL driver for Elixir.

Documentation: http://ericmj.github.io/postgrex

## Usage

Add Postgrex as a dependency in your `mix.exs` file.

```elixir
def deps do
  [ { :postgrex, "~> 0.5" } ]
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

    PostgreSQL      Elixir
    ----------      ------
    NULL            nil
    bool            true | false
    char            "é"
    int             42
    float           42.0
    text            "eric"
    bytea           << 42 >>
    numeric         #Decimal<42.0> *
    date            { 2013, 10, 12 }
    time            { 0, 37, 14 }
    timestamp(tz)   { { 2013, 10, 12 }, { 0, 37, 14 } }
    interval        { 14, 40, 10920 } **
    array           [ 1, 2, 3 ]
    composite type  { 42, "title", "content" }

\* [Decimal](http://github.com/ericmj/decimal)

\*\* interval is encoded as `{ months, days, seconds }`.

## Custom encoder and decoder example

Encoding and decoding from and to JSON:

```elixir
def decoder(TypeInfo[sender: "json"], _format, _default, binary) do
  JSON.decode(binary)
end

def decoder(TypeInfo[], _format, default, binary) do
  default.(binary)
end

def encoder(TypeInfo[sender: "json"], _default, value) do
  JSON.encode(value)
end

def encoder(TypeInfo[], default, value) do
  default.(value)
end

def formatter(TypeInfo[sender: "json"]) do
  :text
end

def formatter(TypeInfo[]) do
  nil
end
```

## TODO

  * Callbacks for asynchronous events

## Contributing

To contribute you need to compile Postgrex from source and test it:

```
$ git clone https://github.com/ericmj/postgrex.git
$ cd postgrex
$ mix test
```

The tests requires some modifications to your [hba file](http://www.postgresql.org/docs/9.3/static/auth-pg-hba-conf.html). The path to it can be found by running `$ psql -U postgres -c "SHOW hba_file"` in your shell. Put the following above all other configurations (so that they override):

```
host    all             postgrex_md5_pw         127.0.0.1/32    md5
host    all             postgrex_cleartext_pw   127.0.0.1/32    password
```

The server needs to be restarted for the changes to take effect. Additionally you need to setup a Postgres user with the same username as the local user and give it trust or ident in your hba file. Or you can export $PGUSER and $PGPASS before running tests.

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
