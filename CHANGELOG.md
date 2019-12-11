# Changelog

## v0.15.3 (2019-12-11)

* Enhancements
  * Allow dynamic connection configuration with the `:configure` for notifications
  * Add `:auto_reconnect` option for notifications
  * Accept `listen` commands even if the notifications connection is down or yet to first connect

* Bug fixes
  * Encode empty arrays in a mechanism compatible with CockroachDB
  * Cleanly terminate connection started with a socket

## v0.15.2 (2019-10-08)

* Enhancements
  * Improve performance of the bootstrap query

## v0.15.1 (2019-09-16)

* Enhancements
  * Add support for microseconds in Postgrex.Interval
  * Reduce bootstrap log message to debug and clarify error message

## v0.15.0 (2019-07-18)

Postgrex v0.15+ requires Elixir v1.6+.

* Enhancements
  * Filter bootstrap more efficiently by avoiding loading tables information on startup
  * Only bootstrap new oids during describe: this means reconnects don't run a bootstrap
    query and describe runs minimal query
  * Parse Postgrex 12beta new version format
  * Raise error when :ssl is required and not started in child_spec/1

* Bug fixes
  * Don't encode DateTime, NaiveDateTime or Time unless Calendar.ISO

## v0.14.3 (2019-05-08)

* Enhancements
  * Make bootstrap query compatible with CockroachDB 19.1
  * Improve error message when encoding bad tuple

## v0.14.2 (2019-04-12)

* Bug fixes
  * Fix Elixir deprecation warnings
  * Do not crash when receiving notices during authentication
  * Do not crash when receiving an error (caused by a raise) during query execution

## v0.14.1 (2018-11-24)

* Bug fixes
  * Bump decimal dependency to avoid runtime warnings

## v0.14.0 (2018-10-29)

* Enhancements
  * Postgrex.INET will add a /32 netmask to an IPv4 address and a /128 netmask to an IPv6 address during encoding where `netmask: nil`. When decoding, a /32 netmask (for IPv4) or /128 netmask (for IPv6) will be removed, resulting in `netmask: nil` for the struct
  * Add `:disconnect_on_error_codes` which allows Postgrex to automatically disconnect and then reconnect on certain errors. This is useful when using Postgrex against systems that support failover, which would emit certain errors on failover. This change allow those errors to be recovered from transparently
  * Add `:cache_statement` to `Postgrex.query/4` as a built-in statement cache
  * Support scram-sha-256 authentication from PostgreSQL 10
  * Add `Postgrex.prepare_execute/4`
  * Automatically reprepare queries that faild to encode due to a database type change

* Backwards incompatible changes
  * Invoke `encode_to_iodata!` instead of `encode!` in JSON encoder
  * Remove Postgrex.CIDR and use Postgrex.INET to encode both inet/cidr (as PostgreSQL may perform implicit/explicit casting at any time)
  * Postgrex.Time, Postgrex.Date and Postgrex.Timestamp were deprecated and now have been effectively removed
  * `Postgrex.execute/4` now always returns the prepared query
  * `:pool_timeout` is removed in favor of `:queue_target` and `:queue_interval`. See `DBConnection.start_link/2` for more information

## v0.13.4 (2018-01-25)

* Enhancements
  * Support custom range domains
  * Support custom array domains
  * Add support for UNIX domain sockets via the `:socket_dir` option
  * Remove warnings on Elixir v1.6

* Bug fixes
  * Fix encoding of empty ranges
  * Fix Postgrex.Path open/closed byte parity

## v0.13.3 (2017-05-31)

* Enhancements
  * Reload types on unknown oid during prepare

* Bug fixes
  * Fix default timeout for connection process from 5000s to 15000s

## v0.13.2 (2017-03-05)

* Bug fixes
  * Do not build invalid dates at compilation time on Elixir master

## v0.13.1 (2017-02-20)

* Enhancements
  * Allow naming Postgrex.Notifications server
  * Provide tsvector and lexeme support using binary formats

* Bug fixes
  * Fix encoding of Decimal values that would be wrong in certain circumstances
  * Add `:crypto` to applications
  * Specify proper Elixir dependency
  * Restore compatibility with postgres versions prior to 8.4 and with redshift

## v0.13.0 (2016-12-17)

* Enchancements
  * Support built-in geometry types
  * Fallback to `PGDATABASE` system env for the database
  * Support `bit` and `varbit` types
  * Add postgres error code to error messages
  * Support unprepared when using a stream
  * `:connect_timeout` and `:handshake_timeout` to configure tcp connect and handshake timeouts
  * Improve numeric encode/decode

* Bug fixes
  * Quote channel on listen/unlisten
  * Check datetime structs available before defining calendar extension
  * Backoff all awaiting connections if a bootstrap fails to prevent timeout loop
  * Handle idle admin shutdown of postgres backend
  * Fix rebootstrap query to be O(Nlog(N)) instead of O(N^2)
  * Fix encoding of numerical values

* Backwards incompatible changes
  * `:copy_data` query option is no longer supported and data can only be copied to the database using a collectable
  * Query struct has removed encoders/decoders and changed param_info/result_info values
  * Extensions now use a new encoder/decoder API based on quoted expressions
  * The `:extensions`, `:decode_binary` and `:null` options in `start_link` are no longer supported in favor of defining custom types with `Postgrex.Types.define(module, extra_extensions, options)`. `Postgrex.Types.define/3` must be called on its own file, outside of any module and function, as it only needs to be defined once during compilation.

## v0.12.1 (2016-09-29)

* Enchancements
  * Support special "char" type

* Bug fixes
  * Limit re-bootstrap to one connection at a time
  * Fix re-bootstrap of new composite types that use old types

## v0.12.0 (2016-09-06)

* Enhancements
  * Raise DBConnection.ConnectionError on connection error
  * Use send encoding to determine citext encoding
  * Use Map in favor of deprecated modules (to avoid warnings on v1.4)
  * Run rebootstrap test synchronously on every connect
  * Add support for Elixir 1.3+ Calendar types

## v0.11.2 (2016-06-16)

* Enhancements
  * Add support for COPY TO STDOUT and COPY FROM STDIN
  * Support packets bigger than 64MB
  * Introduce `mode: :savepoint` for prepare/execute/close that allows wrapping a request in a savepoint so that an error does not fail the transaction
  * Introduce streaming queries
  * Add `:decode_binary` option which is either `:copy` (default) or `:reference`.

* Bug fixes
  * Consistently convert the port number to integer
  * Remove type server entry on disconnect

## v0.11.1 (2016-02-15)

* Enhancements
  * Support PgBouncer transaction/statement pooling
  * Include more information in error messages
  * Add support for built-in postgres point type
  * Add `Postgrex.child_spec/1`
  * Allow custom encoding/decoding of postgres' NULL on a per query basis

* Bug fixes
  * Correctly pad decimal digits during encoding

## v0.11.0 (2016-01-21)

* Enhancements
  * Rely on DBConnection. This means better performance by copying less data between processes, faster encoding/decoding, support for transactions, `after_connect` hooks, connection backoff, logging, prepared queries, the ability to use both Poolboy and Sojourn as pools out of the box, and more

* Backwards incompatible change
  * Connection API from `Postgrex.Connection` has been moved to `Postgrex`
  * Notifications API from `Postgrex.Connection` has been moved to `Postgrex.Notifications`

## v0.10.0 (2015-11-17)

* Enhancements
  * Improve error message on encoding/decoding failures
  * Add network types such as: `inet`, `cidr` and `macaddr`
  * Improve TCP error messages
  * Support `PGPORT` environment variable
  * Improve decoding performance by caching extension information
  * Improve query performance by decoding in the client process hence not blocking the connection
  * Raise if number of parameters to query is wrong

* Bug fixes
  * Correctly handle errors in connection initialization with `sync_connect: true`
  * Do not fail on custom error codes
  * Correctly handle large number of parameters, also fixes some protocol issues where unsigned integers were treated as signed

## v0.9.1 (2015-07-14)

* Enhancements
  * Revert client side decoding as affects performance negatively (around 15% slower)
  * Cast floats and integers to decimal if a decimal is requested

## v0.9.0 (2015-07-12)

* Enhancements
  * Cached type bootstrapping for less memory usage and faster connection set up
  * The result set is now decoded in the calling process to reduce time spent in the connection process
  * Add a `decode: :manual` option to `Postgrex.query/4` and the function `Postgrex.decode/2` for manually decoding the result
  * Add `:sync_connect` option to `Postgrex.start_link/1`

* Bug fixes
  * Correctly handle extension types created inside schemas

* Backwards incompatible changes
  * Each row in `Postgrex.Result.rows` is now a list of columns instead of a tuple

## v0.8.4 (2015-06-24)

* Bug fixes
  * Fix version detection

## v0.8.3 (2015-06-22)

* Enhancements
  * Add `Postgrex.Extensions.JSON` extension for `json` and `jsonb` types
  * Set suitable TCP buffer size automatically

## v0.8.2 (2015-06-01)

* Enhancements
  * Add `:socket_options` option to `Postgrex.start_link/1`
  * Improved performance regarding binary handling
  * Add hstore support

* Backwards incompatible changes
  * Remove `:async_connect` option and make it the default

## v0.8.1 (2015-04-09)

* Enhancements
  * Keep the postgres error code in `:pg_code`
  * Support oid types and all its aliases (regclass etc)

* Backwards incompatible changes
  * Rename `:msec` field to `:usec` on `Postgrex.Time` and `Postgrex.Timestamp`

* Bug fixes
  * Fix numeric encoding for fractional numbers with less digits than the numeric base
  * Support encoding `timetz` type
  * Fix time and timestamp off-by-one bounds

## v0.8.0 (2015-02-26)

* Enhancements
  * Add extensions
  * Encode/decode ranges generically
  * Add bounds when encoding integer types to error instead of overflowing the integer
  * Log unhandled PostgreSQL errors (when it cant be replied to anyone)
  * Add support for enum types
  * Add support for citext type
  * Add microseconds to times and timestamps
  * Add the ability to rebootstrap types for an open connection

* Backwards incompatible changes
  * Remove the support for type-hinted queries
  * Remove encoder, decoder and formatter functions, use extensions instead
  * Use structs for dates, times, timestamps, interval and ranges
  * Change the default timeout for all operations to 5000ms
  * Show PostgreSQL error codes as their names instead

## v0.7.0 (2015-01-20)

* Enhancements
  * Add asynchronous notifications through `listen` and `unlisten`
  * Add support for range types
  * Add support for uuid type
  * Add `:async_connect` option to `start_link/1`

* Bug fixes
  * Fix encoding `nil` values in arrays and composite types

## v0.6.0 (2014-09-07)

* Enhancements
  * Queries can be constructed of `iodata`
  * Support "type hinted" queries to save one client-server round trip which will reduce query latency

* Backwards incompatible changes
  * `Postgrex.Error` `postgres` field is converted from keyword list to map
  * `Postgrex.Connect.query` `params` parameter is no longer optional (pass an empty list if query has no parameters)
  * The `timeout` parameter for all functions have been moved to a keyword list with the key `:timeout`

## v0.5.5 (2014-08-20)

* Enhancements
  * Reduce the amount of intermediary binaries constructed with the help of `iodata`

## v0.5.4 (2014-08-04)

## v0.5.3 (2014-07-13)

## v0.5.2 (2014-06-18)

## v0.5.1 (2014-05-24)

* Backwards incompatible changes
  * `Postgrex.Error` exception converted to struct

## v0.5.0 (2014-05-01)

* Backwards incompatible changes
  * `Postgrex.Result` and `Postgrex.TypeInfo` converted to structs

## v0.4.2 (2014-04-21)

* Enhancements
  * Add timeouts to all synchronous calls. When a timeout is hit an exit error will be raised in the caller process and the connection process will exit
  * Add automatic fallback to environment variables `PGUSER`, `PGHOST` and `PGPASSWORD`

## v0.4.0 (2014-01-16)

* Enhancements
  * Numerics decode and encode to Decimal

## v0.3.1 (2014-01-15)

* Enhancements
  * Compact state before printing to logs and hide password
  * Concurrency support, safe to use connection from multiple processes concurrently

## v0.3.0 (2013-12-16)

* Bug fixes
  * Don't try to decode values of text format

* Backwards incompatible changes
  * Types are stored as binaries instead of atoms, update your custom encoders and decoders

## v0.2.1 (2013-12-10)

* Enhancements
  * Add support for SSL

* Bug fixes
  * Fix decoding of unknown type when using custom decoder

## v0.2.0 (2013-11-14)

* Enhancements
  * Floats handles NaN, inf and -inf
  * Add support for numerics
  * Custom encoders and decoders works on elements in arrays
  * Add support for composite types
  * Add functions that raise on error

* Bug fixes
  * INSERT query works with extended query parameters
  * Return proper `num_rows` on PostgreSQL 8.4
  * Fix race condition

* Backwards incompatible changes
  * Simplify custom decoding and encoding with default function

## v0.1.0 (2013-10-14)

First release!
