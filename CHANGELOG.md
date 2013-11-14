# v0.1.1-dev

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
