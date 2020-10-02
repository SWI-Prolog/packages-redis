# SWI-Prolog redis client

This package provides `library(redis)`, a client for the
[redis](https://redis.io/) _"open source (BSD licensed), in-memory data
structure store, used as a database, cache and message broker"_.

This is a first version based on the [GNU-Prolog pure Prolog redis
client](https://github.com/emacstheviking/gnuprolog-redisclient) by Sean
Charles.  The main difference to the original client are:

  - Replies are not wrapped by type in a compound term.
  - String replies use the SWI-Prolog string type.
  - Values can be specified as prolog(Value), after which they
    are returns as a (copy of) Value.  This prefixes the value
    using "\u0000T\u0000".
  - Strings are in UTF-8 encoding and support full Unicode.
  - Using redis_server/3, actual connections are established
    lazily and when a connection is lost it is automatically
    restarted.
  - This library allows for using the Redis publish/subscribe
    interface.  Messages are propagated using broadcast/1.

## Example

This example communicates with the default redis server at
`localhost:6379`. Use redis_server/3 to change the address of the
default server or setup a secondary named server.

```
?- redis(set(a, 1)).
?- redis(get(a), X).
X = "1".
?- redis(set(a, prolog(1))).
?- redis(get(a), X).
X = 1.
?- redis(set(a, prolog(hello(world)))).
?- redis(get(a), X).
X = hello(world).
```

The command mapping is the same as for the original GNU Prolog client.
A redis command

    COMMAND Arg1, Arg2, ...

maps to the Prolog term

    command(Arg1, Arg2, ...)

On return, the following mapping applies:

   | Redis  | Prolog         |
   |--------|----------------|
   | String | String         |
   | Int    | Int            |
   | List   | List           |
   | nil    | nil (fail if reply is only `nil`) |
   | Status | status(String) |
   | Error  | throws `error(resis_error(String),_)` |


## Status

This library is still _work in progress_. We think the basic interface,
using redis_server/3 to register services by name and the predicates
redis/1-3 are fine. The _subscribe_ API might change. Other areas that
need attention:

  - Although using SWI-Prolog's string reading and writing rather
    then character I/O makes the client a lot faster, there is
    significant room for a performance improvement using a C
    plugin.

  - Currently the library is thread-safe using a lock around redis/3.
    Future versions may use a connection pool that allows multiple
    threads to make progress.
