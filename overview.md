# About the SWI-Prolog Redis client {#redis-overview}

[Redis](https://redis.io) is an in-memory key-value  store. Redis can be
operated as a simple store for   managing  (notably) volatile persistent
data. Redis can operate in serveral modes,  ranging from a single server
to  clusters  organised  in  several  different  ways  to  provide  high
availability, resilience and replicating the data  close to the involved
servers.  In  addition  to  being  a   key-value  store,  Redis  enables
additional communication between clients such  as _publish/subscribe_ to
message channels, _streams_, etc.

These features can be used to connect _micro services_, both for sharing
state, notifications and distributing tasks.

## About versions {#redis-versions}

The current stable version of Redis is  6. Many Linux distros still ship
with version 5. Both talk protocol version   2.  Version 6 also supports
protocol version 3.  The main differences are:

  - The version 3 protocol has several improvements that notably
    improvement passing large objects using a _streaming_ protocol.
  - Hashes (maps) in the version 3 protocol are exchanged as lists
    of _pairs_ (`Name-Value`), while version 2 exchanges hashes as
    a list of alternating names and values.  This is visible to the
    user.  High level predicates such as redis_get_hash/3 deal with
    both representations.
  - The version 3 protocol supports _push messages_ to deal with
    _monitor_ and _subscribe_ events on the same connection as used
    for handling normal requests.

New projects are encouraged to use Redis version 6 with the version 3
protocol.  See redis_server/3.


## History  {#redis-history}

This module is based on the `gpredis.pl` by Sean Charles for GNU-Prolog.
This file greatly helped me understanding what had to be done, although,
eventually, not much of the original interface is left. The main
difference to the original client are:

  - Replies are not wrapped by type in a compound term.
  - String replies use the SWI-Prolog string type.
  - Values can be specified as prolog(Value), after which they
    are returns as a (copy of) Value.  This prefixes the value
    using "\u0000T\u0000".
  - Strings are in UTF-8 encoding to support full Unicode.
  - Using redis_server/3, actual connections are established
    lazily and when a connection is lost it is automatically
    restarted.
  - This library allows for using the Redis publish/subscribe
    interface.  Messages are propagated using broadcast/1.
