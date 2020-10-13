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


## Redis and threads {#redis-threads}

The connection between the redis client and server uses a _stream pair_.
Although SWI-Prolog stream I/O is thread-safe, having multiple threads
using this same connection will mixup writes and their replies.

At the moment, the following locking is in place.

  - Connections created using redis_connect/3 are _not_ locked.  This
    implies the connection handle may be used from a single thread only,
    or redis/3 requests must be protected using with_mutex/2.
  - Redis/3 request using a _server name_ established using redis_server/3
    are locked using a mutex with the same name as the server name.


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


## Redis as a message brokering system {#redis-brokering}

Starting with Redis 5, redis supports _streams_. A stream is a list of
messages. Streams can be used as a reliable alternative to the older
Redis PUB/SUB (Publish Subscribe) mechanism that has no memory, i.e., if
a node is down when a message arrives the message is missed. In
addition, they can be used to have each message processed by a
_consumer_ that belongs to a _consumer group_. Both facilities are
supported by [library(redis_streams)](#redisstreams)

Redis streams provide all the low-level primitives to realise message
brokering.  Putting it all together is non-trivial though.  Notably:

  - We must take care of messages that have been sent to some
    consumer but the consumer fails to process the message and (thus)
    ACK it is processed.  This is handled by xlisten_group/5 using
    several options.  Good defaults for these options are hard to
    give as it depends on the required processing time for a message,
    how common failures are and an acceptable delay time in case
    of a failure, what to do in case of a persistent failure, etc.

  - Streams are independent from consumer groups and acknowledged
    messages remain in the stream.  xstream_set/3 can be used to
    limit the length of the stream, discarding the oldest messages.
    However, it is hard to give a sensible default.  The required
    queue length depends on the the size of the messages, whether
    messages come in more or less randomly or in bursts (that cause
    the stream to grow for a while), available memory, how bad it is
    if some messages get lost, etc.

The directory `doc/packages/examples/redis` in the installation provides
an example using streams and consumer groups to realise one or more
clients connected to one or more compute nodes.


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
