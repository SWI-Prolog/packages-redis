/*
    FILE: gpredis.pl
    DATE: Nov 2013
    DOES: Provides a simple Redis client for GNU Prolog using only Prolog
    WHOM: Sean Charles  <sean at objitsu dot com>

    This program provides a simple Redis client. It does not allow for any
    persistent connection behaviours so pub/sub etc is not possible (yet).

    All other commands are supported using a simple form whereby a functor
    name makes the first part of the command and the arguments are then added
    as the remaining Redis command.

    Please read the test scripts for full examples on how to use.

    BUGS/IDEAS: Please submit to the email address shown above.

    LICENCE: MIT, see the LICENCE file.
*/

:- module(redis,
          [ redis_server/3,             % +Alias,+Address,+Options
            redis_connect/1,            % -Connection
            redis_connect/3,            % -Connection, +Host, +Port
            redis_disconnect/1,         % +Connection
                                        % Queries
            redis/1,                    % +Request
            redis/2,                    % +Connection, +Request
            redis/3,                    % +Connection, +Request, -Reply
            redis_cli/1,                % +Request
                                        % High level queries
            redis_get_list/3,           % +Redis,+Key,-List
            redis_get_list/4,           % +Redis,+Key,+ChunkSize,-List
            redis_set_list/3,           % +Redis,+Key,+List
            redis_get_hash/3,           % +Redis,+Key,-Data:dict
            redis_set_hash/3,           % +Redis,+Key,+Data:dict
            redis_scan/3,               % +Redis,-LazyList,+Options
            redis_sscan/4,              % +Redis,+Set,-LazyList,+Options
            redis_hscan/4,              % +Redis,+Hash,-LazyList,+Options
            redis_zscan/4,              % +Redis,+Set,-LazyList,+Options
                                        % Publish/Subscribe
            redis_subscribe/2,          % +Redis, +Channels
            redis_unsubscribe/2,        % +Redis, +Channels
            redis_write/2,              % +Redis,+Command
            redis_read/2                % +Redis,-Reply
          ]).
:- autoload(library(socket), [tcp_connect/3]).
:- autoload(library(apply), [maplist/2]).
:- autoload(library(broadcast), [broadcast/1]).
:- autoload(library(error),
            [ must_be/2,
              instantiation_error/1,
              existence_error/2,
              permission_error/3
            ]).
:- autoload(library(lazy_lists), [lazy_list/2]).
:- autoload(library(lists), [append/3]).
:- autoload(library(option), [merge_options/3, option/2, option/3]).
:- autoload(library(http/http_stream), [stream_range_open/3]).
:- use_module(library(debug), [debug/3]).

/** <module> Redis client

This library is a client  to   [Redis](https://redis.io),  a popular key
value store to  deal  with  caching   and  communication  between  micro
services. This module is based on the   `gpredis.pl` by Sean Charles for
GNU-Prolog. This file greatly helped  me   understanding  what had to be
done, although, eventually, not much of the original interface is left.

In the typical use case we register  the   details  of one or more Redis
servers using redis_server/3. Subsequenly, redis/1-3   is  used to issue
commands on the server.  For example:

```
?- redis_server(default, 'redis':6379, [password("secret")]).
?- redis(set(user, "Bob")).
?- redis(get(user), User).
User = "Bob"
```

The main difference to the original client are:

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
*/

:- dynamic server/3.

:- dynamic ( connection/2,              % Alias, Stream
             subscription/2,            % Stream, Channel
             listening/2                % Stream, Thread
           ) as volatile.

%!  redis_server(+Alias, +Address, +Options) is det.
%
%   Register a redis server without connecting to  it. The Alias acts as
%   a lazy connection alias. Initially  the   alias  `default` points at
%   `localhost:6379` with no connect options.   The  `default` server is
%   used  for  redis/1  and  redis/2  and  may  be  changed  using  this
%   predicate.
%
%   Connections established this way are   automatically  reconnected if
%   the connection is lost for some reason.

redis_server(Alias, Address, Options) :-
    must_be(ground, Alias),
    retractall(server(Alias, _, _)),
    asserta(server(Alias, Address, Options)).

server(default, localhost:6379, []).

%!  redis_connect(-Connection) is det.
%!  redis_connect(+Address, -Connection, +Options) is det.
%!  redis_connect(-Connection, +Host, +Port) is det.
%
%   Connect to a redis server. The  main mode is redis_connect(+Address,
%   -Connection,   +Options).   redis_connect/1   is     equivalent   to
%   redis_connect(localhost:6379, Connection, []).  Options:
%
%     - alias(Alias)
%       Make the connection globally available as Alias.
%     - open(OpenMode)
%       One of `once` (default if an alias is provided) or `multiple`.
%     - password(+Password)
%       Authenticate using Password
%
%   @compat   redis_connect(-Connection,   +Host,     +Port)    provides
%   compatibility to the original GNU-Prolog interface and is equivalent
%   to redis_connect(Host:Port, Connection, []).
%
%   @arg Address is a term Host:Port or  the name of a server registered
%   using redis_server/3. The latter realises a   new connection that is
%   typically used with redis_subscribe/2 or redis/1.

redis_connect(Conn) :-
    redis_connect(default, Conn, []).

redis_connect(Conn, Host, Port) :-
    var(Conn),
    ground(Host), ground(Port),
    !,                                  % GNU-Prolog compatibility
    redis_connect(Host:Port, Conn, []).
redis_connect(Server, Conn, Options) :-
    ground(Server),
    server(Server, Address, DefaultOptions),
    !,
    merge_options(Options, DefaultOptions, Options2),
    do_connect(Address, Conn, Options2).
redis_connect(Address, Conn, Options) :-
    option(alias(Alias), Options),
    !,
    (   option(open(once), Options, once),
        connection(Alias, S)
    ->  Conn = redis(S)
    ;   do_connect(Address, Conn, Options),
        Conn = redis(S),
        asserta(connection(Alias, S))
    ).
redis_connect(Address, Conn, Options) :-
    do_connect(Address, Conn, Options).

do_connect(Address, Conn, Options) :-
    tcp_connect(Address, Stream, Options),
    stream_pair(Stream, _In, Out),
    set_stream(Out, encoding(utf8)),
    Conn = redis(Stream),
    auth(Conn, Options).

auth(Con, Options) :-
    option(password(Password), Options),
    !,
    redis(Con, auth(Password)).
auth(_, _).

redis_stream(Var, _, _) :-
    var(Var),
    !,
    instantiation_error(Var).
redis_stream(Alias, S, Connect) :-
    atom(Alias),
    !,
    (   connection(Alias, S0)
    ->  S = S0
    ;   Connect == true,
        server(Alias, Address, Options)
    ->  redis_connect(Address, Connection, [alias(Alias)|Options]),
        redis_stream(Connection, S, false)
    ;   existence_error(redis, Alias)
    ).
redis_stream(redis(S), S, _).

has_redis_stream(Var, _) :-
    var(Var),
    !,
    instantiation_error(Var).
has_redis_stream(Alias, S) :-
    atom(Alias),
    !,
    connection(Alias, S).
has_redis_stream(redis(S), S).


%!  redis_disconnect(+Connection) is det.
%!  redis_disconnect(+Connection, +Options) is det.
%
%   Disconnect from a redis server.

redis_disconnect(Redis) :-
    redis_disconnect(Redis, []).

redis_disconnect(Redis, Options) :-
    option(force(true), Options),
    !,
    (   has_redis_stream(Redis, S)
    ->  close(S, [force(true)]),
        retractall(connection(_,S))
    ;   true
    ).
redis_disconnect(Redis, _Options) :-
    redis_stream(Redis, S, false),
    close(S),
    retractall(connection(_,S)).

disconnect_stream(S) :-
    close(S, [force(true)]),
    retractall(connection(_,S)).


%!  redis(+Request) is semidet.
%!  redis(+Request, -Reply) is semidet.
%!  redis(+Connection, +Request, -Reply) is semidet.
%
%   Execute a redis command on Connnection.   The first executes Request
%   on the `default` connection and ignores   the  result unless this is
%   `nil`, or an error. The redis/2 form   runs redis/3 on the `default`
%   connection. The full redis/3 executes Request   and binds the result
%   to Reply.  Reply is one of:
%
%     - status(String)
%     - A number
%     - A string
%     - A list of replies.  A list may also contain `nil`.  If Reply
%       as a whole would be `nil` the call fails.
%
%   @error redis_error(String)

redis(Req) :-
    redis(default, Req, _Out).

redis(Req, Out) :-
    redis(default, Req, Out).

redis(Redis, Req, Out) :-
    Error = error(socket_error(Which, _), _),
    catch(redis1(Redis, Req, Out), Error, true),
    (   var(Which)
    ->  true
    ;   recover(Error, Redis, Req, Out)
    ).

redis1(Redis, Req, Out) :-
    redis_stream(Redis, S, true),
    gpredis_build_cmd(Req, CmdOut),
    with_mutex(redis,
               ( gpredis_write(S, CmdOut),
                 gpredis_read(S, Out)
               )),
    Out \== nil.

recover(Error, Redis, Req, Out) :-
    (   ground(Redis),
        server(Redis, _, _)
    ->  debug(redis(recover), 'Got error ~p; trying to reconnect', [Error]),
        redis_disconnect(Redis, [force(true)]),
        wait(Redis),
        redis(Redis, Req, Out),
        retractall(failure(Redis, _))
    ;   throw(Error)
    ).

:- dynamic failure/2 as volatile.

wait(Redis) :-
    retract(failure(Redis, Times)),
    !,
    Times2 is Times+1,
    asserta(failure(Redis, Times2)),
    Wait is min(6000, 1<<Times)*0.01,
    debug(redis(recover), '  Sleeping ~p seconds', [Wait]),
    sleep(Wait).
wait(Redis) :-
    asserta(failure(Redis, 1)).


%!  redis_cli(+Request)
%
%   Connect to the default redis server,   call  redist/3 using Request,
%   disconnect and print the result.

redis_cli(Req) :-
    setup_call_cleanup(
        redis_connect(default, C, []),
        redis1(C, Req, Out),
        redis_disconnect(C)),
    print(Out).

%!  redis_write(+Redis, +Command) is det.
%!  redis_read(+Redis, -Reply) is det.
%
%   Write command and read replies from a Redis server. These are
%   building blocks for subscribing to event streams.

redis_write(Redis, Command) :-
    redis_stream(Redis, S, true),
    gpredis_build_cmd(Command, String),
    gpredis_write(S, String).

redis_read(Redis, Reply) :-
    redis_stream(Redis, S, true),
    gpredis_read(S, Reply).


		 /*******************************
		 *      HIGH LEVEL ACCESS	*
		 *******************************/

%!  redis_get_list(+Redis, +Key, -List) is det.
%!  redis_get_list(+Redis, +Key, +ChunkSize, -List) is det.
%
%   Get the content of a Redis list in   List. If ChunkSize is given and
%   smaller than the list length, List is returned as a _lazy list_. The
%   actual values are requested using   redis  ``LRANGE`` requests. Note
%   that this results in O(N^2) complexity. Using   a  lazy list is most
%   useful for relatively short lists holding possibly large items.
%
%   Note that values retrieved are _strings_, unless the value was added
%   using prolog(Term).
%
%   @see lazy_list/2 for a discussion  on   the  difference between lazy
%   lists and normal lists.

redis_get_list(Redis, Key, List) :-
    redis_get_list(Redis, Key, -1, List).

redis_get_list(Redis, Key, Chunk, List) :-
    redis(Redis, llen(Key), Len),
    (   (   Chunk >= Len
        ;   Chunk == -1
        )
    ->  (   Len == 0
        ->  List = []
        ;   End is Len-1,
            list_range(Redis, Key, 0, End, List)
        )
    ;   lazy_list(rlist_next(s(Redis,Key,0,Chunk,Len)), List)
    ).

rlist_next(State, List, Tail) :-
    State = s(Redis,Key,Offset,Slice,Len),
    End is min(Len-1, Offset+Slice-1),
    list_range(Redis, Key, Offset, End, Elems),
    (   End =:= Len-1
    ->  List = Elems,
        Tail = []
    ;   Offset2 is Offset+Slice,
        nb_setarg(3, State, Offset2),
        append(Elems, Tail, List)
    ).

% Redis LRANGE demands End > Start and returns inclusive.

list_range(DB, Key, Start, Start, [Elem]) :-
    !,
    redis(DB, lindex(Key, Start), Elem).
list_range(DB, Key, Start, End, List) :-
    !,
    redis(DB, lrange(Key, Start, End), List).



%!  redis_set_list(+Redis, +Key, +List) is det.
%
%   Associate a Redis key with a list.  As   Redis  has no concept of an
%   empty list, if List is `[]`, Key  is _deleted_. Note that key values
%   are always strings in  Redis.  The   same  conversion  rules  as for
%   redis/1-3 apply.

redis_set_list(Redis, Key, List) :-
    redis(Redis, del(Key), _),
    (   List == []
    ->  true
    ;   Term =.. [rpush,Key|List],
        redis(Redis, Term, _Count)
    ).


%!  redis_get_hash(+Redis, +Key, -Data:dict) is det.
%!  redis_set_hash(+Redis, +Key, +Data:dict) is det.
%
%   Put/get a Redis hash as a Prolog  dict. Putting a dict first deletes
%   Key. Note that in many cases   applications will manage Redis hashes
%   by key. redis_get_hash/3 is notably a   user friendly alternative to
%   the Redis ``HGETALL`` command. If the  Redis   hash  is  not used by
%   other (non-Prolog) applications one  may   also  consider  using the
%   prolog(Term) syntax to store the Prolog dict as-is.

redis_get_hash(Redis, Key, Dict) :-
    redis(Redis, hgetall(Key), TwoList),
    pairs_2list(Pairs, TwoList),
    dict_pairs(Dict, _, Pairs).

redis_set_hash(Redis, Key, Dict) :-
    dict_pairs(Dict, _Tag, Pairs),
    pairs_2list(Pairs, TwoList),
    Term =.. [hset,Key|TwoList],
    redis(Redis, del(Key), _),
    redis(Redis, Term, _Count).

pairs_2list([], []) :-
    !.
pairs_2list([Name-Value|T0], [NameS,Value|T]) :-
    atom_string(Name, NameS),
    pairs_2list(T0, T).

%!  redis_scan(+Redis, -LazyList, +Options) is det.
%!  redis_sscan(+Redis, -LazyList, +Options) is det.
%!  redis_hscan(+Redis, -LazyList, +Options) is det.
%!  redis_zscan(+Redis, -LazyList, +Options) is det.
%
%   Map the Redis ``SCAN``, ``SSCAN``,   ``HSCAN`` and `ZSCAN`` commands
%   into a _lazy list_. For redis_scan/3 and redis_sscan/4 the result is
%   a list of strings. For redis_hscan/4   and redis_zscan/4, the result
%   is a list of _pairs_.   Options processed:
%
%     - match(Pattern)
%       Adds the ``MATCH`` subcommand, only returning matches for
%       Pattern.
%     - count(Count)
%       Adds the ``COUNT`` subcommand, giving a hint to the size of the
%       chunks fetched.
%     - type(Type)
%       Adds the ``TYPE`` subcommand, only returning answers of the
%       indicated type.
%
%   @see lazy_list/2.

redis_scan(Redis, LazyList, Options) :-
    scan_options([match,count,type], Options, Parms),
    lazy_list(scan_next(s(scan,Redis,0,Parms)), LazyList).

redis_sscan(Redis, Set, LazyList, Options) :-
    scan_options([match,count,type], Options, Parms),
    lazy_list(scan_next(s(sscan(Set),Redis,0,Parms)), LazyList).

redis_hscan(Redis, Hash, LazyList, Options) :-
    scan_options([match,count,type], Options, Parms),
    lazy_list(scan_next(s(hscan(Hash),Redis,0,Parms)), LazyList).

redis_zscan(Redis, Set, LazyList, Options) :-
    scan_options([match,count,type], Options, Parms),
    lazy_list(scan_next(s(zscan(Set),Redis,0,Parms)), LazyList).

scan_options([], _, []).
scan_options([H|T0], Options, [H,V|T]) :-
    Term =.. [H,V],
    option(Term, Options),
    !,
    scan_options(T0, Options, T).
scan_options([_|T0], Options, T) :-
    scan_options(T0, Options, T).


scan_next(State, List, Tail) :-
    State = s(Command,Redis,Cursor,Params),
    Command =.. CList,
    append(CList, [Cursor|Params], CList2),
    Term =.. CList2,
    redis(Redis, Term, [NewCursor,Elems0]),
    scan_pairs(Command, Elems0, Elems),
    (   NewCursor == "0"
    ->  List = Elems,
        Tail = []
    ;   number_string(CursorI, NewCursor),
        nb_setarg(3, State, CursorI),
        append(Elems, Tail, List)
    ).

scan_pairs(hscan(_), List, Pairs) :-
    !,
    scan_pairs(List, Pairs).
scan_pairs(zscan(_), List, Pairs) :-
    !,
    scan_pairs(List, Pairs).
scan_pairs(_, List, List).

scan_pairs([], []).
scan_pairs([Key,Value|T0], [Key-Value|T]) :-
    scan_pairs(T0, T).


		 /*******************************
		 *            SUBSCRIBE		*
		 *******************************/

%!  redis_subscribe(+Redis, +Channels) is det.
%!  redis_unsubscribe(+Redis, +Channels) is det.
%
%   Subscribe to one or more Redis  PUB/PUB channels. Multiple subscribe
%   and unsubscribe messages may be issued on the same Redis connection.
%   The first thread that subscribes  on   a  channel blocks, forwarding
%   events using broadcast/1 using the following message. Here `Channel`
%   is an atom denoting the channal that   received a message and `Data`
%   is a string containing the message data.
%
%       redis(Redis, Channel, Data)
%
%   If redis_unsubscribe/2 removes the last   subscription, the blocking
%   redis_subscribe/2 completes.
%
%   redis_subscribe/2 is normally executed in a  thread. To simply print
%   the incomming messages use e.g.
%
%       ?- listen(redis(_, Channel, Data),
%                 format('Channel ~p got ~p~n', [Channel,Data])).
%       true.
%       ?- redis_connect(Redis),
%          thread_create(redis_subscribe(Redis, [test]), Id, []).
%       Redis = ..., Id = ...,
%       ?- redis(publish(test, "Hello world")).
%       Channel test got "Hello world"
%       1
%       true.


redis_subscribe(Redis, Channels) :-
    redis_stream(Redis, S, true),
    Req =.. [subscribe|Channels],
    gpredis_build_cmd(Req, CmdOut),
    gpredis_write(S, CmdOut),
    maplist(register_subscription(S), Channels),
    (   listening(S, _Thread)
    ->  true
    ;   redis_listen(Redis)
    ).

redis_unsubscribe(Redis, Channels) :-
    redis_stream(Redis, S, true),
    Req =.. [unsubscribe|Channels],
    gpredis_build_cmd(Req, CmdOut),
    gpredis_write(S, CmdOut),
    maplist(unregister_subscription(S), Channels).

register_subscription(S, Channel) :-
    (   subscription(S, Channel)
    ->  true
    ;   assertz(subscription(S, Channel))
    ).

unregister_subscription(S, Channel) :-
    retractall(subscription(S, Channel)).

redis_listen(Redis) :-
    redis_stream(Redis, S, true),
    listening(S, _Thread),
    !,
    permission_error(listen, redis, S).
redis_listen(Redis) :-
    redis_stream(Redis, S, true),
    thread_self(Me),
    setup_call_cleanup(
        assertz(listening(S, Me), Ref),
        redis_listen_loop(Redis),
        erase(Ref)).

redis_listen_loop(Redis) :-
    redis_stream(Redis, S, true),
    (   subscription(S, _)
    ->  gpredis_read(S, Reply),
        redis_broadcast(Redis, Reply),
        redis_listen_loop(Redis)
    ;   true
    ).

redis_broadcast(_, ["subscribe", _Channel, _N]) :-
    !.
redis_broadcast(Redis, ["message", ChannelS, Data]) :-
    !,
    atom_string(Channel, ChannelS),
    catch(broadcast(redis(Redis, Channel, Data)),
          Error,
          print_message(error, Error)).
redis_broadcast(Redis, Message) :-
    debug(redis(warning), '~p: Unknown message while listening: ~p',
          [Redis,Message]).


		 /*******************************
		 *          READ/WRITE		*
		 *******************************/

%!  gpredis_write(+SO, +String) is det.


gpredis_write(SO,String) :-
    write(SO, String),
    flush_output(SO).

%!  gpredis_read(+Stream, -Term) is det.
%
%   Read a message from a Redis stream.  Term is one of
%
%     - A list of terms
%     - The atom `nil`
%     - A number
%     - A term status(String)
%     - A string
%
%   If something goes wrong, the connection   is closed and an exception
%   is raised.

gpredis_read(SI, Out) :-
    (   catch(gpredis_read_(SI, Out0), E, true)
    ->  (   var(E)
        ->  Out = Out0
        ;   print_message(error, E),
            disconnect_stream(SI),
            throw(error(redis_error(protocol), _))
        )
    ;   disconnect_stream(SI),
        throw(error(redis_error(protocol), _))
    ).

gpredis_read_(SI, Out) :-
    get_char(SI, ReplyMode),
    gpredis_read(ReplyMode, SI, Out).

gpredis_read(-, SI, Out) :-
    gpredis_get_line(SI, Out),
    format(atom(Err), '~s', [Out]),
    throw(error(redis_error(Err), _)).
gpredis_read(+, SI, Out) :-
    gpredis_get_line(SI, Out2),
    gpredis_wrap_as(status, Out2, Out).
gpredis_read(:, SI, Out) :-
    gpredis_read_number(SI, Out).
gpredis_read($, SI, Out) :-
    gpredis_read_number(SI, Length),
    gpredis_read_bulk(SI, Length, Out).
gpredis_read(*, SI, Out) :-
    gpredis_read_number(SI, Length),
    gpredis_mbulk_reply(SI, Length, Out).
gpredis_read(end_of_file, _, _) :-
    throw(error(socket_error(end_of_file, 'Unexpected end of file'),_)).

gpredis_mbulk_reply(_, -1, nil).
gpredis_mbulk_reply(_, 0, []) :-
    !.
gpredis_mbulk_reply(SI, N, [H|T]) :-
    get_char(SI, ReplyMode),
    gpredis_read(ReplyMode, SI, H),
    N1 is N-1,
    gpredis_mbulk_reply(SI, N1, T).

gpredis_read_number(SI, N) :-
    gpredis_get_line(SI, Line),
    number_string(N, Line).

gpredis_crlf(SI) :-
    get_code(SI,13),
    get_code(SI,10),
    !.
gpredis_crlf(SI) :-
    throw(error(redis_protocol_error(SI, crlf_expected), _)).

gpredis_read_bulk(_, -1, nil) :-
    !.
gpredis_read_bulk(SI, Len, Value) :-
    stream_pair(SI, In, _Out),
    setup_call_cleanup(
        stream_range_open(In, Range, [size(Len)]),
        ( set_stream(Range, encoding(utf8)),
          read_string(Range, _, String)
        ),
        close(Range)),
    gpredis_crlf(SI),
    unstringify(String, Value).

gpredis_get_line(SI, Line) :-
    read_string(SI, "\n", "\r", _Sep, Line).

gpredis_wrap_as(_, nil, Out) :-
    !,
    Out = nil.
gpredis_wrap_as(Type, Value, Out) :-
    Out =.. [Type, Value].


%!  gpredis_build_cmd(+Command, -String) is det.
%
%   Building a redis command is very simple...   Req is a term whose
%   functor name is the name of the  redis command and the arguments
%   are the command  arguments,  some   examples  should  paint  the
%   picture:
%
%     - gpredis_build_cmd(info).
%     - gpredis_build_cmd(info(clients)).
%     - gpredis_build_cmd(keys(*)).
%     - gpredis_build_cmd(keys('users:*')).
%     - gpredis_build_cmd(set("users:eric:logged_in", 1)).
%
%   Of course, you can substitute  *instantiated variables* anywhere
%   in the above to pass through the   current  value as part of the
%   outgoing command.

gpredis_build_cmd(Req, X) :-
    Req =.. [Cmd|Args],
    gpredis_cmdargs([Cmd|Args], Args2),
    atomics_to_string(Args2, CmdData),
    length(Args, N),
    NArgs is N+1,
    format(string(X), '*~d\r\n~s', [NArgs, CmdData]).

gpredis_cmdargs([], []).
gpredis_cmdargs([Arg|Args], [ArgLen, "\r\n", X, "\r\n" | Output]) :-
    gpredis_stringify(Arg, X),
    utf_string_length(X, XLen),
    format(string(ArgLen), "$~d", [XLen]),
    gpredis_cmdargs(Args, Output).

gpredis_stringify(X,Y) :-
    string(X),
    !,
    X = Y.
gpredis_stringify(X,Y) :-
    atom(X),
    !,
    X = Y.
gpredis_stringify(X, Y) :-
    compound(X),
    X = prolog(V),
    !,
    format(string(Y), '\u0000T\u0000~k', [V]).
gpredis_stringify(X,Y) :-
    !,
    format(string(Y), '~w', [X]).

unstringify(S, V) :-
    string_concat('\u0000T\u0000', S1, S),
    term_string(V0, S1),
    !,
    V = V0.
unstringify(S, S).

utf_string_length(S, Len) :-
    setup_call_cleanup(
        open_null_stream(Out),
        (   write(Out, S),
            byte_count(Out, Len)
        ),
        close(Out)).




		 /*******************************
		 *            MESSAGES		*
		 *******************************/

:- multifile
    prolog:error_message//1.

prolog:error_message(redis_error(String)) -->
    [ 'REDIS: ~s'-[String] ].
