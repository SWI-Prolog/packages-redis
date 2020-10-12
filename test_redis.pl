/*  Part of SWI-Prolog

    Author:        Jan Wielemaker
    E-mail:        jan@swi-prolog.org
    WWW:           http://www.swi-prolog.org
    Copyright (c)  2020, SWI-Prolog Solutions b.v.
    All rights reserved.

    Redistribution and use in source and binary forms, with or without
    modification, are permitted provided that the following conditions
    are met:

    1. Redistributions of source code must retain the above copyright
       notice, this list of conditions and the following disclaimer.

    2. Redistributions in binary form must reproduce the above copyright
       notice, this list of conditions and the following disclaimer in
       the documentation and/or other materials provided with the
       distribution.

    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
    "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
    LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
    FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
    COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
    INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
    BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
    LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
    CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
    LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
    ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
    POSSIBILITY OF SUCH DAMAGE.
*/

:- module(test_redis,
          [ test_redis/0,
            test_redis/1                % +Options
          ]).
:- use_module(library(plunit)).
:- use_module(library(redis_streams)).
:- use_module(library(apply)).
:- use_module(library(debug)).

/** <module> Redis client tests

These tests are for a  large  part  based   on  the  test  suite for the
GNU-Prolog redis client by Sean Charles.
*/

:- dynamic
    resp/1.

%!  test_redis
%
%   Run the Redis tests.  These  are  only   executed  if  there  is  an
%   environment variable ``SWIPL_REDIS_SERVER`` with value <host>:<port>
%   For successful execution there must be a  Redis server that does not
%   require a password at this location. If this server supports version
%   3 of the Redis protocol the tests   are  executed in version 3 mode.
%   Otherwise in version 2 mode.

test_redis :-
    retractall(resp(_)),
    redis_server_address(Address), !,
    (   hello_at(Address)
    ->  test_redis([version(3)])
    ;   test_redis([])
    ).
test_redis :-
    print_message(informational,
                  test_redis(no_server)).

test_redis(Options) :-
    redis_server_address(Address),
    redis_server(test_redis, Address, Options),
    option(version(V), Options, 2),
    retractall(resp(_)),
    asserta(resp(V)),
    run_tests([ redis_operation,
                redis_strings,
                redis_lists,
                redis_hashes,
                redis_prolog,
                redis_groups
              ]).

redis_server_address(Host:Port) :-
    getenv('SWIPL_REDIS_SERVER', Server),
    split_string(Server, ":", "", [HostS,PortS]),
    atom_string(Host, HostS),
    number_string(Port, PortS).

hello_at(Address) :-
    setup_call_cleanup(
        redis_connect(Address, C, []),
        redis_current_command(C, hello),
        redis_disconnect(C)).


:- begin_tests(redis_operation).

test(default_connection_and_echo, Reply == "GNU Prolog rocks!") :-
    run([ echo('GNU Prolog rocks!') - Reply
        ], []).
test(ping_the_server, Reply == status("PONG")) :-
    run([ ping - Reply
        ], []).
test(set_and_get_client_name, Client == "Objitsu") :-
    run([ client(setname, "Objitsu"),
          client(getname) - Client
        ], []).
test(set_and_get_timeout, Reply == OK) :-
    run([ config(set, timeout, 86400),
          config(get, timeout) - Reply
        ], []),
    (   resp(2)
    ->  OK = ["timeout", "86400"]
    ;   OK = ["timeout"-"86400"]
    ).
test(dbsize, Val == "Hello") :-
    run([ dbsize - Size1,
          set(test_key_1, "Hello"),
          get(test_key_1) - Val,
          dbsize - Size2
        ], [test_key_1]),
    assertion(Size2-Size1 =:= 1).
test(key_creation_exists_set_get_and_deletion) :-
    run([ exists(test_key_1) -  Exists0,
          assertion(Exists0 == 0),
          set(test_key_1, "Hello"),
          exists(test_key_1) - Exists1,
          assertion(Exists1 == 1),
          del(test_key_1) - Del1,
          assertion(Del1 == 1),
          exists(test_key_1) - Exists2,
          assertion(Exists2 == 0)
        ], [test_key_1]).
test(key_expiry_with_set_ttl_expire_and_exists, condition(resp(2))) :-
    run([ set(test_key_1, "Hello"),
          ttl(test_key_1) - Minus1,
          assertion(Minus1 == -1),
          expire(test_key_1, 1) - Set1,
          assertion(Set1 == 1),
          call(sleep(1.5)),
          exists(test_key_1) - Exists0,
          assertion(Exists0 == 0)
        ], [test_key_1]).
test(key_expiry_with_set_ttl_expire_and_exists, condition(resp(3))) :-
    run([ set(test_key_1, "Hello"),
          ttl(test_key_1) - Minus1,
          assertion(Minus1 == -1),
          pexpire(test_key_1, 100) - Set1,
          assertion(Set1 == 1),
          call(sleep(0.2)),
          exists(test_key_1) - Exists0,
          assertion(Exists0 == 0)
        ], [test_key_1]).

:- end_tests(redis_operation).

:- begin_tests(redis_strings).

test(get_and_set, S == "Hello World") :-
    run([ set(test_string, 'Hello World') - status("OK"),
          get(test_string) - S
        ], [test_string]).
test(set_and_get_with_expiry, condition(resp(2))) :-
    run([ set(test_string, 'Miller time!', ex, 1),
          call(sleep(1.5)),
          \+ get(test_string)
        ], [test_string]).
test(set_and_get_with_expiry, condition(resp(3))) :-
    run([ set(test_string, 'Miller time!', px, 100),
          call(sleep(0.2)),
          \+ get(test_string)
        ], [test_string]).
test(append_to_an_existing_string) :-
    run([ set(test_string, 'GNU Prolog'),
          append(test_string, ' is Cool') - Len,
          assertion(Len == 18),
          strlen(test_string) - Len2,
          assertion(Len2 == 18)
        ], [test_string]).
test(counting_bits_in_a_string, Count == 4) :-
    run([ set('bitbucket(!)', 'U'),
          bitcount('bitbucket(!)') - Count
        ], ['bitbucket(!)']).
test(get_and_set_unicode, Reply == S) :-
    numlist(0, 10000, L),
    string_codes(S, L),
    run([ set(test_string, S) - status("OK"),
          get(test_string) - Reply
        ], [test_string]).

:- end_tests(redis_strings).

:- begin_tests(redis_lists).

test(create_a_list_with_a_single_value, Len1 == 1) :-
    run([ lpush(test_list, 42) - R1,
          assertion(R1 == 1),
          llen(test_list) - Len1
        ], [test_list]).
test(pop_only_entry_from_a_list, Pop1 == "42") :-
    run([ lpush(test_list, 42),
          lpop(test_list) - Pop1,
          llen(test_list) - Len0,
          assertion(Len0 == 0)
        ], [test_list]).
test(create_a_list_with_multiple_values_lpush, Len == 3) :-
    run([ lpush(test_list, "Hello", world, 42),
          llen(test_list) - Len
        ], [test_list]).
test(lrange_on_existing_list_with_lpush, List == ["42", "world", "Hello"]) :-
    run([ lpush(test_list, "Hello", world, 42),
          lrange(test_list, 0, -1) - List
        ], [test_list]).
test(get_values_by_lindex_position) :-
    run([ lpush(test_list, "Hello", world, 42),
          lindex(test_list,1) - World,
          assertion(World == "world"),
          lindex(test_list,2) - Hello,
          assertion(Hello == "Hello"),
          lindex(test_list,0) - FourthyTwo,
          assertion(FourthyTwo == "42")
        ], [test_list]).
test(add_to_list_with_linset_command) :-
    run([ lpush(test_list, "Hello", world, 42),
          linsert(test_list, before, 42,"FRIST") - 4,
          linsert(test_list, after, world, 'custard creams rock') - 5,
          lindex(test_list, 3) - "custard creams rock",
          lindex(test_list, -1) - "Hello",
          lindex(test_list, -3) - "world",
          lindex(test_list, 0) - "FRIST"
        ], [test_list]).
test(popping_with_lpop_and_rpop) :-
    run([ rpush(test_list, "FRIST", 42, world, "custard creams rock", 'Hello'),
          lpop(test_list) - "FRIST",
          rpop(test_list) - "Hello",
          lpop(test_list) - "42",
          rpop(test_list) - "custard creams rock",
          lpop(test_list) - "world"
        ], [test_list]).

:- end_tests(redis_lists).

:- begin_tests(redis_hashes).

test(create_hash_with_data) :-
    run([ exists(test_hash) - 0,
          hset(test_hash, name, 'Emacs The Viking') - 1,
          hset(test_hash, age, 48) - 1,
          hset(test_hash, status, "Thinking") - 1,
          exists(test_hash) - 1
        ], [test_hash]).
test(previously_created_keys_exist) :-
    run([ hset(test_hash,
               name, 'Emacs The Viking',
               age, 48,
               status, "Thinking") - 3,
          hlen(test_hash) - 3,
          hexists(test_hash, name) - 1,
          hexists(test_hash, age) - 1,
          hexists(test_hash, status) - 1,
          hexists(test_hash, gender) - 0
        ], [test_hash]).
test(values_of_previously_created_keys) :-
    run([ hset(test_hash,
               name, 'Emacs The Viking',
               age, 48,
               status, "Thinking") - 3,
          hget(test_hash, name) - "Emacs The Viking",
          hget(test_hash, age) - "48",
          hget(test_hash, status) - "Thinking"
        ], [test_hash]).
test(increment_of_hash_value) :-
    run([ hset(test_hash,
               name, 'Emacs The Viking',
               age, 48,
               status, "Thinking") - 3,
          hincrby(test_hash, age, -20) - 28,
          hincrby(test_hash, age, 20) - 48,
          hincrbyfloat(test_hash, age, -0.5) - "47.5",
          hincrbyfloat(test_hash, age, 1.5) - "49"
        ], [test_hash]).
test(multiple_keys_at_once, List == ["Hello", "World", "42"]) :-
    run([ hset(test_hash,
               new_field_1, "Hello",
               new_field_2, "World",
               new_field_3, 42) - 3,
          hmget(test_hash, new_field_1, new_field_2, new_field_3) - List
        ], [test_hash]).
test(getting_all_hash_keys_at_once, Len == OK) :-
    run([ hset(test_hash,
               new_field_1, "Hello",
               new_field_2, "World",
               new_field_3, 42) - 3,
          hgetall(test_hash) - List
        ], [test_hash]),
    length(List, Len),
    (   resp(2)
    ->  OK = 6
    ;   OK = 3
    ).
test(deleting_some_existing_fields) :-
    run([ hset(test_hash,
               name, 'Emacs The Viking',
               age, 48,
               status, "Thinking") - 3,
          hdel(test_hash, name) - 1,
          hdel(test_hash, age) - 1,
          hdel(test_hash, unknown) - 0,
          hlen(test_hash) - 1
        ], [test_hash]).

:- end_tests(redis_hashes).

:- begin_tests(redis_prolog).

test(prolog_value) :-
    run([ set(test_key, prolog(hello(world))),
          get(test_key) - hello(world)
        ], [test_key]).

% List transfer
test(empty_list, Reply == []) :-
    run([ exists(test_no_list) - 0,
          call(redis_get_list(test_redis, test_no_list, Reply))
        ], [test_no_list]).
test(list_length_1, Reply == ["one"]) :-
    run([ rpush(test_list, one),
          call(redis_get_list(test_redis, test_list, Reply))
        ], [test_list]).
test(list_length_2, Reply == ["one", "two"]) :-
    run([ rpush(test_list, one, two),
          call(redis_get_list(test_redis, test_list, Reply))
        ], [test_list]).
test(list_long, L2 == List) :-
    numlist(1, 1000, List),
    run([ call(redis_set_list(test_redis, test_list, List)),
          call(redis_get_list(test_redis, test_list, L1)),
          call(maplist(number_string, L2, L1))
        ], [test_list]).

% Hash transfer
test(empty_hash, Reply =@= _{}) :-
    run([ exists(test_no_hash) - 0,
          call(redis_get_hash(test_redis, test_no_hash, Reply))
        ], [test_no_hash]).
test(one_hash, Reply =@= _{name:"Jan Wielemaker"}) :-
    run([ hset(test_hash,
               name, 'Jan Wielemaker') - 1,
          call(redis_get_hash(test_redis, test_hash, Reply))
        ], [test_hash]).
test(trip_hash, Reply =@= Hash) :-
    Hash = _{ name: "Jan Wielemaker",
              status: "Testing"
            },
    run([ call(redis_set_hash(test_redis, test_hash, Hash)),
          call(redis_get_hash(test_redis, test_hash, Reply))
        ], [test_hash]).

:- end_tests(redis_prolog).


%!  run(+Command, +Clean)

run(List, Clean) :-
    setup_call_cleanup(
        redis_connect(test_redis, C, []),
        maplist(raction(C), List),
        rcleanup(C, Clean)).

raction(C, Action-Reply) :-
    !,
    (   var(Reply)
    ->  redis(C, Action, Reply)
    ;   redis(C, Action, Reply0),
        assertion(Reply0 == Reply)
    ).
raction(C, \+ Action) :-
    !,
    \+ raction(C, Action).
raction(_, call(Goal)) :-
    !,
    call(Goal).
raction(_, assertion(Goal)) :-
    !,
    assertion(Goal).
raction(C, Action) :-
    redis(C, Action).

rcleanup(C, Clean) :-
    maplist(rdel(C), Clean),
    redis_disconnect(C).

rdel(C, Key) :-
    redis(C, del(Key), _).


		 /*******************************
		 *             STREAMS		*
		 *******************************/

:- begin_tests(redis_groups).

test(primes, [setup(clean_group), %cleanup(clean_group),
              Len == 1000]) :-
    xprimes(1, 1000),
    xwait,
    redis(test_redis, llen(test_primes), Len).

:- end_tests(redis_groups).

clean_group :-
    redis(test_redis, del(test_candidates, test_primes), _).

make_group :-
    catch(redis(test_redis,
                xgroup(create, test_candidates, test_primes, $, mkstream)),
          error(redis_error(busygroup,_),_),
          true).

xprimes(Low, High) :-
    make_group,
    listen_primes(bob),
    listen_primes(alice),
    forall(between(Low, High, I),
           add_candidate(I, test_primes)),
    xadd(test_redis, test_candidates, _, _{leave:bob}),
    xadd(test_redis, test_candidates, _, _{leave:alice}).

add_candidate(I, Into) :-
    get_time(Now),
    xadd(test_redis, test_candidates, _, _{candidate:I, time:Now, drain:Into}).

listen_primes(Consumer) :-
    thread_create(xlisten_group(test_redis,
                                test_primes, Consumer, [test_candidates],
                                [ block(0.1)
                                ]),
                  _, [alias(Consumer), detached(true)]).

:- listen(redis_consume(test_candidates, Data, Context),
          check_prime_string(Data, Context)).

check_prime_string(Data, Context) :-
    number_string(N, Data.get(candidate)),
    number_string(T0, Data.get(time)),
    !,
    call_time(is_prime(N), Dict, True),
    get_time(T1),
    T is T1-T0,
    redis(test_redis,
          rpush(Data.drain, prolog(p(N,True,Context.consumer,Dict.cpu,T))),
          _).
check_prime_string(Data, Context) :-
    !,
    Consumer = Data.get(leave),
    (   atom_string(Context.consumer, Consumer)
    ->  debug(test, '~p: asked to leave ~p (accept)~n',
              [Context.consumer, Consumer]),
        xconsumer_stop(false)
    ;   debug(test, '~p: asked to leave ~p (reject)~n',
              [Context.consumer, Consumer]),
        fail
    ).

is_prime(1) :- !.
is_prime(2) :- !.
is_prime(N) :-
    End is floor(sqrt(N)),
    (   between(2, End, I),
        N mod I =:= 0
    ->  !, fail
    ;   true
    ).

xwait :-
    xwait(test_redis, 10, test_candidates, test_primes).

xwait(Redis, MaxTime, Stream, Group) :-
    get_time(T0),
    End is T0+MaxTime,
    repeat,
    redis(Redis, xpending(Stream, Group), [Pending|_]),
    (   Pending =:= 0
    ->  !
    ;   get_time(T1),
        T1 > End
    ->  !, fail
    ;   %format(user_error, "\rPending: ~D", [Pending]),
        sleep(0.1),
        fail
    ).


		 /*******************************
		 *            MESSAGES		*
		 *******************************/

:- multifile
    prolog:message//1.

prolog:message(test_redis(no_server)) -->
    [ 'REDIS tests: enable by setting SWIPL_REDIS_SERVER to host:port'-[] ].
