:- include(testing_framework).
:- use_module('../redis').


%% This file shows the use of my test framework to test for a simple
%% expectation that if it fails, the expected and actual values are
%% displayed. In reality, using the more natural Prolog unification
%% / pattern matching idiom would be preferred to keep the test code
%% leaner and meaner, resorting to the tf_equals/2 etc to pinpoint
%% a failing test until success.


%% CONNECTION...

test_default_connection_and_echo :-
	redis_connect(C),
	redis(C, echo('GNU Prolog rocks!'), Output),
	redis_disconnect(C),
	tf_equals(Output, "GNU Prolog rocks!").


test_explicit_connection_and_echo :-
	redis_connect(C, localhost, 6379),
	redis(C, echo('GNU Prolog rocks!'), Output),
	redis_disconnect(C),
	tf_equals(Output, "GNU Prolog rocks!").


test_ping_the_server :-
	redis_connect(C),
	redis(C, ping, status(Output)),
	redis_disconnect(C),
	tf_equals(Output, "PONG").

%% SERVER...

test_set_and_get_client_name :-
	redis_connect(C),
	redis(C, client(setname, "Objitsu"), status(Set)),
	redis(C, client(getname), Get),
	redis_disconnect(C),
	tf_equals(Set, "OK"),
	tf_equals(Get, "Objitsu").


test_set_and_get_timeout :-
	redis_connect(C),
	redis(C, config(set, timeout, 86400), status(Set)),
	redis(C, config(get, timeout), [Key, Val]),
	redis_disconnect(C),
	tf_equals(Set, "OK"),
	tf_equals(Key, "timeout"),
	tf_equals(Val, "86400").


test_flushall_flushdb_and_dbsize :-
	redis_connect(C),
	redis(C, flushall, status(OK1)),
	redis(C, set(test_key_1, "Hello"), status(OK2)),
	redis(C, get(test_key_1), Val),
	redis(C, dbsize, Size1),
	redis(C, flushdb, status(OK3)),
	redis(C, dbsize, Size2),
	redis_disconnect(C),
	tf_equals(OK1, "OK"),
	tf_equals(OK2, "OK"),
	tf_equals(OK3, "OK"),
	tf_equals(Val, "Hello"),
	tf_equals(Size1, 1),
	tf_equals(Size2, 0).

%% KEYS...

test_key_creation_exists_set_get_and_deletion :-
	redis_connect(C),
	redis(C, flushall, status(OK1)),
	redis(C, exists(test_key_1), Exists0),
	redis(C, set(test_key_1, "Hello"), status(OK2)),
	redis(C, exists(test_key_1), Exists1),
	redis(C, del(test_key_1), Del1),
	redis(C, exists(test_key_1), Exists2),
	redis_disconnect(C),
	tf_equals(OK1, "OK"),
	tf_equals(OK2, "OK"),
	tf_equals(Exists0, 0),
	tf_equals(Exists1, 1),
	tf_equals(Del1, 1),
	tf_equals(Exists2, 0).

test_key_expiry_with_set_ttl_expire_and_exists :-
	redis_connect(C),
	redis(C, flushall, status(OK1)),
	redis(C, set(test_key_1, "Hello"), status(OK2)),
	redis(C, ttl(test_key_1), Minus1),
	redis(C, expire(test_key_1, 1), Set1),
	sleep(2),
	redis(C, exists(test_key_1), Exists0),
	redis_disconnect(C),
	tf_equals(OK1, "OK"),
	tf_equals(OK2, "OK"),
	tf_equals(Set1, 1),
	tf_equals(Minus1, -1),
	tf_equals(Exists0, 0).
