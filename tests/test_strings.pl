:- include(testing_framework).
:- use_module('../redis').

test_basic_string_get_and_set :-
	redis_connect(C),
	redis(C, flushall, status("OK")),
	redis(C, set(test_string, 'Hello World'), status("OK")),
	redis(C, get(test_string), "Hello World"),
	redis_disconnect(C).


test_extended_set_and_get_with_expiry :-
	redis_connect(C),
	redis(C, flushall, status("OK")),
	redis(C, set(test_string, 'Miller time!', ex, 1), status("OK")),
	sleep(2),
	\+ redis(C, get(test_string), _),
	redis_disconnect(C).


test_append_to_an_existing_string :-
	redis_connect(C),
	redis(C, set(test_string, 'GNU Prolog'), status("OK")),
	redis(C, append(test_string, ' is Cool'), 18),
	redis(C, strlen(test_string), 18),
	redis_disconnect(C).


test_counting_bits_in_a_string :-
	redis_connect(C),
	redis(C, flushall, status("OK")),
	redis(C, set('bitbucket(!)', 'U'), status("OK")),
	redis(C, bitcount('bitbucket(!)'), 4),
	redis_disconnect(C).
