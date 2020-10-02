:- include(testing_framework).
:- use_module('../redis').


test_create_hash_with_data :-
	redis_connect(C),
	redis(C, flushall, status("OK")),
	redis(C, exists(test_hash), 0),
	redis(C, hset(test_hash, name, 'Emacs The Viking'), 1),
	redis(C, hset(test_hash, age, 48), 1),
	redis(C, hset(test_hash, status, "Thinking"), 1),
	redis(C, exists(test_hash), 1),
	redis_disconnect(C).


test_previously_created_keys_exist :-
	redis_connect(C),
	redis(C, hlen(test_hash), 3),
	redis(C, hexists(test_hash, name), 1),
	redis(C, hexists(test_hash, age), 1),
	redis(C, hexists(test_hash, status), 1),
	redis_disconnect(C).


test_values_of_previously_created_keys :-
	redis_connect(C),
	redis(C, hget(test_hash, name), "Emacs The Viking"),
	redis(C, hget(test_hash, age), "48"),
	redis(C, hget(test_hash, status), "Thinking"),
	redis_disconnect(C).


test_integer_increment_of_hash_value :-
	redis_connect(C),
	redis(C, hincrby(test_hash, age, -20), 28),
	redis(C, hincrby(test_hash, age, 20), 48),
	redis_disconnect(C).


test_float_increment_of_hash_value :-
	redis_connect(C),
	redis(C, hincrbyfloat(test_hash, age, -0.5), "47.5"),
	redis(C, hincrbyfloat(test_hash, age, 1.5), "49"),
	redis_disconnect(C).

test_setting_multiple_keys_at_once :-
	redis_connect(C),
	redis(C, hmset(test_hash,
		new_field_1, "Hello",
		new_field_2, "World",
		new_field_3, 42), status("OK")),
	redis_disconnect(C).


test_getting_multiple_keys_previously_set :-
	redis_connect(C),
	redis(C, hmget(test_hash, new_field_1, new_field_2, new_field_3),
		["Hello", "World", "42"]),
	redis_disconnect(C).

test_getting_all_hash_keys_at_once :-
	redis_connect(C),
	redis(C, hgetall(test_hash), X),
	length(X, 12),
	redis_disconnect(C).


test_deleting_some_existing_fields :-
	redis_connect(C),
	redis(C, hdel(test_hash, name), 1),
	redis(C, hdel(test_hash, age), 1),
	redis(C, hdel(test_hash, status), 1),
	redis(C, hdel(test_hash, unknown), 0),
	redis(C, hlen(test_hash), 3),
	redis_disconnect(C).
