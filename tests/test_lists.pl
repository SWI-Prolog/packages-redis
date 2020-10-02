:- include(testing_framework).
:- use_module('../redis').



test_create_a_list_with_a_single_value :-
	redis_connect(C),
	redis(C, flushall, status("OK")),
	redis(C, lpush(test_list, 42), 1),
	redis(C, llen(test_list), 1),
	redis_disconnect(C).


test_pop_only_entry_from_a_list :-
	redis_connect(C),
	redis(C, lpop(test_list), "42"),
	redis(C, llen(test_list), 0),
	redis_disconnect(C).


test_create_a_list_with_multiple_values_lpush :-
	redis_connect(C),
	redis(C, lpush(test_list, "Hello", world, 42), 3),
	redis(C, llen(test_list), 3),
	redis_disconnect(C).


test_lrange_on_existing_list_with_lpush :-
	redis_connect(C),
	redis(C, lrange(test_list, 0, -1),
		 ["42", "world", "Hello"]),
	redis_disconnect(C).


test_get_length_of_existing_list :-
	redis_connect(C),
	redis(C, llen(test_list), 3),
	redis_disconnect(C).


test_get_values_by_lindex_position :-
	redis_connect(C),
	redis(C, lindex(test_list,1), "world"),
	redis(C, lindex(test_list,2), "Hello"),
	redis(C, lindex(test_list,0), "42"),
	redis_disconnect(C).


test_add_to_list_with_linset_command :-
	redis_connect(C),
	redis(C, linsert(test_list, before, 42,"FRIST"), 4),
	redis(C, linsert(test_list, after, world, 'custard creams rock'), 5),
	redis(C, lindex(test_list, 3), "custard creams rock"),
	redis(C, lindex(test_list, -1), "Hello"),
	redis(C, lindex(test_list, -3), "world"),
	redis(C, lindex(test_list, 0), "FRIST"),
	redis_disconnect(C).


test_popping_with_lpop_and_rpop :-
	redis_connect(C),
	redis(C, lpop(test_list), "FRIST"),
	redis(C, rpop(test_list), "Hello"),
	redis(C, lpop(test_list), "42"),
	redis(C, rpop(test_list), "custard creams rock"),
	redis(C, lpop(test_list), "world"),
	redis_disconnect(C).
