cmake_minimum_required(VERSION 3.5)
project(swipl-redis)

include("../cmake/PrologPackage.cmake")

swipl_plugin(redis
	     PL_LIBS redis.pl redis_streams.pl)

swipl_plugin(redis4pl
	     C_SOURCES redis4pl.c)

pkg_doc(redis
	SECTION
	SOURCES
	    overview.md redis_streams.pl
	SOURCE
	    redis.pl libredis.tex)

test_libs(redis)

set(DEMO_FILES
    common.pl  prime-node.pl  primes.pl  README.md)
prepend(DEMO_FILES demo ${DEMO_FILES})

swipl_examples(${DEMO_FILES})
