cmake_minimum_required(VERSION 3.5)
project(swipl-redis)

include("../cmake/PrologPackage.cmake")

swipl_plugin(redis
	     PL_LIBS redis.pl)

swipl_plugin(redis4pl
	     C_SOURCES redis4pl.c)

pkg_doc(redis
	SECTION
	SOURCES
	    overview.md
	SOURCE
	    redis.pl libredis.tex)

test_libs(redis)
