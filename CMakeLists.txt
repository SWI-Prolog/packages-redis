cmake_minimum_required(VERSION 3.5)
project(swipl-redis)

include("../cmake/PrologPackage.cmake")

swipl_plugin(redis
	     PL_LIBS redis.pl)

pkg_doc(redis
	SECTION
	SOURCE redis.pl libredis.tex)
