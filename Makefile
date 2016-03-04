PROJECT = texas_pgsql

DEPS = epgsql lager bucs
dep_epgsql = git https://github.com/epgsql/epgsql.git master
dep_lager = git https://github.com/basho/lager.git master
dep_bucs = git https://github.com/botsunit/bucs.git master

include erlang.mk

