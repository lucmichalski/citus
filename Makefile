# cstore_fdw/Makefile
#
# Copyright (c) 2016 Citus Data, Inc.
#

MODULE_big = cstore_fdw

PG_CPPFLAGS = -std=c11
OBJS = cstore.o cstore_fdw.o cstore_writer.o cstore_reader.o \
       cstore_compression.o mod.o cstore_metadata_tables.o cstore_tableam.o

EXTENSION = cstore_fdw
DATA = cstore_fdw--1.7.sql cstore_fdw--1.6--1.7.sql  cstore_fdw--1.5--1.6.sql cstore_fdw--1.4--1.5.sql \
	   cstore_fdw--1.3--1.4.sql cstore_fdw--1.2--1.3.sql cstore_fdw--1.1--1.2.sql \
	   cstore_fdw--1.0--1.1.sql cstore_fdw--1.7--1.8.sql

REGRESS = fdw_create fdw_load fdw_query fdw_analyze fdw_data_types fdw_functions \
	  fdw_block_filtering fdw_drop fdw_insert fdw_copyto fdw_alter fdw_truncate
EXTRA_CLEAN = cstore.pb-c.h cstore.pb-c.c data/*.cstore data/*.cstore.footer \
              sql/block_filtering.sql sql/create.sql sql/data_types.sql sql/load.sql \
              sql/copyto.sql expected/block_filtering.out expected/create.out \
              expected/data_types.out expected/load.out expected/copyto.out

ifeq ($(enable_coverage),yes)
	PG_CPPFLAGS += --coverage
	SHLIB_LINK  += --coverage
	EXTRA_CLEAN += *.gcno
endif

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
	PG_CPPFLAGS += -I/usr/local/include
endif

#
# Users need to specify their Postgres installation path through pg_config. For
# example: /usr/local/pgsql/bin/pg_config or /usr/lib/postgresql/9.3/bin/pg_config
#

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

ifndef MAJORVERSION
    MAJORVERSION := $(basename $(VERSION))
endif

ifeq (,$(findstring $(MAJORVERSION), 9.3 9.4 9.5 9.6 10 11 12))
    $(error PostgreSQL 9.3 to 12 is required to compile this extension)
endif

installcheck: remove_cstore_files

remove_cstore_files:
	rm -f data/*.cstore data/*.cstore.footer

reindent:
	citus_indent .
