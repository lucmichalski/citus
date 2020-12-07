/*-------------------------------------------------------------------------
 *
 * deparse_statistics_stmts.c
 *	  All routines to deparse statistics statements.
 *	  This file contains all entry points specific for statistics statement deparsing
 *    as well as functions that are currently only used for deparsing of the statistics
 *    statements.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "distributed/citus_ruleutils.h"
#include "distributed/deparser.h"
#include "distributed/relay_utility.h"
#include "lib/stringinfo.h"
#include "nodes/nodes.h"
#include "utils/builtins.h"

static void AppendCreateStatisticsStmt(StringInfo buf, CreateStatsStmt *stmt, uint64
									   shardId);
static void AppendStatisticsName(StringInfo buf, CreateStatsStmt *stmt, uint64 shardId);
static void AppendStatTypes(StringInfo buf, CreateStatsStmt *stmt);
static void AppendColumnNames(StringInfo buf, CreateStatsStmt *stmt);
static void AppendTableName(StringInfo buf, CreateStatsStmt *stmt, uint64 shardId);

char *
DeparseCreateStatisticsStmt(CreateStatsStmt *stmt, uint64 shardId)
{
	StringInfoData str;
	initStringInfo(&str);

	AppendCreateStatisticsStmt(&str, stmt, shardId);

	return str.data;
}


static void
AppendCreateStatisticsStmt(StringInfo buf, CreateStatsStmt *stmt, uint64 shardId)
{
	appendStringInfo(buf, "CREATE STATISTICS ");

	appendStringInfo(buf, "%s", stmt->if_not_exists ? "IF NOT EXISTS " : "");

	AppendStatisticsName(buf, stmt, shardId);

	AppendStatTypes(buf, stmt);

	appendStringInfo(buf, " ON ");

	AppendColumnNames(buf, stmt);

	appendStringInfo(buf, " FROM ");

	AppendTableName(buf, stmt, shardId);

	appendStringInfo(buf, ";");
}


static void
AppendStatisticsName(StringInfo buf, CreateStatsStmt *stmt, uint64 shardId)
{
	Value *statNameVal = (Value *) linitial(stmt->defnames);
	char *statName = strVal(statNameVal);
	RangeVar *relation = (RangeVar *) linitial(stmt->relations);
	char *schemaName = relation->schemaname;

	if (schemaName != NULL)
	{
		appendStringInfo(buf, "%s.", quote_identifier(schemaName));
	}

	appendStringInfo(buf, "%s_%ld", quote_identifier(statName), shardId);
}


static void
AppendStatTypes(StringInfo buf, CreateStatsStmt *stmt)
{
	if (list_length(stmt->stat_types) == 0)
	{
		return;
	}

	appendStringInfo(buf, " (");

	ListCell *cell = NULL;
	foreach(cell, stmt->stat_types)
	{
		Value *statType = (Value *) lfirst(cell);

		appendStringInfoString(buf, strVal(statType));

		if (cell != list_tail(stmt->stat_types))
		{
			appendStringInfo(buf, ", ");
		}
	}

	appendStringInfo(buf, ")");
}


static void
AppendColumnNames(StringInfo buf, CreateStatsStmt *stmt)
{
	ListCell *cell = NULL;
	foreach(cell, stmt->exprs)
	{
		Node *node = (Node *) lfirst(cell);
		Assert(IsA(node, ColumnRef));

		ColumnRef *column = (ColumnRef *) node;
		Assert(list_length(column->fields) == 1);

		char *columnName = strVal((Value *) linitial(column->fields));

		appendStringInfoString(buf, columnName);

		if (cell != list_tail(stmt->exprs))
		{
			appendStringInfo(buf, ", ");
		}
	}
}


static void
AppendTableName(StringInfo buf, CreateStatsStmt *stmt, uint64 shardId)
{
	RangeVar *relation = (RangeVar *) linitial(stmt->relations);
	char *relationName = relation->relname;
	char *schemaName = relation->schemaname;

	if (schemaName != NULL)
	{
		appendStringInfo(buf, "%s.", quote_identifier(schemaName));
	}

	appendStringInfo(buf, "%s_%ld", quote_identifier(relationName), shardId);
}
