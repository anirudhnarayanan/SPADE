/*
 --------------------------------------------------------------------------------
 SPADE - Support for Provenance Auditing in Distributed Environments.
 Copyright (C) 2018 SRI International

 This program is free software: you can redistribute it and/or
 modify it under the terms of the GNU General Public License as
 published by the Free Software Foundation, either version 3 of the
 License, or (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program. If not, see <http://www.gnu.org/licenses/>.
 --------------------------------------------------------------------------------
 */
package spade.query.quickgrail.postgresql.execution;

import spade.query.quickgrail.core.execution.AbstractGetLink;
import spade.query.quickgrail.core.kernel.ExecutionContext;
import spade.query.quickgrail.postgresql.core.PostgreSQLEnvironment;
import spade.query.quickgrail.postgresql.entities.PostgreSQLGraph;
import spade.query.quickgrail.postgresql.entities.PostgreSQLGraphMetadata;
import spade.storage.PostgreSQL;
import spade.storage.PostgreSQLSchema;

/**
 * Similar to GetPath but treats the graph edges as indirected.
 */
public class GetLink
	extends AbstractGetLink<PostgreSQLGraph, PostgreSQLGraphMetadata, PostgreSQLEnvironment, PostgreSQL>{
	
	public GetLink(PostgreSQLGraph targetGraph, PostgreSQLGraph subjectGraph,
				   PostgreSQLGraph srcGraph, PostgreSQLGraph dstGraph,
				   Integer maxDepth){
		super(targetGraph, subjectGraph, srcGraph, dstGraph, maxDepth);
	}

	@Override
	public void execute(PostgreSQLEnvironment env, ExecutionContext ctx, PostgreSQL storage){
		PostgreSQLSchema schema = storage.getSchema();
		final String cursorTable = "m_cur";
		final String nextTable = "m_next";
		final String answerTable = "m_answer";
		
		final String mainEdgeTableFormatted = schema.formatTableNameForQuery(schema.mainEdgeTableName);
		final String hashColumnFormatted = schema.formatColumnNameForQuery(schema.hashColumnName);
		final String subjectEdgeTableFormatted = schema.formatTableNameForQuery(subjectGraph.getEdgeTableName());
		final String childHashFormatted = schema.formatColumnNameForQuery(schema.childVertexHashColumnName);
		final String parentHashFormatted = schema.formatColumnNameForQuery(schema.parentVertexHashColumnName);
		
		storage.executeQuery(
				schema.queriesDropAndCreateHashOnlyTable(cursorTable) +
				schema.queriesDropAndCreateHashOnlyTable(nextTable) +
				schema.queriesDropAndCreateHashOnlyTable(answerTable)
				);
		
		String filter;
		if(env.IsBaseGraph(subjectGraph)){
			filter = "";
		}else{
			filter = " and " + mainEdgeTableFormatted + "." + hashColumnFormatted + " in (select " + 
					hashColumnFormatted + " from " + subjectEdgeTableFormatted + ")";
		}

		final String subgraphRefTable = "m_sgconn";
		final String subgraphRefTableFormatted = schema.formatTableNameForQuery(subgraphRefTable);
		final String depthName = "depth";
		final String depthNameFormatted = schema.formatColumnNameForQuery(depthName);
		
		final String cursorTableFormatted = schema.formatTableNameForQuery(cursorTable);
		final String answerTableFormatted = schema.formatTableNameForQuery(answerTable);
		final String nextTableFormatted = schema.formatTableNameForQuery(nextTable);
		
		// Create subgraph edges table.
		storage.executeQuery("drop table if exists " + subgraphRefTableFormatted + ";" +
				" create table " + subgraphRefTableFormatted + "(" + childHashFormatted + " uuid, " +
				parentHashFormatted + " uuid, "+depthNameFormatted+" int);");

		storage.executeQuery(
				"insert into " + cursorTableFormatted + " select " + hashColumnFormatted + " from " + 
				schema.formatTableNameForQuery(destinationGraph.getVertexTableName()) + ";" +
				"insert into "+answerTableFormatted+" select " + hashColumnFormatted + " from " + cursorTableFormatted + ";");

		final String depthStringReplacer = "<depth>";
		
		String loopStmts = 
				"insert into " + subgraphRefTableFormatted + " select " + childHashFormatted + ", " +
				parentHashFormatted + ", " + depthStringReplacer + " from " + mainEdgeTableFormatted +
				" where " + parentHashFormatted + " in (select " + hashColumnFormatted + " from " + 
				cursorTableFormatted + ") " + filter + ";";
		loopStmts += 
				"insert into " + subgraphRefTableFormatted + " select " + childHashFormatted + ", " + parentHashFormatted +
				", " + depthStringReplacer + " from " + mainEdgeTableFormatted + " where " + childHashFormatted + " in " +
				"(select " + hashColumnFormatted + " from " + cursorTableFormatted + ") " + filter + ";";
		loopStmts += 
				schema.queriesDropAndCreateHashOnlyTable(nextTable);
		loopStmts += 
				"insert into " + nextTableFormatted + " select " + childHashFormatted + " from " + mainEdgeTableFormatted +
				" where " + parentHashFormatted + " in (select " + hashColumnFormatted + " from " + cursorTableFormatted +
				") " + filter + " group by " + childHashFormatted + ";";
		loopStmts += 
				"insert into " + nextTableFormatted + " select " + parentHashFormatted + " from " + mainEdgeTableFormatted +
				" where " + childHashFormatted + " in (select " + hashColumnFormatted + " from " + cursorTableFormatted +
				") " + filter + " group by " + parentHashFormatted + ";";
		loopStmts += 
				schema.queriesDropAndCreateHashOnlyTable(cursorTable);
		loopStmts += 
				"insert into " + cursorTableFormatted + " select " + hashColumnFormatted + " from " + nextTableFormatted +
				" where " + hashColumnFormatted + " not in (select " + hashColumnFormatted + " from " + answerTableFormatted +
				");";
		loopStmts += 
				"insert into " + answerTableFormatted + " select " + hashColumnFormatted + " from " + cursorTableFormatted + ";";
		
		for(int i = 0; i < maxDepth; ++i){
			String resolvedLoopStmts = loopStmts.replace(depthStringReplacer, String.valueOf((i + 1)));
			storage.executeQuery(resolvedLoopStmts);
			long size = storage.getRowCountOfTableSafe(cursorTable);
			if(size <= 0){
				break;
			}
		}

		storage.executeQuery(
				schema.queriesDropAndCreateHashOnlyTable(cursorTable) +
				schema.queriesDropAndCreateHashOnlyTable(nextTable));

		storage.executeQuery(
				"insert into "+cursorTableFormatted+" select " + hashColumnFormatted + " from " + 
				schema.formatTableNameForQuery(sourceGraph.getVertexTableName()) +
				" where + " + hashColumnFormatted + " in (select " + hashColumnFormatted + " from " +
				answerTableFormatted + ");");

		storage.executeQuery(
				schema.queriesDropAndCreateHashOnlyTable(answerTable) +
				"insert into "+answerTableFormatted+" select " + hashColumnFormatted + " from "+cursorTableFormatted+";");

		// TODO not fully tested
		String secondLoopStmts =
				schema.queriesDropAndCreateHashOnlyTable(nextTable) +
				"insert into " + nextTableFormatted + " select " + parentHashFormatted + " from " + subgraphRefTableFormatted +
				" where " + childHashFormatted + " in (select " + hashColumnFormatted + " from " + cursorTableFormatted + ")" +
				" and "+depthNameFormatted + " + " + depthStringReplacer + " <= " + maxDepth + " group by " +
				parentHashFormatted + ";";
		secondLoopStmts +=
				"insert into " + nextTableFormatted + " select " + childHashFormatted + " from " + subgraphRefTableFormatted +
				" where " + parentHashFormatted + " in (select " + hashColumnFormatted + " from " + cursorTableFormatted + ")" +
				" and " + depthNameFormatted + " + " + depthStringReplacer + " <= " + maxDepth + " group by " +
				childHashFormatted + ";";
		secondLoopStmts +=
				schema.queriesDropAndCreateHashOnlyTable(cursorTable);
		secondLoopStmts +=
				"insert into " + cursorTableFormatted + " select " + hashColumnFormatted + " from " + nextTableFormatted +
				" where " + hashColumnFormatted + " not in (select " + hashColumnFormatted + " from " + answerTableFormatted +
				");";
		secondLoopStmts +=
				"insert into " + answerTableFormatted + " select " + hashColumnFormatted + " from " + cursorTableFormatted + ";";
		
		for(int i = 0; i < maxDepth; ++i){
			String resolvedSecondLoopStmts = secondLoopStmts.replace(depthStringReplacer, String.valueOf(i));
			storage.executeQuery(resolvedSecondLoopStmts);
			long size = storage.getRowCountOfTableSafe(cursorTable);
			if(size <= 0){
				break;
			}
		}

		final String targetVertexTable = targetGraph.getVertexTableName();
		final String targetEdgeTable = targetGraph.getEdgeTableName();
		
		final String targetVertexTableFormatted = schema.formatTableNameForQuery(targetVertexTable);
		final String targetEdgeTableFormatted = schema.formatTableNameForQuery(targetEdgeTable);

		storage.executeQuery(
				"insert into " + targetVertexTableFormatted + " select " + hashColumnFormatted + " from " +
				answerTableFormatted + ";" +
				"insert into " + targetEdgeTableFormatted + " select " + hashColumnFormatted + " from " + 
				mainEdgeTableFormatted + " where " + childHashFormatted + " in (select " + hashColumnFormatted +
				" from " + answerTableFormatted + ") where " + childHashFormatted + " in (select " + hashColumnFormatted + 
				" from " + answerTableFormatted + ") and " + parentHashFormatted + " in (select " + hashColumnFormatted +
				" from " + answerTableFormatted + ") " + filter + ";");

		storage.executeQuery(
				schema.queryDropTableIfExists(cursorTable) + 
				schema.queryDropTableIfExists(nextTable) + 
				schema.queryDropTableIfExists(answerTable) + 
				schema.queryDropTableIfExists(subgraphRefTable));
	}
}
