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

import static spade.query.quickgrail.postgresql.core.CommonVariables.CHILD_VERTEX_KEY;
import static spade.query.quickgrail.postgresql.core.CommonVariables.EDGE_TABLE;
import static spade.query.quickgrail.postgresql.core.CommonVariables.PARENT_VERTEX_KEY;
import static spade.query.quickgrail.postgresql.core.CommonVariables.PRIMARY_KEY;

import spade.query.quickgrail.core.execution.AbstractGetPath;
import spade.query.quickgrail.core.kernel.ExecutionContext;
import spade.query.quickgrail.postgresql.core.PostgreSQLEnvironment;
import spade.query.quickgrail.postgresql.entities.PostgreSQLGraph;
import spade.query.quickgrail.postgresql.entities.PostgreSQLGraphMetadata;
import spade.storage.PostgreSQL;
import spade.storage.PostgreSQLSchema;

/**
 * Get a graph that includes all the paths from a set of source vertices to a
 * set of destination vertices.
 */
public class GetPath
	extends AbstractGetPath<PostgreSQLGraph, PostgreSQLGraphMetadata, PostgreSQLEnvironment, PostgreSQL>{
	
	public GetPath(PostgreSQLGraph targetGraph, PostgreSQLGraph subjectGraph,
				   PostgreSQLGraph srcGraph, PostgreSQLGraph dstGraph,
				   Integer maxDepth){
		super(targetGraph, subjectGraph, srcGraph, dstGraph, maxDepth);
	}

	@Override
	public void execute(PostgreSQLEnvironment env, ExecutionContext ctx, PostgreSQL storage){
//		PostgreSQLGraph ancestorsOfSrcGraph = env.allocateGraph();
//		PostgreSQLGraph descendantsOfDstGraph = env.allocateGraph();
//		GetLineage ancestorsOfSrc = new GetLineage(ancestorsOfSrcGraph, subjectGraph, sourceGraph, 
//				maxDepth, Direction.kAncestor);
//		ancestorsOfSrc.execute(env, ctx, storage);
//		GetLineage descendantsOfDst = new GetLineage(descendantsOfDstGraph, subjectGraph, destinationGraph, 
//				maxDepth, Direction.kDescendant);
//		descendantsOfDst.execute(env, ctx, storage);
//		IntersectGraph intersection = new IntersectGraph(targetGraph, ancestorsOfSrcGraph, descendantsOfDstGraph);
//		intersection.execute(env, ctx, storage);
		
		String targetVertexTable = targetGraph.getVertexTableName();
		String targetEdgeTable = targetGraph.getEdgeTableName();
		
		PostgreSQLSchema schema = storage.getSchema();
		
		final String cursorTable = "m_cur";
		final String nextTable = "m_next";
		final String answerTable = "m_answer";
		final String cursorTableFormatted = schema.formatTableNameForQuery(cursorTable);
		final String nextTableFormatted = schema.formatTableNameForQuery(nextTable);
		final String answerTableFormatted = schema.formatTableNameForQuery(answerTable);

		final String mainEdgeTableFormatted = schema.formatTableNameForQuery(schema.mainEdgeTableName);
		final String hashColumnFormatted = schema.formatColumnNameForQuery(schema.hashColumnName);
		final String childHashFormatted = schema.formatColumnNameForQuery(schema.childVertexHashColumnName);
		final String parentHashFormatted = schema.formatColumnNameForQuery(schema.parentVertexHashColumnName);
		
		storage.executeQuery(
				schema.queriesDropAndCreateHashOnlyTable(cursorTable) +
				schema.queriesDropAndCreateHashOnlyTable(nextTable) +
				schema.queriesDropAndCreateHashOnlyTable(answerTable));

		String filter;
		if(env.IsBaseGraph(subjectGraph)){
			filter = "";
		}else{
			filter = " and " + mainEdgeTableFormatted + "." + hashColumnFormatted + " in (select " + 
					hashColumnFormatted + " from " +
					schema.formatTableNameForQuery(subjectGraph.getEdgeTableName()) + ")";
		}

		final String subgraphRefTable = "m_sgconn";
		final String subgraphRefTableFormatted = schema.formatTableNameForQuery(subgraphRefTable);
		final String depthName = "depth";
		final String depthNameFormatted = schema.formatColumnNameForQuery(depthName);
		final String depthStringReplacer = "<depth>";
		
		// Create subgraph edges table.
		storage.executeQuery(
				schema.queryDropTableIfExists(subgraphRefTable) +
				"create table " + subgraphRefTableFormatted + "(" + childHashFormatted + " uuid, " +
				parentHashFormatted + " uuid, " + depthNameFormatted + " int);");

		storage.executeQuery(
				"insert into "+cursorTableFormatted+" select " + hashColumnFormatted + " from " + 
				schema.formatTableNameForQuery(destinationGraph.getVertexTableName()) + ";" +
				"insert into "+answerTableFormatted+" select " + hashColumnFormatted + " from "+cursorTableFormatted+";");

		String loopStmts =
				"insert into "+subgraphRefTableFormatted+" select " + childHashFormatted + ", " + 
				parentHashFormatted + ", "+depthStringReplacer+" from " + mainEdgeTableFormatted + " where " +
				parentHashFormatted + " in (select " + hashColumnFormatted + " from " + cursorTableFormatted + ") " +
				filter + ";";
		loopStmts +=
				schema.queriesDropAndCreateHashOnlyTable(nextTable) +
				"insert into " + nextTableFormatted + " select " + childHashFormatted + " from " + mainEdgeTableFormatted +
				" where " + parentHashFormatted + " in (select " + hashColumnFormatted + " from " + cursorTableFormatted +
				") " + filter + " group by " + childHashFormatted + ";";
		loopStmts +=
				schema.queriesDropAndCreateHashOnlyTable(cursorTable) +
				"insert into " + cursorTableFormatted + " select " + hashColumnFormatted + " from " + nextTableFormatted +
				" where " + hashColumnFormatted + " not in (select " + hashColumnFormatted + " from " + answerTableFormatted + 
				"); insert into " + answerTableFormatted + " select " + hashColumnFormatted + " from " +
				cursorTableFormatted + ";";
		
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
				"insert into " + cursorTableFormatted + " select " + hashColumnFormatted + " from " +
				schema.formatTableNameForQuery(sourceGraph.getVertexTableName()) + " where " + hashColumnFormatted +
				" in (select " + hashColumnFormatted + " from " + answerTableFormatted + ");");

		storage.executeQuery(
				schema.queriesDropAndCreateHashOnlyTable(answerTable) +
				"insert into "+answerTableFormatted+" select " + hashColumnFormatted + " from "+cursorTableFormatted+";");

		String secondLoopStmts =
				"drop table IF EXISTS m_next;" + "CREATE TABLE m_next (" + PRIMARY_KEY + " UUID);" +
						"INSERT INTO m_next SELECT \"" + PARENT_VERTEX_KEY + "\" FROM m_sgconn" +
						" WHERE \"" + CHILD_VERTEX_KEY + "\" IN (SELECT " + PRIMARY_KEY + " FROM m_cur)" +
						" AND depth + $depth <= " + maxDepth + " GROUP BY \"" + PARENT_VERTEX_KEY + "\";" +
						"INSERT INTO " + targetEdgeTable + " SELECT " + PRIMARY_KEY + " FROM " + EDGE_TABLE +
						" WHERE \"" + CHILD_VERTEX_KEY + "\" IN (SELECT " + PRIMARY_KEY + " FROM m_cur)" +
						" AND \"" + PARENT_VERTEX_KEY + "\" IN (SELECT " + PRIMARY_KEY + " FROM m_next)" + filter + ";" +
						"DROP TABLE IF EXISTS m_cur;" + "CREATE TABLE m_cur (" + PRIMARY_KEY + " UUID);" +
						"INSERT INTO m_cur SELECT " + PRIMARY_KEY + " FROM m_next WHERE " + PRIMARY_KEY +
						" NOT IN (SELECT " + PRIMARY_KEY + " FROM m_answer);" +
						"INSERT INTO m_answer SELECT " + PRIMARY_KEY + " FROM m_cur;";
		for(int i = 0; i < maxDepth; ++i)
		{
			storage.executeQuery(loopStmts.replace("$depth", String.valueOf(i)));

			String worksetSizeQuery = "COPY (SELECT COUNT(*) FROM m_cur) TO stdout;";
			if(storage.executeQueryForLongResult(worksetSizeQuery) == 0)
			{
				break;
			}
		}

		storage.executeQuery("INSERT INTO " + targetVertexTable + " SELECT " + PRIMARY_KEY + " FROM m_answer;");

		storage.executeQuery("DROP TABLE IF EXISTS m_cur;" +
				"DROP TABLE IF EXISTS m_next;" +
				"DROP TABLE IF EXISTS m_answer;" +
				"DROP TABLE IF EXISTS m_sgconn;");
	}
}
