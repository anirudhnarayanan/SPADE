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

import java.util.ArrayList;

import spade.query.quickgrail.core.entities.Graph.Direction;
import spade.query.quickgrail.core.execution.AbstractGetLineage;
import spade.query.quickgrail.core.kernel.ExecutionContext;
import spade.query.quickgrail.postgresql.core.PostgreSQLEnvironment;
import spade.query.quickgrail.postgresql.entities.PostgreSQLGraph;
import spade.query.quickgrail.postgresql.entities.PostgreSQLGraphMetadata;
import spade.storage.PostgreSQL;
import spade.storage.PostgreSQLSchema;

/**
 * Get the lineage of a set of vertices in a graph.
 */
public class GetLineage
	extends AbstractGetLineage<PostgreSQLGraph, PostgreSQLGraphMetadata, PostgreSQLEnvironment, PostgreSQL>{
	
	public GetLineage(PostgreSQLGraph targetGraph, PostgreSQLGraph subjectGraph,
					  PostgreSQLGraph startGraph, Integer depth, Direction direction){
		super(targetGraph, subjectGraph, startGraph, depth, direction);
	}

	@Override
	public void execute(PostgreSQLEnvironment env, ExecutionContext ctx, PostgreSQL storage){
		ArrayList<Direction> oneDirs = new ArrayList<>();
		if(direction == Direction.kBoth){
			oneDirs.add(Direction.kAncestor);
			oneDirs.add(Direction.kDescendant);
		}else{
			if(direction == Direction.kAncestor || direction == Direction.kDescendant){
				oneDirs.add(direction);
			}else{
				throw new RuntimeException("Unknown direction: " + direction);
			}
		}

		PostgreSQLSchema schema = storage.getSchema();
		
		String targetVertexTable = targetGraph.getVertexTableName();
		String targetEdgeTable = targetGraph.getEdgeTableName();
		String subjectEdgeTable = subjectGraph.getEdgeTableName();
		
		String filter = "";
		if(!env.IsBaseGraph(subjectGraph)){
			filter = " AND edge." + schema.formatColumnNameForQuery(schema.hashColumnName) + 
					" in (select " + schema.formatColumnNameForQuery(schema.hashColumnName) + " from " + 
					schema.formatTableNameForQuery(subjectEdgeTable) + ")";
		}

		String answerTable = "m_answer";
		String cursorTable = "m_cur";
		String nextTable = "m_next";
		String edgeAnswerTable = "m_answer_edge";
		
		for(Direction oneDir : oneDirs){
			executeOneDirection(oneDir, storage, filter, 
					answerTable, cursorTable, nextTable, edgeAnswerTable);
			storage.executeQuery(
					"insert into " + schema.formatTableNameForQuery(targetVertexTable) + 
					" select " + schema.formatColumnNameForQuery(schema.hashColumnName) + " from " +
					schema.formatTableNameForQuery(answerTable) + "; insert into " +
					schema.formatTableNameForQuery(targetEdgeTable) + " select " +
					schema.formatColumnNameForQuery(schema.hashColumnName) + " from " +
					schema.formatTableNameForQuery(answerTable) + " group by " + 
					schema.formatColumnNameForQuery(schema.hashColumnName) + ";");
		}

		storage.executeQuery(
				schema.queryDropTableIfExists(cursorTable) +
				schema.queryDropTableIfExists(nextTable) +
				schema.queryDropTableIfExists(answerTable) +
				schema.queryDropTableIfExists(edgeAnswerTable));
	}

	private void executeOneDirection(Direction dir, PostgreSQL storage, String filter, 
			String answerTable, String cursorTable, String nextTable, String edgeAnswerTable){
		PostgreSQLSchema schema = storage.getSchema();
		String formattedSrcName, formattedDstName;
		if(dir == Direction.kAncestor){
			formattedSrcName = schema.formatColumnNameForQuery(schema.childVertexHashColumnName);
			formattedDstName = schema.formatColumnNameForQuery(schema.parentVertexHashColumnName);
		}else if(dir == Direction.kDescendant){
			formattedSrcName = schema.formatColumnNameForQuery(schema.parentVertexHashColumnName);
			formattedDstName = schema.formatColumnNameForQuery(schema.childVertexHashColumnName);
		}else{
			throw new RuntimeException("Unknown direction: " + dir);
		}

		storage.executeQuery(
				schema.queriesDropAndCreateHashOnlyTable(cursorTable) +
				schema.queriesDropAndCreateHashOnlyTable(nextTable) +
				schema.queriesDropAndCreateHashOnlyTable(answerTable) +
				schema.queriesDropAndCreateHashOnlyTable(edgeAnswerTable));

		String startVertexTable = startGraph.getVertexTableName();
		
		final String formattedHashColumn = schema.formatColumnNameForQuery(schema.hashColumnName);
		final String formattedStartVertexTable = schema.formatTableNameForQuery(startVertexTable);
		final String formattedCursorTable = schema.formatTableNameForQuery(cursorTable);
		final String formattedNextTable = schema.formatTableNameForQuery(nextTable);
		final String formattedAnswerTable = schema.formatTableNameForQuery(answerTable);
		final String formattedEdgeAnswerTable = schema.formatTableNameForQuery(edgeAnswerTable);
		final String formattedMainEdgeTable = schema.formatTableNameForQuery(schema.mainEdgeTableName);
		
		storage.executeQuery(
				"insert into "+formattedCursorTable+" select " + formattedHashColumn + " from " + 
				formattedStartVertexTable + ";" +
				"insert into "+formattedAnswerTable+" select " + formattedHashColumn + " from "+formattedCursorTable+";");

		String loopStmts =
				schema.queriesDropAndCreateHashOnlyTable(nextTable) +
				"insert into " + formattedNextTable + " select " + formattedDstName + " from " +
				formattedMainEdgeTable + " where " + formattedSrcName + " in (select " + formattedHashColumn + 
				" from " + formattedCursorTable + ")" + filter + " group by " + formattedDstName + ";"; 
				
		loopStmts += 
				"insert into " + formattedEdgeAnswerTable + " select " + formattedHashColumn + " from " + 
				formattedMainEdgeTable + " where " + formattedSrcName + " in (select " + formattedHashColumn + 
				" from " + formattedCursorTable + ") " + filter + ";";
		
		loopStmts += 
				schema.queriesDropAndCreateHashOnlyTable(cursorTable);
		
		loopStmts += 
				"insert into " + formattedCursorTable + " select " + formattedHashColumn + " from " +
				formattedNextTable + " where " + formattedHashColumn + " not in (select " + formattedHashColumn + 
				" from " + formattedAnswerTable + ");";
		
		loopStmts += 
				"insert into " + formattedAnswerTable + " select " + formattedHashColumn + " from " +
				formattedCursorTable + ";";
		
		for(int i = 0; i < depth; ++i){
			storage.executeQuery(loopStmts);

			long numEdges = storage.getRowCountOfTableSafe(cursorTable);
			if(numEdges <= 0){
				break;
			}
		}
	}
}
