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

import spade.query.quickgrail.core.execution.AbstractLimitGraph;
import spade.query.quickgrail.core.kernel.ExecutionContext;
import spade.query.quickgrail.postgresql.core.PostgreSQLEnvironment;
import spade.query.quickgrail.postgresql.entities.PostgreSQLGraph;
import spade.query.quickgrail.postgresql.entities.PostgreSQLGraphMetadata;
import spade.storage.PostgreSQL;
import spade.storage.PostgreSQLSchema;

/**
 * Sample a subset of vertices / edges from a graph.
 */
public class LimitGraph
	extends AbstractLimitGraph<PostgreSQLGraph, PostgreSQLGraphMetadata, PostgreSQLEnvironment, PostgreSQL>{
	
	public LimitGraph(PostgreSQLGraph targetGraph, PostgreSQLGraph sourceGraph, int limit){
		super(targetGraph, sourceGraph, limit);
	}

	@Override
	public void execute(PostgreSQLEnvironment env, ExecutionContext ctx, PostgreSQL storage){
		String sourceVertexTable = sourceGraph.getVertexTableName();
		String sourceEdgeTable = sourceGraph.getEdgeTableName();

		long numVertices = storage.getRowCountOfTableSafe(sourceVertexTable);
		long numEdges = storage.getRowCountOfTableSafe(sourceEdgeTable);

		PostgreSQLSchema schema = storage.getSchema();
		
		final String hashColumnFormatted = schema.formatColumnNameForQuery(schema.hashColumnName);
		
		final String sourceVertexFormatted = schema.formatTableNameForQuery(sourceVertexTable);
		final String sourceEdgeFormatted = schema.formatTableNameForQuery(sourceEdgeTable);
		
		final String targetVertexFormatted = schema.formatTableNameForQuery(targetGraph.getVertexTableName());
		final String targetEdgeFormatted = schema.formatTableNameForQuery(targetGraph.getEdgeTableName());
		
		if(numVertices > 0){
			storage.executeQuery("insert into " + targetVertexFormatted + " select " + 
					hashColumnFormatted + " from " + sourceVertexFormatted + " group by " + 
					hashColumnFormatted + " limit " + limit + ";");

		}
		if(numEdges > 0){
			storage.executeQuery("insert into " + targetEdgeFormatted + " select " + 
					hashColumnFormatted + " from " + sourceEdgeFormatted + " group by " + 
					hashColumnFormatted + " limit " + limit + ";");
		}
	}

}
