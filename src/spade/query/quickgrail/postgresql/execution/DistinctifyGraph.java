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

import spade.query.quickgrail.core.execution.AbstractDistinctifyGraph;
import spade.query.quickgrail.core.kernel.ExecutionContext;
import spade.query.quickgrail.postgresql.core.PostgreSQLEnvironment;
import spade.query.quickgrail.postgresql.entities.PostgreSQLGraph;
import spade.query.quickgrail.postgresql.entities.PostgreSQLGraphMetadata;
import spade.storage.PostgreSQL;
import spade.storage.PostgreSQLSchema;

/**
 * Remove all duplicated vertices and edges.
 */
public class DistinctifyGraph
	extends AbstractDistinctifyGraph<PostgreSQLGraph, PostgreSQLGraphMetadata, PostgreSQLEnvironment, PostgreSQL>{

	public DistinctifyGraph(PostgreSQLGraph targetGraph, PostgreSQLGraph sourceGraph){
		super(targetGraph, sourceGraph);
	}

	@Override
	public void execute(PostgreSQLEnvironment env, ExecutionContext ctx, PostgreSQL storage){
		PostgreSQLSchema schema = storage.getSchema();
		
		final String columnNameHash = schema.formatTableNameForQuery(schema.hashColumnName);
		
		final String tableNameTargetVertex = 
				schema.formatTableNameForQuery(targetGraph.getVertexTableName());
		final String tableNameTargetEdge = 
				schema.formatTableNameForQuery(targetGraph.getEdgeTableName());
		final String tableNameSourceVertex = 
				schema.formatTableNameForQuery(sourceGraph.getVertexTableName());
		final String tableNameSourceEdge = 
				schema.formatTableNameForQuery(sourceGraph.getEdgeTableName());

		storage.executeQuery("insert into " + tableNameTargetVertex +
				" select " + columnNameHash + " from " + tableNameSourceVertex + " group by " + columnNameHash + ";");
		storage.executeQuery("insert into " + tableNameTargetEdge +
				" select " + columnNameHash + " from " + tableNameSourceEdge + " group by " + columnNameHash + ";");
	}
}
