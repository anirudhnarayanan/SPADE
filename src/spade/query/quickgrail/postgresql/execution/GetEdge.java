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

import spade.query.quickgrail.core.execution.AbstractGetEdge;
import spade.query.quickgrail.core.kernel.ExecutionContext;
import spade.query.quickgrail.postgresql.core.PostgreSQLEnvironment;
import spade.query.quickgrail.postgresql.entities.PostgreSQLGraph;
import spade.query.quickgrail.postgresql.entities.PostgreSQLGraphMetadata;
import spade.storage.PostgreSQL;
import spade.storage.PostgreSQLSchema;

/**
 * Get the a set of edges in a graph.
 */
public class GetEdge
	extends AbstractGetEdge<PostgreSQLGraph, PostgreSQLGraphMetadata, PostgreSQLEnvironment, PostgreSQL>{

	public GetEdge(PostgreSQLGraph targetGraph, PostgreSQLGraph subjectGraph, String field, String operation, String value){
		super(targetGraph, subjectGraph, field, operation, value);
	}

	@Override
	public void execute(PostgreSQLEnvironment env, ExecutionContext ctx, PostgreSQL storage){
		PostgreSQLSchema schema = storage.getSchema();

		String query = "insert into " + schema.formatTableNameForQuery(targetGraph.getEdgeTableName()) +
				" select " + schema.formatColumnNameForQuery(schema.hashColumnName) + " from " +
				schema.formatTableNameForQuery(schema.mainEdgeTableName) + " where ";
		
		if(!field.equals("*")){
			query += schema.formatColumnNameForQuery(field) + " " + operation + " " +
					schema.formatStringAsVarCharForQuery(value);
		}
		
		if(!env.IsBaseGraph(subjectGraph)){
			query += " and " + schema.formatColumnNameForQuery(schema.hashColumnName) + " in "
					+ "(select " + schema.formatColumnNameForQuery(schema.hashColumnName) + " from "
					+ schema.formatTableNameForQuery(subjectGraph.getEdgeTableName()) + ")";
		}
		
		query += " group by " + schema.formatColumnNameForQuery(schema.hashColumnName) + ";";
		
		storage.executeQuery(query);
	}

}
