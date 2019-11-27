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

import static spade.query.quickgrail.postgresql.core.CommonVariables.PRIMARY_KEY;
import static spade.query.quickgrail.postgresql.core.PostgresUtil.formatString;

import spade.query.quickgrail.core.execution.AbstractGetVertex;
import spade.query.quickgrail.core.kernel.ExecutionContext;
import spade.query.quickgrail.postgresql.core.PostgreSQLEnvironment;
import spade.query.quickgrail.postgresql.entities.PostgreSQLGraph;
import spade.query.quickgrail.postgresql.entities.PostgreSQLGraphMetadata;
import spade.storage.PostgreSQL;

/**
 * Get the a set of vertices in a graph.
 */
public class GetVertex
	extends AbstractGetVertex<PostgreSQLGraph, PostgreSQLGraphMetadata, PostgreSQLEnvironment, PostgreSQL>{
	
	public GetVertex(PostgreSQLGraph targetGraph, PostgreSQLGraph subjectGraph, String field, String operation, String value){
		super(targetGraph, subjectGraph, field, operation, value);
	}

	@Override
	public void execute(PostgreSQLEnvironment env, ExecutionContext ctx, PostgreSQL storage){
		StringBuilder sqlQuery = new StringBuilder(100);
		sqlQuery.append("INSERT INTO " + targetGraph.getVertexTableName() +
				" SELECT " + PRIMARY_KEY + " FROM " + PostgreSQLGraph.GetBaseVertexAnnotationTableName());
		if(field != null)
		{
			sqlQuery.append(" WHERE ");
			// TODO: handle wild card columns
			if(!field.equals("*"))
			{
				sqlQuery.append(formatString(field, true) + operation + formatString(value, false));
			}
			if(!env.IsBaseGraph(subjectGraph))
			{
				sqlQuery.append(" AND " + PRIMARY_KEY + " IN (SELECT " + PRIMARY_KEY + " FROM " +
						subjectGraph.getVertexTableName() + ")");
			}
		}
		sqlQuery.append(" GROUP BY " + PRIMARY_KEY + ";");
		storage.executeQuery(sqlQuery.toString());
	}
}
