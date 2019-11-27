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

import spade.query.quickgrail.core.entities.Graph.EdgeComponent;
import spade.query.quickgrail.core.execution.AbstractGetEdgeEndpoint;
import spade.query.quickgrail.core.kernel.ExecutionContext;
import spade.query.quickgrail.postgresql.core.PostgreSQLEnvironment;
import spade.query.quickgrail.postgresql.entities.PostgreSQLGraph;
import spade.query.quickgrail.postgresql.entities.PostgreSQLGraphMetadata;
import spade.storage.PostgreSQL;
import spade.storage.PostgreSQLSchema;

/**
 * Get end points of all edges in a graph.
 */
public class GetEdgeEndpoint
	extends AbstractGetEdgeEndpoint<PostgreSQLGraph, PostgreSQLGraphMetadata, PostgreSQLEnvironment, PostgreSQL>{
	
	public GetEdgeEndpoint(PostgreSQLGraph targetGraph, PostgreSQLGraph subjectGraph, EdgeComponent component){
		super(targetGraph, subjectGraph, component);
	}

	@Override
	public void execute(PostgreSQLEnvironment env, ExecutionContext ctx, PostgreSQL storage){
		PostgreSQLSchema schema = storage.getSchema();
		
		String targetVertexTable = targetGraph.getVertexTableName();
		String subjectEdgeTable = subjectGraph.getEdgeTableName();

		String mAnswerTableName = "m_answer";
		
		storage.executeQuery(schema.queriesDropAndCreateHashOnlyTable(mAnswerTableName));

		String columnNames[] = null;
		
		if(component == EdgeComponent.kBoth){
			columnNames = new String[]{schema.childVertexHashColumnName, schema.parentVertexHashColumnName};
		}else{
			if(component == EdgeComponent.kSource){
				columnNames = new String[]{schema.childVertexHashColumnName};
			}else if(component == EdgeComponent.kDestination){
				columnNames = new String[]{schema.parentVertexHashColumnName};
			}else{
				throw new RuntimeException("Unknown component type: " + component);
			}
		}
		
		for(String columnName : columnNames){
			storage.executeCopy("insert into " + schema.formatTableNameForQuery(mAnswerTableName) + 
					" select " + schema.formatColumnNameForQuery(columnName) + 
					" from " + schema.formatTableNameForQuery(schema.mainEdgeTableName) + 
					" where " + schema.formatColumnNameForQuery(schema.hashColumnName) + " in " + 
					"(select " + schema.formatColumnNameForQuery(schema.hashColumnName) + " from " +
					schema.formatTableNameForQuery(subjectEdgeTable) + ");");
		}

		storage.executeQuery(
				"insert into " + schema.formatTableNameForQuery(targetVertexTable) + 
				" select " + schema.formatColumnNameForQuery(schema.hashColumnName) + " from " +
				schema.formatTableNameForQuery(mAnswerTableName) + " group by " +
				schema.formatColumnNameForQuery(schema.hashColumnName) + ";" + 
				schema.queryDropTableIfExists(mAnswerTableName));
	}
}
