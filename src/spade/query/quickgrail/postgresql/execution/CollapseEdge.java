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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import spade.query.quickgrail.core.execution.AbstractCollapseEdge;
import spade.query.quickgrail.core.kernel.ExecutionContext;
import spade.query.quickgrail.postgresql.core.PostgreSQLEnvironment;
import spade.query.quickgrail.postgresql.entities.PostgreSQLGraph;
import spade.query.quickgrail.postgresql.entities.PostgreSQLGraphMetadata;
import spade.storage.PostgreSQL;
import spade.storage.PostgreSQLSchema;

/**
 * Collapse all edges whose specified fields are the same.
 */
public class CollapseEdge
	extends AbstractCollapseEdge<PostgreSQLGraph, PostgreSQLGraphMetadata, PostgreSQLEnvironment, PostgreSQL>{
	
	public CollapseEdge(PostgreSQLGraph targetGraph, PostgreSQLGraph sourceGraph, ArrayList<String> fields){
		super(targetGraph, sourceGraph, fields);
	}

	@Override
	public void execute(PostgreSQLEnvironment env, ExecutionContext ctx, PostgreSQL storage){
		PostgreSQLSchema schema = storage.getSchema();
		
		List<String> edgeTableColumnNames = schema.getEdgeTableColumnNames();
		Set<String> missingColumns = new HashSet<String>();
		for(String field : getFields()){
			if(!edgeTableColumnNames.contains(field)){
				missingColumns.add(field);
			}
		}
		if(!missingColumns.isEmpty()){
			throw new RuntimeException("Cannot collapse edges because of not-present fields: " + missingColumns);
		}
		
		final String tableNameMainEdge = 
				schema.formatTableNameForQuery(schema.mainEdgeTableName);
		
		final String tableNameTargetVertex = 
				schema.formatTableNameForQuery(targetGraph.getVertexTableName());
		final String tableNameTargetEdge = 
				schema.formatTableNameForQuery(targetGraph.getEdgeTableName());
		final String tableNameSourceVertex = 
				schema.formatTableNameForQuery(sourceGraph.getVertexTableName());
		final String tableNameSourceEdge = 
				schema.formatTableNameForQuery(sourceGraph.getEdgeTableName());
		
		final String columnNameHash = 
				schema.formatColumnNameForQuery(schema.hashColumnName);
		final String columnNameChildHash = 
				schema.formatColumnNameForQuery(schema.childVertexHashColumnName);
		final String columnNameParentHash = 
				schema.formatColumnNameForQuery(schema.parentVertexHashColumnName);
		
		storage.executeQuery("INSERT INTO " + tableNameTargetVertex +
				" SELECT " + columnNameHash + " FROM " + tableNameSourceVertex + ";");

		StringBuilder groups = new StringBuilder();

		ArrayList<String> fields = getFields();
		for(int i = 0; i < fields.size(); ++i){
			groups.append(", " + schema.formatColumnNameForQuery(fields.get(i)));
		}

		storage.executeQuery(
				"INSERT INTO " + tableNameTargetEdge +
				" SELECT MIN(e." + columnNameHash + ") FROM " + tableNameMainEdge + " e" +
				" WHERE e." + columnNameHash + " IN (SELECT " + columnNameHash + " FROM " + tableNameSourceEdge + ")" +
				" GROUP BY " + columnNameChildHash + ", " + columnNameParentHash + "" + groups.toString() + ";");
	}

	// quickstep
	// INSERT INTO targetEdgeTable
	// SELECT MIN(e.id) FROM edge e, edge_anno ea0, edge_anno ea1
	// WHERE e.id IN (SELECT id FROM sourceEdgeTable)
	// AND e.id=ea0.id AND ea0.field='field_value1' AND e.id=ea1.id AND ea1.field='field_value2'
	// GROUP BY src, dst, ea0.value, ea1.value

	// postgres
	// INSERT INTO targetEdgeTable
	// SELECT MIN(e.hash) FROM edge e
	// WHERE e.id IN (SELECT id FROM sourceEdgeTable)
	// GROUP BY src, dst, field1, field2
}
