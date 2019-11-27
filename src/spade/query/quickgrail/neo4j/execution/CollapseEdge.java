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
package spade.query.quickgrail.neo4j.execution;

import static spade.query.quickgrail.neo4j.core.CommonVariables.EDGE_ALIAS;
import static spade.query.quickgrail.neo4j.core.CommonVariables.VERTEX_ALIAS;
import static spade.query.quickgrail.neo4j.core.CommonVariables.RelationshipTypes.EDGE;

import java.util.ArrayList;

import spade.query.quickgrail.core.execution.AbstractCollapseEdge;
import spade.query.quickgrail.core.kernel.ExecutionContext;
import spade.query.quickgrail.neo4j.core.Neo4jEnvironment;
import spade.query.quickgrail.neo4j.entities.Neo4jGraph;
import spade.query.quickgrail.neo4j.entities.Neo4jGraphMetadata;
import spade.storage.Neo4j;

/**
 * Collapse all edges whose specified fields are the same.
 */
public class CollapseEdge 
	extends AbstractCollapseEdge<Neo4jGraph, Neo4jGraphMetadata, Neo4jEnvironment, Neo4j>{

	public CollapseEdge(Neo4jGraph targetGraph, Neo4jGraph sourceGraph, ArrayList<String> fields){
		super(targetGraph, sourceGraph, fields);
	}

	@Override
	public void execute(Neo4jEnvironment env, ExecutionContext ctx, Neo4j storage){
		String sourceVertexTable = sourceGraph.getVertexTableName();
		String sourceEdgeTable = sourceGraph.getEdgeTableName();
		String targetVertexTable = targetGraph.getVertexTableName();
		String targetEdgeTable = targetGraph.getEdgeTableName();

		String cypherQuery = "MATCH (" + VERTEX_ALIAS + ":" + sourceVertexTable + ")-[edge:" + EDGE.toString() +
				"]->(n" + ":" + sourceVertexTable + ") ";
		if(!env.IsBaseGraph(sourceGraph))
		{
			cypherQuery += " WHERE edge.quickgrail_symbol CONTAINS '," + sourceEdgeTable + ",'";
		}
		cypherQuery += " SET " + VERTEX_ALIAS + ":" + targetVertexTable +
				" SET n:" + targetVertexTable;
		cypherQuery += " WITH head(collect(edge)) AS " + EDGE_ALIAS;

		StringBuilder groups = new StringBuilder(20);
		for(String field : getFields())
		{
			groups.append(", edge.");
			groups.append(field);
			groups.append(" AS ");
			groups.append(field);
		}
		cypherQuery += groups;
		cypherQuery += " SET " + EDGE_ALIAS + ".quickgrail_symbol = CASE WHEN NOT EXISTS(" + EDGE_ALIAS +
				".quickgrail_symbol) THEN '," + targetEdgeTable + ",'" +
				" WHEN " + EDGE_ALIAS + ".quickgrail_symbol CONTAINS '," +
				targetEdgeTable + ",' THEN " + EDGE_ALIAS + ".quickgrail_symbol " +
				" ELSE " + EDGE_ALIAS + ".quickgrail_symbol + '," + targetEdgeTable + ",' END";

		storage.executeQuery(cypherQuery);
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
	// GROUP BY src, dst, field1, field2, ..., fieldn

	// neo4j
	// MATCH (v)-[e]->(n)
	// WITH head(collect(e)) as edge, e.field1 as field1, e.field2 as field2, ...
	// RETURN edge, field1, field2, ...
}
