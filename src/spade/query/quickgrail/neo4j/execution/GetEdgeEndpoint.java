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
import static spade.query.quickgrail.neo4j.core.CommonVariables.RelationshipTypes.EDGE;

import spade.query.quickgrail.core.entities.Graph.EdgeComponent;
import spade.query.quickgrail.core.execution.AbstractGetEdgeEndpoint;
import spade.query.quickgrail.core.kernel.ExecutionContext;
import spade.query.quickgrail.neo4j.core.Neo4jEnvironment;
import spade.query.quickgrail.neo4j.entities.Neo4jGraph;
import spade.query.quickgrail.neo4j.entities.Neo4jGraphMetadata;
import spade.storage.Neo4j;

/**
 * Get end points of all edges in a graph.
 */
public class GetEdgeEndpoint 
	extends AbstractGetEdgeEndpoint<Neo4jGraph, Neo4jGraphMetadata, Neo4jEnvironment, Neo4j>{
	
	public GetEdgeEndpoint(Neo4jGraph targetGraph, Neo4jGraph subjectGraph, EdgeComponent component){
		super(targetGraph, subjectGraph, component);
	}

	@Override
	public void execute(Neo4jEnvironment env, ExecutionContext ctx, Neo4j storage){
		String subjectEdgeTable = subjectGraph.getEdgeTableName();
		String targetVertexTable = targetGraph.getVertexTableName();
		String query = "MATCH (child)-[" + EDGE_ALIAS + ":" + EDGE.toString() + "]->(parent) ";
		if(!env.IsBaseGraph(subjectGraph))
		{
			query += "WHERE " + EDGE_ALIAS + ".quickgrail_symbol CONTAINS '," + subjectEdgeTable + ",' ";
		}
		if(component == EdgeComponent.kSource || component == EdgeComponent.kBoth)
		{
			query += " SET child:" + targetVertexTable;
		}
		if(component == EdgeComponent.kDestination || component == EdgeComponent.kBoth)
		{
			query += " SET parent:" + targetVertexTable;
		}
		storage.executeQuery(query);
	}
}
