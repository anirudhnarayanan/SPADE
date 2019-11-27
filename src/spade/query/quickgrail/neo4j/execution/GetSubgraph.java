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

import spade.query.quickgrail.core.execution.AbstractGetSubgraph;
import spade.query.quickgrail.core.kernel.ExecutionContext;
import spade.query.quickgrail.neo4j.core.Neo4jEnvironment;
import spade.query.quickgrail.neo4j.entities.Neo4jGraph;
import spade.query.quickgrail.neo4j.entities.Neo4jGraphMetadata;
import spade.storage.Neo4j;

/**
 * Let $S be the subject graph and $T be the skeleton graph.
 * The operation $S.getSubgraph($T) is to find all the vertices and edges that
 * are spanned by the skeleton graph.
 */
public class GetSubgraph
	extends AbstractGetSubgraph<Neo4jGraph, Neo4jGraphMetadata, Neo4jEnvironment, Neo4j>{

	public GetSubgraph(Neo4jGraph targetGraph, Neo4jGraph subjectGraph, Neo4jGraph skeletonGraph){
		super(targetGraph, subjectGraph, skeletonGraph);
	}

	@Override
	public void execute(Neo4jEnvironment env, ExecutionContext ctx, Neo4j storage){
		String targetVertexTable = targetGraph.getVertexTableName();
		String targetEdgeTable = targetGraph.getEdgeTableName();
		String subjectVertexTable = subjectGraph.getVertexTableName();
		String skeletonVertexTable = skeletonGraph.getVertexTableName();

		String cypherQuery = "MATCH (" + VERTEX_ALIAS + ":" + subjectVertexTable + ":" + skeletonVertexTable + ")" +
				"-[" + EDGE_ALIAS + ":" + EDGE.toString() + "]-" +
				"(n:" + subjectVertexTable + ":" + skeletonVertexTable + ")" +
				" SET " + VERTEX_ALIAS + ":" + targetVertexTable +

				" SET " + EDGE_ALIAS + ".quickgrail_symbol = CASE WHEN NOT EXISTS(" + EDGE_ALIAS +
				".quickgrail_symbol) THEN '," + targetEdgeTable + ",'" +
				" WHEN " + EDGE_ALIAS + ".quickgrail_symbol CONTAINS '," +
				targetEdgeTable + ",' THEN " + EDGE_ALIAS + ".quickgrail_symbol " +
				" ELSE " + EDGE_ALIAS + ".quickgrail_symbol + '," + targetEdgeTable + ",' END";
		storage.executeQuery(cypherQuery);
	}
}
