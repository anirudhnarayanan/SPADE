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
import static spade.query.quickgrail.neo4j.core.Neo4jStorageHelper.formatSymbol;

import spade.query.quickgrail.core.entities.Graph.Direction;
import spade.query.quickgrail.core.execution.AbstractGetLineage;
import spade.query.quickgrail.core.kernel.ExecutionContext;
import spade.query.quickgrail.neo4j.core.Neo4jEnvironment;
import spade.query.quickgrail.neo4j.entities.Neo4jGraph;
import spade.query.quickgrail.neo4j.entities.Neo4jGraphMetadata;
import spade.storage.Neo4j;

/**
 * Get the lineage of a set of vertices in a graph.
 */
public class GetLineage
	extends AbstractGetLineage<Neo4jGraph, Neo4jGraphMetadata, Neo4jEnvironment, Neo4j>{

	public GetLineage(Neo4jGraph targetGraph, Neo4jGraph subjectGraph,
					  Neo4jGraph startGraph, Integer depth, Direction direction){
		super(targetGraph, subjectGraph, startGraph, depth, direction);
	}

	@Override
	public void execute(Neo4jEnvironment env, ExecutionContext ctx, Neo4j storage){
		String targetVertexTable = targetGraph.getVertexTableName();
		String targetEdgeTable = targetGraph.getEdgeTableName();
		String subjectVertexTable = subjectGraph.getVertexTableName();
		String subjectEdgeTable = subjectGraph.getEdgeTableName();
		String startGraphName = startGraph.getVertexTableName();

		String cypherQuery = "MATCH p=(" + VERTEX_ALIAS;
		if(direction == Direction.kAncestor || direction == Direction.kBoth)
		{
			cypherQuery += ":" + startGraphName;
		}
		cypherQuery += ")-[*0.." + depth + "]-";
		if(direction != Direction.kBoth)
		{
			cypherQuery += ">";
		}
		cypherQuery += "(n";
		if(direction == Direction.kDescendant)
		{
			cypherQuery += ":" + startGraphName;
		}
		cypherQuery += ")";
		cypherQuery += " WHERE ALL(node IN nodes(p) WHERE node:" + subjectVertexTable + ") ";
		if(!env.IsBaseGraph(subjectGraph))
		{
			cypherQuery += " AND ALL(r IN relationships(p) WHERE r.quickgrail_symbol CONTAINS " +
					formatSymbol(subjectEdgeTable) + ")";
		}
		cypherQuery += " WITH p UNWIND nodes(p) AS node SET node:" + targetVertexTable + " WITH p ";
		cypherQuery += " UNWIND relationships(p) AS " + EDGE_ALIAS + " SET " + EDGE_ALIAS +
				".quickgrail_symbol = CASE WHEN NOT EXISTS(" +
				EDGE_ALIAS + ".quickgrail_symbol) THEN '," + targetEdgeTable + ",'" + " WHEN " +
				EDGE_ALIAS + ".quickgrail_symbol CONTAINS '," + targetEdgeTable + ",' THEN " +
				EDGE_ALIAS + ".quickgrail_symbol " + " ELSE " + EDGE_ALIAS + ".quickgrail_symbol + '," +
				targetEdgeTable + ",' END";

		storage.executeQuery(cypherQuery);
	}
}
