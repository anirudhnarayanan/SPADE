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
import static spade.query.quickgrail.neo4j.core.CommonVariables.NodeTypes.VERTEX;
import static spade.query.quickgrail.neo4j.core.CommonVariables.RelationshipTypes.EDGE;
import static spade.query.quickgrail.neo4j.core.Neo4jStorageHelper.formatSymbol;

import spade.query.quickgrail.core.execution.AbstractIntersectGraph;
import spade.query.quickgrail.core.kernel.ExecutionContext;
import spade.query.quickgrail.neo4j.core.Neo4jEnvironment;
import spade.query.quickgrail.neo4j.core.Neo4jStorageHelper;
import spade.query.quickgrail.neo4j.entities.Neo4jGraph;
import spade.query.quickgrail.neo4j.entities.Neo4jGraphMetadata;
import spade.storage.Neo4j;

/**
 * Intersect two graphs (i.e. find common vertices and edges).
 */

public class IntersectGraph
	extends AbstractIntersectGraph<Neo4jGraph, Neo4jGraphMetadata, Neo4jEnvironment, Neo4j>{

	public IntersectGraph(Neo4jGraph outputGraph, Neo4jGraph lhsGraph, Neo4jGraph rhsGraph){
		super(outputGraph, lhsGraph, rhsGraph);
	}

	@Override
	public void execute(Neo4jEnvironment env, ExecutionContext ctx, Neo4j storage){
		String outputVertexTable = outputGraph.getVertexTableName();
		String outputEdgeTable = outputGraph.getEdgeTableName();
		String lhsVertexTable = lhsGraph.getVertexTableName();
		String lhsEdgeTable = lhsGraph.getEdgeTableName();
		String rhsVertexTable = rhsGraph.getVertexTableName();
		String rhsEdgeTable = rhsGraph.getEdgeTableName();

		String condition = "x:" + lhsVertexTable + ":" + rhsVertexTable;
		String cypherQuery = Neo4jStorageHelper.vertexLabelQuery(condition, VERTEX.toString(), outputVertexTable);

		// allows execution of multiple queries in one statement
		cypherQuery += " WITH count(*) as dummy \n";

		cypherQuery += "MATCH ()-[" + EDGE_ALIAS + ":" + EDGE.toString() + "]->() ";
		condition = "";
		boolean isBase = false;
		if(!env.IsBaseGraph(lhsGraph))
		{
			condition += " x.quickgrail_symbol CONTAINS " + formatSymbol(lhsEdgeTable);
			isBase = true;
		}
		if(!env.IsBaseGraph(rhsGraph))
		{
			if(isBase)
			{
				condition += " AND ";
			}
			condition += " x.quickgrail_symbol CONTAINS " + formatSymbol(rhsEdgeTable);
		}
		cypherQuery += Neo4jStorageHelper.edgeSymbolQuery(condition, outputEdgeTable);

		storage.executeQuery(cypherQuery);
	}

}
