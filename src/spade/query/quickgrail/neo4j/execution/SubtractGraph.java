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

import static spade.query.quickgrail.neo4j.core.CommonVariables.NodeTypes.VERTEX;

import spade.query.quickgrail.core.entities.Graph.GraphComponent;
import spade.query.quickgrail.core.execution.AbstractSubtractGraph;
import spade.query.quickgrail.core.kernel.ExecutionContext;
import spade.query.quickgrail.neo4j.core.Neo4jEnvironment;
import spade.query.quickgrail.neo4j.core.Neo4jStorageHelper;
import spade.query.quickgrail.neo4j.entities.Neo4jGraph;
import spade.query.quickgrail.neo4j.entities.Neo4jGraphMetadata;
import spade.storage.Neo4j;

/**
 * Subtract one graph from the other.
 */
public class SubtractGraph
	extends AbstractSubtractGraph<Neo4jGraph, Neo4jGraphMetadata, Neo4jEnvironment, Neo4j>{
	
	public SubtractGraph(Neo4jGraph outputGraph, Neo4jGraph minuendGraph, Neo4jGraph subtrahendGraph, GraphComponent component){
		super(outputGraph, minuendGraph, subtrahendGraph, component);
	}

	@Override
	public void execute(Neo4jEnvironment env, ExecutionContext ctx, Neo4j storage){
		String outputVertexTable = outputGraph.getVertexTableName();
		String outputEdgeTable = outputGraph.getEdgeTableName();
		String minuendVertexTable = minuendGraph.getVertexTableName();
		String minuendEdgeTable = minuendGraph.getEdgeTableName();
		String subtrahendVertexTable = subtrahendGraph.getVertexTableName();
		String subtrahendEdgeTable = subtrahendGraph.getEdgeTableName();

		String cypherQuery = "";
		if(component == null || component == GraphComponent.kVertex)
		{
			String condition = "x:" + minuendVertexTable +
					" AND NOT x:" + subtrahendVertexTable;
			cypherQuery = Neo4jStorageHelper.vertexLabelQuery(condition, VERTEX.toString(), outputVertexTable);
			// allows execution of multiple MATCH queries in one statement
			cypherQuery += " WITH count(*) as dummy \n";
		}
		if(component == null || component == GraphComponent.kEdge)
		{
			String condition = "";
			if(env.IsBaseGraph(minuendGraph))
			{
				condition += " TRUE ";
			}
			else
			{
				condition += " x.quickgrail_symbol CONTAINS " + Neo4jStorageHelper.formatSymbol(minuendEdgeTable);
			}
			if(env.IsBaseGraph(subtrahendGraph))
			{
				condition += " AND FALSE";
			}
			else
			{
				condition += " AND NOT x.quickgrail_symbol CONTAINS " + Neo4jStorageHelper.formatSymbol(subtrahendEdgeTable);
			}
			cypherQuery += Neo4jStorageHelper.edgeSymbolQuery(condition, outputEdgeTable);
		}
		storage.executeQuery(cypherQuery);
	}
}
