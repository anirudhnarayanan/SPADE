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

import spade.query.quickgrail.core.execution.AbstractUnionGraph;
import spade.query.quickgrail.core.kernel.ExecutionContext;
import spade.query.quickgrail.neo4j.core.Neo4jEnvironment;
import spade.query.quickgrail.neo4j.core.Neo4jStorageHelper;
import spade.query.quickgrail.neo4j.entities.Neo4jGraph;
import spade.query.quickgrail.neo4j.entities.Neo4jGraphMetadata;
import spade.storage.Neo4j;

/**
 * Union one graph into the other.
 */
public class UnionGraph
	extends AbstractUnionGraph<Neo4jGraph, Neo4jGraphMetadata, Neo4jEnvironment, Neo4j>{
	
	public UnionGraph(Neo4jGraph targetGraph, Neo4jGraph sourceGraph){
		super(targetGraph, sourceGraph);
	}

	@Override
	public void execute(Neo4jEnvironment env, ExecutionContext ctx, Neo4j storage){
		// go into sourceGraph and put labels of targetGraph in all its edges and vertices
		String sourceVertexTable = sourceGraph.getVertexTableName();
		String sourceEdgeTable = sourceGraph.getEdgeTableName();
		String targetVertexTable = targetGraph.getVertexTableName();
		String targetEdgeTable = targetGraph.getEdgeTableName();

		// union vertices
		String condition = " x: " + sourceVertexTable;
		String cypherQuery = Neo4jStorageHelper.vertexLabelQuery(condition, VERTEX.toString(), targetVertexTable);

		// allows execution of multiple queries in one statement
		cypherQuery += " WITH count(*) as dummy \n";

		cypherQuery += "MATCH (child:" + sourceVertexTable + ")-[" + EDGE_ALIAS + ":" + EDGE.toString() + "]->(parent:" +
				sourceVertexTable + ") SET child:" + targetVertexTable + " SET parent:" + targetVertexTable;

		if(!env.IsBaseGraph(sourceGraph))
		{
			condition = " x.quickgrail_symbol CONTAINS " + formatSymbol(sourceEdgeTable);
		}
		String addSymbol = " SET a.quickgrail_symbol = CASE WHEN NOT EXISTS(a.quickgrail_symbol) THEN " +
				formatSymbol(targetEdgeTable) + " WHEN a.quickgrail_symbol CONTAINS " +
				formatSymbol(targetEdgeTable) + " THEN a.quickgrail_symbol ELSE a.quickgrail_symbol + " +
				formatSymbol(targetEdgeTable) + " END";
		String removeSymbol = "SET d.quickgrail_symbol = " +
				"replace(d.quickgrail_symbol, " + formatSymbol(targetEdgeTable) + ", '')";

		// add edge label
		cypherQuery += " WITH REDUCE(s = {a:[], d:[]}, x IN COLLECT(" + EDGE_ALIAS + ") | " +
				" CASE  WHEN " + condition + " THEN {a: s.a+x, d: s.d} " +
				" WHEN x.quickgrail_symbol CONTAINS " + formatSymbol(targetEdgeTable) + " THEN {a: s.a, d: s.d+x} " +
				" ELSE {a:s.a, d:s.d} END) AS actions " +
				" FOREACH (d IN actions.d | " + removeSymbol + ")" +
				" FOREACH(a IN actions.a | " + addSymbol + ")";

		storage.executeQuery(cypherQuery);
	}
}
