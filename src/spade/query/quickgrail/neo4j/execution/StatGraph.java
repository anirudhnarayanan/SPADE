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

import spade.query.quickgrail.core.execution.AbstractStatGraph;
import spade.query.quickgrail.core.kernel.ExecutionContext;
import spade.query.quickgrail.neo4j.core.Neo4jEnvironment;
import spade.query.quickgrail.neo4j.core.Neo4jStorageHelper;
import spade.query.quickgrail.neo4j.entities.Neo4jGraph;
import spade.query.quickgrail.neo4j.entities.Neo4jGraphMetadata;
import spade.storage.Neo4j;

/**
 * Show statistics of a graph.
 */
public class StatGraph
	extends AbstractStatGraph<Neo4jGraph, Neo4jGraphMetadata, Neo4jEnvironment, Neo4j>{

	public StatGraph(Neo4jGraph targetGraph){
		super(targetGraph);
	}

	@Override
	public void execute(Neo4jEnvironment env, ExecutionContext ctx, Neo4j storage){
		String tableName = targetGraph.getName();
		long numVertices = Neo4jStorageHelper.GetNumVertices(storage, tableName);
		long numEdges = Neo4jStorageHelper.GetNumEdges(storage, tableName);

		String stat = "# vertices = " + numVertices + ", # edges = " + numEdges;
		ctx.addResponse(stat);
	}

}
