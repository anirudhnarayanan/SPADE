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
package spade.query.quickgrail.quickstep.execution;

import spade.query.quickgrail.core.execution.AbstractLimitGraph;
import spade.query.quickgrail.core.kernel.ExecutionContext;
import spade.query.quickgrail.quickstep.core.QuickstepEnvironment;
import spade.query.quickgrail.quickstep.entities.QuickstepGraph;
import spade.query.quickgrail.quickstep.entities.QuickstepGraphMetadata;
import spade.storage.Quickstep;

/**
 * Sample a subset of vertices / edges from a graph.
 */
public class LimitGraph
	extends AbstractLimitGraph<QuickstepGraph, QuickstepGraphMetadata, QuickstepEnvironment, Quickstep>{

	public LimitGraph(QuickstepGraph targetGraph, QuickstepGraph sourceGraph, int limit){
		super(targetGraph, sourceGraph, limit);
	}

	@Override
	public void execute(QuickstepEnvironment env, ExecutionContext ctx, Quickstep storage){
		String sourceVertexTable = sourceGraph.getVertexTableName();
		String sourceEdgeTable = sourceGraph.getEdgeTableName();

		long numVertices = storage.executeQueryForLongResult(
				"COPY SELECT COUNT(*) FROM " + sourceVertexTable + " TO stdout;");
		long numEdges = storage.executeQueryForLongResult(
				"COPY SELECT COUNT(*) FROM " + sourceEdgeTable + " TO stdout;");

		if(numVertices > 0)
		{
			storage.executeQuery("\\analyzerange " + sourceVertexTable + "\n" +
					"INSERT INTO " + targetGraph.getVertexTableName() +
					" SELECT id FROM " + sourceVertexTable + " GROUP BY id" +
					" ORDER BY id LIMIT " + limit + ";");

		}
		if(numEdges > 0)
		{
			storage.executeQuery("\\analyzerange " + sourceEdgeTable + "\n" +
					"INSERT INTO " + targetGraph.getEdgeTableName() +
					" SELECT id FROM " + sourceEdgeTable + " GROUP BY id" +
					" ORDER BY id LIMIT " + limit + ";");
		}
	}
}
