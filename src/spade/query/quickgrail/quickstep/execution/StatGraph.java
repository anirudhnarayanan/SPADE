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

import spade.query.quickgrail.core.execution.AbstractStatGraph;
import spade.query.quickgrail.core.kernel.ExecutionContext;
import spade.query.quickgrail.quickstep.core.QuickstepEnvironment;
import spade.query.quickgrail.quickstep.entities.QuickstepGraph;
import spade.query.quickgrail.quickstep.entities.QuickstepGraphMetadata;
import spade.storage.Quickstep;

/**
 * Show statistics of a graph.
 */
public class StatGraph
	extends AbstractStatGraph<QuickstepGraph, QuickstepGraphMetadata, QuickstepEnvironment, Quickstep>{
	
	public StatGraph(QuickstepGraph targetGraph){
		super(targetGraph);
	}

	@Override
	public void execute(QuickstepEnvironment env, ExecutionContext ctx, Quickstep storage){
		String targetVertexTable = targetGraph.getVertexTableName();
		String targetEdgeTable = targetGraph.getEdgeTableName();
		long numVertices = storage.executeQueryForLongResult(
				"COPY SELECT COUNT(*) FROM " + targetVertexTable + " TO stdout;");
		long numEdges = storage.executeQueryForLongResult(
				"COPY SELECT COUNT(*) FROM " + targetEdgeTable + " TO stdout;");

		String stat = "# vertices = " + numVertices + ", # edges = " + numEdges;
		ctx.addResponse(stat);
	}
}
