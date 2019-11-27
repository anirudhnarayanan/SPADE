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

import spade.query.quickgrail.core.execution.AbstractIntersectGraph;
import spade.query.quickgrail.core.kernel.ExecutionContext;
import spade.query.quickgrail.quickstep.core.QuickstepEnvironment;
import spade.query.quickgrail.quickstep.entities.QuickstepGraph;
import spade.query.quickgrail.quickstep.entities.QuickstepGraphMetadata;
import spade.storage.Quickstep;

/**
 * Intersect two graphs (i.e. find common vertices and edges).
 */

public class IntersectGraph
	extends AbstractIntersectGraph<QuickstepGraph, QuickstepGraphMetadata, QuickstepEnvironment, Quickstep>{

	public IntersectGraph(QuickstepGraph outputGraph, QuickstepGraph lhsGraph, QuickstepGraph rhsGraph){
		super(outputGraph, lhsGraph, rhsGraph);
	}

	@Override
	public void execute(QuickstepEnvironment env, ExecutionContext ctx, Quickstep storage){
		String outputVertexTable = outputGraph.getVertexTableName();
		String outputEdgeTable = outputGraph.getEdgeTableName();
		String lhsVertexTable = lhsGraph.getVertexTableName();
		String lhsEdgeTable = lhsGraph.getEdgeTableName();
		String rhsVertexTable = rhsGraph.getVertexTableName();
		String rhsEdgeTable = rhsGraph.getEdgeTableName();

		storage.executeQuery("\\analyzerange " + rhsVertexTable + " " + rhsEdgeTable + "\n");
		storage.executeQuery("INSERT INTO " + outputVertexTable +
				" SELECT id FROM " + lhsVertexTable +
				" WHERE id IN (SELECT id FROM " + rhsVertexTable + ");");
		storage.executeQuery("INSERT INTO " + outputEdgeTable +
				" SELECT id FROM " + lhsEdgeTable +
				" WHERE id IN (SELECT id FROM " + rhsEdgeTable + ");");
	}

}
