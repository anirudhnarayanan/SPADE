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

import spade.query.quickgrail.core.entities.Graph.GraphComponent;
import spade.query.quickgrail.core.execution.AbstractSubtractGraph;
import spade.query.quickgrail.core.kernel.ExecutionContext;
import spade.query.quickgrail.quickstep.core.QuickstepEnvironment;
import spade.query.quickgrail.quickstep.entities.QuickstepGraph;
import spade.query.quickgrail.quickstep.entities.QuickstepGraphMetadata;
import spade.storage.Quickstep;

/**
 * Subtract one graph from the other.
 */
public class SubtractGraph
	extends AbstractSubtractGraph<QuickstepGraph, QuickstepGraphMetadata, QuickstepEnvironment, Quickstep>{

	public SubtractGraph(QuickstepGraph outputGraph, QuickstepGraph minuendGraph,
						 QuickstepGraph subtrahendGraph, GraphComponent component){
		super(outputGraph, minuendGraph, subtrahendGraph, component);
	}

	@Override
	public void execute(QuickstepEnvironment env, ExecutionContext ctx, Quickstep storage){
		String outputVertexTable = outputGraph.getVertexTableName();
		String outputEdgeTable = outputGraph.getEdgeTableName();
		String minuendVertexTable = minuendGraph.getVertexTableName();
		String minuendEdgeTable = minuendGraph.getEdgeTableName();
		String subtrahendVertexTable = subtrahendGraph.getVertexTableName();
		String subtrahendEdgeTable = subtrahendGraph.getEdgeTableName();

		if(component == null || component == GraphComponent.kVertex)
		{
			storage.executeQuery("\\analyzerange " + subtrahendVertexTable + "\n");
			storage.executeQuery("INSERT INTO " + outputVertexTable +
					" SELECT id FROM " + minuendVertexTable +
					" WHERE id NOT IN (SELECT id FROM " + subtrahendVertexTable + ");");
		}
		if(component == null || component == GraphComponent.kEdge)
		{
			storage.executeQuery("\\analyzerange " + subtrahendEdgeTable + "\n");
			storage.executeQuery("INSERT INTO " + outputEdgeTable +
					" SELECT id FROM " + minuendEdgeTable +
					" WHERE id NOT IN (SELECT id FROM " + subtrahendEdgeTable + ");");
		}
	}
}
