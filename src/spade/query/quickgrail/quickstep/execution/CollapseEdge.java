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

import java.util.ArrayList;

import spade.query.quickgrail.core.execution.AbstractCollapseEdge;
import spade.query.quickgrail.core.kernel.ExecutionContext;
import spade.query.quickgrail.quickstep.core.QuickstepEnvironment;
import spade.query.quickgrail.quickstep.entities.QuickstepGraph;
import spade.query.quickgrail.quickstep.entities.QuickstepGraphMetadata;
import spade.storage.Quickstep;

/**
 * Collapse all edges whose specified fields are the same.
 */
public class CollapseEdge
	extends AbstractCollapseEdge<QuickstepGraph, QuickstepGraphMetadata, QuickstepEnvironment, Quickstep>{

	public CollapseEdge(QuickstepGraph targetGraph, QuickstepGraph sourceGraph, ArrayList<String> fields){
		super(targetGraph, sourceGraph, fields);
	}

	@Override
	public void execute(QuickstepEnvironment env, ExecutionContext ctx, Quickstep storage){
		String sourceVertexTable = sourceGraph.getVertexTableName();
		String sourceEdgeTable = sourceGraph.getEdgeTableName();
		String targetVertexTable = targetGraph.getVertexTableName();
		String targetEdgeTable = targetGraph.getEdgeTableName();

		storage.executeQuery("\\analyzerange " + sourceVertexTable + " " + sourceEdgeTable + "\n");
		storage.executeQuery("INSERT INTO " + targetVertexTable +
				" SELECT id FROM " + sourceVertexTable + ";");

		StringBuilder tables = new StringBuilder();
		StringBuilder predicates = new StringBuilder();
		StringBuilder groups = new StringBuilder();

		for(int i = 0; i < getFields().size(); ++i)
		{
			String edgeAnnoName = "ea" + i;
			tables.append(", edge_anno " + edgeAnnoName);
			predicates.append(" AND e.id = " + edgeAnnoName + ".id" +
					" AND " + edgeAnnoName + ".field = '" + getFields().get(i) + "'");
			groups.append(", " + edgeAnnoName + ".value");
		}

		storage.executeQuery("INSERT INTO " + targetEdgeTable +
				" SELECT MIN(e.id) FROM edge e" + tables.toString() +
				" WHERE e.id IN (SELECT id FROM " + sourceEdgeTable + ")" + predicates.toString() +
				" GROUP BY src, dst" + groups.toString() + ";");
	}
}
