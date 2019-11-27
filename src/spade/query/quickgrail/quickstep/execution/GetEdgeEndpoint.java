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

import spade.query.quickgrail.core.entities.Graph.EdgeComponent;
import spade.query.quickgrail.core.execution.AbstractGetEdgeEndpoint;
import spade.query.quickgrail.core.kernel.ExecutionContext;
import spade.query.quickgrail.quickstep.core.QuickstepEnvironment;
import spade.query.quickgrail.quickstep.entities.QuickstepGraph;
import spade.query.quickgrail.quickstep.entities.QuickstepGraphMetadata;
import spade.storage.Quickstep;

/**
 * Get end points of all edges in a graph.
 */
public class GetEdgeEndpoint
	extends AbstractGetEdgeEndpoint<QuickstepGraph, QuickstepGraphMetadata, QuickstepEnvironment, Quickstep>{
	
	public GetEdgeEndpoint(QuickstepGraph targetGraph, QuickstepGraph subjectGraph, EdgeComponent component){
		super(targetGraph, subjectGraph, component);
	}

	@Override
	public void execute(QuickstepEnvironment env, ExecutionContext ctx, Quickstep storage){
		String targetVertexTable = targetGraph.getVertexTableName();
		String subjectEdgeTable = subjectGraph.getEdgeTableName();

		storage.executeQuery("DROP TABLE m_answer;\n" +
				"CREATE TABLE m_answer (id INT);\n" +
				"\\analyzerange " + subjectEdgeTable + "\n");

		if(component == EdgeComponent.kSource || component == EdgeComponent.kBoth)
		{
			storage.executeQuery("INSERT INTO m_answer SELECT src FROM edge" +
					" WHERE id IN (SELECT id FROM " + subjectEdgeTable + ");");
		}

		if(component == EdgeComponent.kDestination || component == EdgeComponent.kBoth)
		{
			storage.executeQuery("INSERT INTO m_answer SELECT dst FROM edge" +
					" WHERE id IN (SELECT id FROM " + subjectEdgeTable + ");");
		}

		storage.executeQuery("\\analyzerange m_answer\n" +
				"INSERT INTO " + targetVertexTable + " SELECT id FROM m_answer GROUP BY id;\n" +
				"DROP TABLE m_answer;");
	}
}
