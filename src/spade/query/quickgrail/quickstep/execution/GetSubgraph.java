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

import spade.query.quickgrail.core.execution.AbstractGetSubgraph;
import spade.query.quickgrail.core.kernel.ExecutionContext;
import spade.query.quickgrail.quickstep.core.QuickstepEnvironment;
import spade.query.quickgrail.quickstep.entities.QuickstepGraph;
import spade.query.quickgrail.quickstep.entities.QuickstepGraphMetadata;
import spade.storage.Quickstep;

/**
 * Let $S be the subject graph and $T be the skeleton graph.
 * The operation $S.getSubgraph($T) is to find all the vertices and edges that
 * are spanned by the skeleton graph.
 */
public class GetSubgraph
	extends AbstractGetSubgraph<QuickstepGraph, QuickstepGraphMetadata, QuickstepEnvironment, Quickstep>{

	public GetSubgraph(QuickstepGraph targetGraph, QuickstepGraph subjectGraph, QuickstepGraph skeletonGraph){
		super(targetGraph, subjectGraph, skeletonGraph);
	}

	@Override
	public void execute(QuickstepEnvironment env, ExecutionContext ctx, Quickstep storage){
		String targetVertexTable = targetGraph.getVertexTableName();
		String targetEdgeTable = targetGraph.getEdgeTableName();
		String subjectVertexTable = subjectGraph.getVertexTableName();
		String subjectEdgeTable = subjectGraph.getEdgeTableName();
		String skeletonVertexTable = skeletonGraph.getVertexTableName();
		String skeletonEdgeTable = skeletonGraph.getEdgeTableName();

		storage.executeQuery("DROP TABLE m_answer;\n" + "CREATE TABLE m_answer (id INT);");

		// Get vertices.
		storage.executeQuery("\\analyzerange " + subjectVertexTable + "\n" +
				"INSERT INTO m_answer SELECT id FROM " + skeletonVertexTable +
				" WHERE id IN (SELECT id FROM " + subjectVertexTable + ");\n" +
				"INSERT INTO m_answer SELECT src FROM edge " +
				" WHERE id IN (SELECT id FROM " + skeletonEdgeTable + ")" +
				" AND src IN (SELECT id FROM " + subjectVertexTable + ");\n" +
				"INSERT INTO m_answer SELECT dst FROM edge" +
				" WHERE id IN (SELECT id FROM " + skeletonEdgeTable + ")" +
				" AND dst IN (SELECT id FROM " + subjectVertexTable + ");\n" +
				"\\analyzerange m_answer\n" +
				"INSERT INTO " + targetVertexTable + " SELECT id FROM m_answer GROUP BY id;\n");

		// Get edges.
		storage.executeQuery("\\analyzerange " + subjectEdgeTable + "\n" +
				"INSERT INTO " + targetEdgeTable +
				" SELECT s.id FROM " + subjectEdgeTable + " s, edge e" +
				" WHERE s.id = e.id AND e.src IN (SELECT id FROM m_answer)" +
				" AND e.dst IN (SELECT id FROM m_answer) GROUP BY s.id;");

		storage.executeQuery("DROP TABLE m_answer;");
	}
}
