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

import spade.query.quickgrail.core.entities.Graph.Direction;
import spade.query.quickgrail.core.execution.AbstractGetLineage;
import spade.query.quickgrail.core.kernel.ExecutionContext;
import spade.query.quickgrail.quickstep.core.QuickstepEnvironment;
import spade.query.quickgrail.quickstep.entities.QuickstepGraph;
import spade.query.quickgrail.quickstep.entities.QuickstepGraphMetadata;
import spade.storage.Quickstep;

/**
 * Get the lineage of a set of vertices in a graph.
 */
public class GetLineage
	extends AbstractGetLineage<QuickstepGraph, QuickstepGraphMetadata, QuickstepEnvironment, Quickstep>{

	public GetLineage(QuickstepGraph targetGraph, QuickstepGraph subjectGraph,
					  QuickstepGraph startGraph, Integer depth, Direction direction){
		super(targetGraph, subjectGraph, startGraph, depth, direction);
	}

	@Override
	public void execute(QuickstepEnvironment env, ExecutionContext ctx, Quickstep storage){
		ArrayList<Direction> oneDirs = new ArrayList<>();
		if(direction == Direction.kBoth)
		{
			oneDirs.add(Direction.kAncestor);
			oneDirs.add(Direction.kDescendant);
		}
		else
		{
			oneDirs.add(direction);
		}

		String targetVertexTable = targetGraph.getVertexTableName();
		String targetEdgeTable = targetGraph.getEdgeTableName();

		String subjectEdgeTable = subjectGraph.getEdgeTableName();
		String filter = "";
		if(!env.IsBaseGraph(subjectGraph))
		{
			storage.executeQuery("\\analyzerange " + subjectEdgeTable + "\n");
			filter = " AND edge.id IN (SELECT id FROM " + subjectEdgeTable + ")";
		}

		for(Direction oneDir : oneDirs)
		{
			executeOneDirection(oneDir, storage, filter);
			storage.executeQuery("\\analyzerange m_answer m_answer_edge\n" +
					"INSERT INTO " + targetVertexTable + " SELECT id FROM m_answer;\n" +
					"INSERT INTO " + targetEdgeTable + " SELECT id FROM m_answer_edge GROUP BY id;");
		}

		storage.executeQuery("DROP TABLE m_cur;\n" +
				"DROP TABLE m_next;\n" +
				"DROP TABLE m_answer;\n" +
				"DROP TABLE m_answer_edge;");
	}

	private void executeOneDirection(Direction dir, Quickstep storage, String filter)
	{
		String src, dst;
		if(dir == Direction.kAncestor)
		{
			src = "src";
			dst = "dst";
		}
		else
		{
			assert dir == Direction.kDescendant;
			src = "dst";
			dst = "src";
		}

		storage.executeQuery("DROP TABLE m_cur;\n" +
				"DROP TABLE m_next;\n" +
				"DROP TABLE m_answer;\n" +
				"DROP TABLE m_answer_edge;\n" +
				"CREATE TABLE m_cur (id INT);\n" +
				"CREATE TABLE m_next (id INT);\n" +
				"CREATE TABLE m_answer (id INT);\n" +
				"CREATE TABLE m_answer_edge (id LONG);");

		String startVertexTable = startGraph.getVertexTableName();
		storage.executeQuery("INSERT INTO m_cur SELECT id FROM " + startVertexTable + ";\n" +
				"INSERT INTO m_answer SELECT id FROM m_cur;");

		String loopStmts =
				"DROP TABLE m_next;\n" + "CREATE TABLE m_next (id INT);\n" +
						"\\analyzerange m_cur\n" +
						"INSERT INTO m_next SELECT " + dst + " FROM edge" +
						" WHERE " + src + " IN (SELECT id FROM m_cur)" + filter +
						" GROUP BY " + dst + ";\n" +
						"INSERT INTO m_answer_edge SELECT id FROM edge" +
						" WHERE " + src + " IN (SELECT id FROM m_cur)" + filter + ";\n" +
						"DROP TABLE m_cur;\n" + "CREATE TABLE m_cur (id INT);\n" +
						"\\analyzerange m_answer\n" +
						"INSERT INTO m_cur SELECT id FROM m_next WHERE id NOT IN (SELECT id FROM m_answer);\n" +
						"INSERT INTO m_answer SELECT id FROM m_cur;";
		for(int i = 0; i < depth; ++i)
		{
			storage.executeQuery(loopStmts);

			String worksetSizeQuery = "COPY SELECT COUNT(*) FROM m_cur TO stdout;";
			if(storage.executeQueryForLongResult(worksetSizeQuery) == 0)
			{
				break;
			}
		}
	}
}
