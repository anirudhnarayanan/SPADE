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

import spade.query.quickgrail.core.execution.AbstractGetLink;
import spade.query.quickgrail.core.kernel.ExecutionContext;
import spade.query.quickgrail.quickstep.core.QuickstepEnvironment;
import spade.query.quickgrail.quickstep.entities.QuickstepGraph;
import spade.query.quickgrail.quickstep.entities.QuickstepGraphMetadata;
import spade.storage.Quickstep;

/**
 * Similar to GetPath but treats the graph edges as indirected.
 */
public class GetLink 
	extends AbstractGetLink<QuickstepGraph, QuickstepGraphMetadata, QuickstepEnvironment, Quickstep>{

	public GetLink(QuickstepGraph targetGraph, QuickstepGraph subjectGraph,
				   QuickstepGraph srcGraph, QuickstepGraph dstGraph,
				   Integer maxDepth){
		super(targetGraph, subjectGraph, srcGraph, dstGraph, maxDepth);
	}

	@Override
	public void execute(QuickstepEnvironment env, ExecutionContext ctx, Quickstep storage){
		storage.executeQuery("DROP TABLE m_cur;\n" +
				"DROP TABLE m_next;\n" +
				"DROP TABLE m_answer;\n" +
				"CREATE TABLE m_cur (id INT);\n" +
				"CREATE TABLE m_next (id INT);\n" +
				"CREATE TABLE m_answer (id INT);");

		String filter;
		if(env.IsBaseGraph(subjectGraph))
		{
			filter = "";
		}
		else
		{
			filter = " AND edge.id IN (SELECT id FROM " + subjectGraph.getEdgeTableName() + ")";
		}

		// Create subgraph edges table.
		storage.executeQuery("DROP TABLE m_sgconn;\n" +
				"CREATE TABLE m_sgconn (src INT, dst INT, depth INT);");

		storage.executeQuery("INSERT INTO m_cur SELECT id FROM " + destinationGraph.getVertexTableName() + ";\n" +
				"INSERT INTO m_answer SELECT id FROM m_cur;\n" +
				"\\analyzerange edge\n");

		String loopStmts =
				"\\analyzerange m_cur\n" +
						"INSERT INTO m_sgconn SELECT src, dst, $depth FROM edge" +
						" WHERE dst IN (SELECT id FROM m_cur)" + filter + ";\n" +
						"INSERT INTO m_sgconn SELECT src, dst, $depth FROM edge" +
						" WHERE src IN (SELECT id FROM m_cur)" + filter + ";\n" +
						"DROP TABLE m_next;\n" + "CREATE TABLE m_next (id INT);\n" +
						"INSERT INTO m_next SELECT src FROM edge" +
						" WHERE dst IN (SELECT id FROM m_cur)" + filter + " GROUP BY src;\n" +
						"INSERT INTO m_next SELECT dst FROM edge" +
						" WHERE src IN (SELECT id FROM m_cur)" + filter + " GROUP BY dst;\n" +
						"DROP TABLE m_cur;\n" + "CREATE TABLE m_cur (id INT);\n" +
						"\\analyzerange m_answer\n" +
						"INSERT INTO m_cur SELECT id FROM m_next WHERE id NOT IN (SELECT id FROM m_answer);\n" +
						"INSERT INTO m_answer SELECT id FROM m_cur;";
		for(int i = 0; i < maxDepth; ++i)
		{
			storage.executeQuery(loopStmts.replace("$depth", String.valueOf(i + 1)));

			String worksetSizeQuery = "COPY SELECT COUNT(*) FROM m_cur TO stdout;";
			if(storage.executeQueryForLongResult(worksetSizeQuery) == 0)
			{
				break;
			}
		}

		storage.executeQuery("DROP TABLE m_cur;\n" +
				"DROP TABLE m_next;\n" +
				"CREATE TABLE m_cur (id INT);\n" +
				"CREATE TABLE m_next (id INT);");

		storage.executeQuery("\\analyzerange m_answer\n" +
				"INSERT INTO m_cur SELECT id FROM " + sourceGraph.getVertexTableName() +
				" WHERE id IN (SELECT id FROM m_answer);\n");

		storage.executeQuery("DROP TABLE m_answer;\n" +
				"CREATE TABLE m_answer (id INT);\n" +
				"INSERT INTO m_answer SELECT id FROM m_cur;" +
				"\\analyzerange m_answer m_sgconn\n");

		loopStmts =
				"DROP TABLE m_next;\n" + "CREATE TABLE m_next (id int);\n" +
						"\\analyzerange m_cur\n" +
						"INSERT INTO m_next SELECT dst FROM m_sgconn" +
						" WHERE src IN (SELECT id FROM m_cur)" +
						" AND depth + $depth <= " + String.valueOf(maxDepth) + " GROUP BY dst;\n" +
						"INSERT INTO m_next SELECT src FROM m_sgconn" +
						" WHERE dst IN (SELECT id FROM m_cur)" +
						" AND depth + $depth <= " + String.valueOf(maxDepth) + " GROUP BY src;\n" +
						"DROP TABLE m_cur;\n" + "CREATE TABLE m_cur (id INT);\n" +
						"\\analyzerange m_answer\n" +
						"INSERT INTO m_cur SELECT id FROM m_next WHERE id NOT IN (SELECT id FROM m_answer);\n" +
						"INSERT INTO m_answer SELECT id FROM m_cur;";
		for(int i = 0; i < maxDepth; ++i)
		{
			storage.executeQuery(loopStmts.replace("$depth", String.valueOf(i)));

			String worksetSizeQuery = "COPY SELECT COUNT(*) FROM m_cur TO stdout;";
			if(storage.executeQueryForLongResult(worksetSizeQuery) == 0)
			{
				break;
			}
		}

		String targetVertexTable = targetGraph.getVertexTableName();
		String targetEdgeTable = targetGraph.getEdgeTableName();

		storage.executeQuery("\\analyzerange m_answer\n" +
				"INSERT INTO " + targetVertexTable + " SELECT id FROM m_answer;\n" +
				"INSERT INTO " + targetEdgeTable + " SELECT id FROM edge" +
				" WHERE src IN (SELECT id FROM m_answer)" +
				" AND dst IN (SELECT id FROM m_answer)" + filter + ";");

		storage.executeQuery("DROP TABLE m_cur;\n" +
				"DROP TABLE m_next;\n" +
				"DROP TABLE m_answer;\n" +
				"DROP TABLE m_sgconn;");
	}
}
