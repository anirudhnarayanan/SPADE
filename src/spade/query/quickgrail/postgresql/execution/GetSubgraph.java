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
package spade.query.quickgrail.postgresql.execution;

import static spade.query.quickgrail.postgresql.core.CommonVariables.CHILD_VERTEX_KEY;
import static spade.query.quickgrail.postgresql.core.CommonVariables.EDGE_TABLE;
import static spade.query.quickgrail.postgresql.core.CommonVariables.PARENT_VERTEX_KEY;
import static spade.query.quickgrail.postgresql.core.CommonVariables.PRIMARY_KEY;

import spade.query.quickgrail.core.execution.AbstractGetSubgraph;
import spade.query.quickgrail.core.kernel.ExecutionContext;
import spade.query.quickgrail.postgresql.core.PostgreSQLEnvironment;
import spade.query.quickgrail.postgresql.entities.PostgreSQLGraph;
import spade.query.quickgrail.postgresql.entities.PostgreSQLGraphMetadata;
import spade.storage.PostgreSQL;

/**
 * Let $S be the subject graph and $T be the skeleton graph.
 * The operation $S.getSubgraph($T) is to find all the vertices and edges that
 * are spanned by the skeleton graph.
 */
public class GetSubgraph
	extends AbstractGetSubgraph<PostgreSQLGraph, PostgreSQLGraphMetadata, PostgreSQLEnvironment, PostgreSQL>{
	
	public GetSubgraph(PostgreSQLGraph targetGraph, PostgreSQLGraph subjectGraph, PostgreSQLGraph skeletonGraph){
		super(targetGraph, subjectGraph, skeletonGraph);
	}

	@Override
	public void execute(PostgreSQLEnvironment env, ExecutionContext ctx, PostgreSQL storage){
		String targetVertexTable = targetGraph.getVertexTableName();
		String targetEdgeTable = targetGraph.getEdgeTableName();
		String subjectVertexTable = subjectGraph.getVertexTableName();
		String subjectEdgeTable = subjectGraph.getEdgeTableName();
		String skeletonVertexTable = skeletonGraph.getVertexTableName();
		String skeletonEdgeTable = skeletonGraph.getEdgeTableName();

		storage.executeQuery("DROP TABLE IF EXISTS m_answer;" + "CREATE TABLE m_answer (" + PRIMARY_KEY + " UUID);");

		// Get vertices.
		storage.executeQuery("INSERT INTO m_answer SELECT " + PRIMARY_KEY + " FROM " + skeletonVertexTable +
				" WHERE " + PRIMARY_KEY + " IN (SELECT " + PRIMARY_KEY + " FROM " + subjectVertexTable + ");" +
				"INSERT INTO m_answer SELECT \"" + CHILD_VERTEX_KEY + "\" FROM " + EDGE_TABLE +
				" WHERE " + PRIMARY_KEY + " IN (SELECT " + PRIMARY_KEY + " FROM " + skeletonEdgeTable + ")" +
				" AND \"" + CHILD_VERTEX_KEY + "\" IN (SELECT " + PRIMARY_KEY + " FROM " + subjectVertexTable + ");" +
				"INSERT INTO m_answer SELECT \"" + PARENT_VERTEX_KEY + "\" FROM " + EDGE_TABLE +
				" WHERE " + PRIMARY_KEY + " IN (SELECT " + PRIMARY_KEY + " FROM " + skeletonEdgeTable + ")" +
				" AND \"" + PARENT_VERTEX_KEY + "\" IN (SELECT " + PRIMARY_KEY + " FROM " + subjectVertexTable + ");" +
				"INSERT INTO " + targetVertexTable + " SELECT " + PRIMARY_KEY + " FROM m_answer GROUP BY " +
				PRIMARY_KEY + " ;");

		// Get edges.
		storage.executeQuery("INSERT INTO " + targetEdgeTable +
				" SELECT s." + PRIMARY_KEY + " FROM " + subjectEdgeTable + " s, " + EDGE_TABLE + " e" +
				" WHERE s." + PRIMARY_KEY + " = e." + PRIMARY_KEY + " AND e.\"" + CHILD_VERTEX_KEY + "\" IN (SELECT " + PRIMARY_KEY +
				" FROM m_answer)" +
				" AND e.\"" + PARENT_VERTEX_KEY + "\" IN (SELECT " + PRIMARY_KEY + " FROM m_answer) GROUP BY s." + PRIMARY_KEY + " ;");

		storage.executeQuery("DROP TABLE IF EXISTS m_answer;");
	}
}
