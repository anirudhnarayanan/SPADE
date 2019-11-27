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

import static spade.query.quickgrail.postgresql.core.CommonVariables.PRIMARY_KEY;

import spade.query.quickgrail.core.execution.AbstractIntersectGraph;
import spade.query.quickgrail.core.kernel.ExecutionContext;
import spade.query.quickgrail.postgresql.core.PostgreSQLEnvironment;
import spade.query.quickgrail.postgresql.entities.PostgreSQLGraph;
import spade.query.quickgrail.postgresql.entities.PostgreSQLGraphMetadata;
import spade.storage.PostgreSQL;

/**
 * Intersect two graphs (i.e. find common vertices and edges).
 */

public class IntersectGraph
	extends AbstractIntersectGraph<PostgreSQLGraph, PostgreSQLGraphMetadata, PostgreSQLEnvironment, PostgreSQL>{
	
	public IntersectGraph(PostgreSQLGraph outputGraph, PostgreSQLGraph lhsGraph, PostgreSQLGraph rhsGraph){
		super(outputGraph, lhsGraph, rhsGraph);
	}

	@Override
	public void execute(PostgreSQLEnvironment env, ExecutionContext ctx, PostgreSQL storage){
		String outputVertexTable = outputGraph.getVertexTableName();
		String outputEdgeTable = outputGraph.getEdgeTableName();
		String lhsVertexTable = lhsGraph.getVertexTableName();
		String lhsEdgeTable = lhsGraph.getEdgeTableName();
		String rhsVertexTable = rhsGraph.getVertexTableName();
		String rhsEdgeTable = rhsGraph.getEdgeTableName();

		storage.executeQuery("INSERT INTO " + outputVertexTable +
				" SELECT " + PRIMARY_KEY + " FROM " + lhsVertexTable +
				" WHERE " + PRIMARY_KEY + " IN (SELECT " + PRIMARY_KEY + " FROM " + rhsVertexTable + ");");
		storage.executeQuery("INSERT INTO " + outputEdgeTable +
				" SELECT " + PRIMARY_KEY + " FROM " + lhsEdgeTable +
				" WHERE " + PRIMARY_KEY + " IN (SELECT " + PRIMARY_KEY + " FROM " + rhsEdgeTable + ");");
	}
}
