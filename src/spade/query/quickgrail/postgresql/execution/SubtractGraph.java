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

import spade.query.quickgrail.core.entities.Graph.GraphComponent;
import spade.query.quickgrail.core.execution.AbstractSubtractGraph;
import spade.query.quickgrail.core.kernel.ExecutionContext;
import spade.query.quickgrail.postgresql.core.PostgreSQLEnvironment;
import spade.query.quickgrail.postgresql.entities.PostgreSQLGraph;
import spade.query.quickgrail.postgresql.entities.PostgreSQLGraphMetadata;
import spade.storage.PostgreSQL;

/**
 * Subtract one graph from the other.
 */
public class SubtractGraph
	extends AbstractSubtractGraph<PostgreSQLGraph, PostgreSQLGraphMetadata, PostgreSQLEnvironment, PostgreSQL>{
	
	public SubtractGraph(PostgreSQLGraph outputGraph, PostgreSQLGraph minuendGraph,
						 PostgreSQLGraph subtrahendGraph, GraphComponent component){
		super(outputGraph, minuendGraph, subtrahendGraph, component);
	}

	@Override
	public void execute(PostgreSQLEnvironment env, ExecutionContext ctx, PostgreSQL storage){
		String outputVertexTable = outputGraph.getVertexTableName();
		String outputEdgeTable = outputGraph.getEdgeTableName();
		String minuendVertexTable = minuendGraph.getVertexTableName();
		String minuendEdgeTable = minuendGraph.getEdgeTableName();
		String subtrahendVertexTable = subtrahendGraph.getVertexTableName();
		String subtrahendEdgeTable = subtrahendGraph.getEdgeTableName();

		if(component == null || component == GraphComponent.kVertex)
		{
			storage.executeQuery("INSERT INTO " + outputVertexTable +
					" SELECT " + PRIMARY_KEY + " FROM " + minuendVertexTable +
					" WHERE " + PRIMARY_KEY + " NOT IN (SELECT " + PRIMARY_KEY +
					" FROM " + subtrahendVertexTable + ");");
		}
		if(component == null || component == GraphComponent.kEdge)
		{
			storage.executeQuery("INSERT INTO " + outputEdgeTable +
					" SELECT " + PRIMARY_KEY + " FROM " + minuendEdgeTable +
					" WHERE " + PRIMARY_KEY + " NOT IN (SELECT " + PRIMARY_KEY +
					" FROM " + subtrahendEdgeTable + ");");
		}
	}
}
