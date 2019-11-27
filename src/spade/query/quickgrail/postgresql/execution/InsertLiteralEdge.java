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

import java.util.ArrayList;

import spade.query.quickgrail.core.execution.AbstractInsertLiteralEdge;
import spade.query.quickgrail.core.kernel.ExecutionContext;
import spade.query.quickgrail.postgresql.core.PostgreSQLEnvironment;
import spade.query.quickgrail.postgresql.entities.PostgreSQLGraph;
import spade.query.quickgrail.postgresql.entities.PostgreSQLGraphMetadata;
import spade.storage.PostgreSQL;

/**
 * Insert a list of edges into a graph by hash.
 */
public class InsertLiteralEdge
	extends AbstractInsertLiteralEdge<PostgreSQLGraph, PostgreSQLGraphMetadata, PostgreSQLEnvironment, PostgreSQL>{

	public InsertLiteralEdge(PostgreSQLGraph targetGraph, ArrayList<String> edges){
		super(targetGraph, edges);
	}

	@Override
	public void execute(PostgreSQLEnvironment env, ExecutionContext ctx, PostgreSQL storage){
		String prefix = "INSERT INTO " + targetGraph.getEdgeTableName() + " VALUES(";
		StringBuilder sqlQuery = new StringBuilder();
		for(String edge : getEdges())
		{
			sqlQuery.append(prefix + edge + ");");
		}
		storage.executeQuery(sqlQuery.toString());
	}
}
