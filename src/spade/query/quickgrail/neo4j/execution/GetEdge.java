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
package spade.query.quickgrail.neo4j.execution;

import static spade.query.quickgrail.neo4j.core.Neo4jStorageHelper.formatString;
import static spade.query.quickgrail.neo4j.core.Neo4jStorageHelper.formatSymbol;

import spade.query.quickgrail.core.execution.AbstractGetEdge;
import spade.query.quickgrail.core.kernel.ExecutionContext;
import spade.query.quickgrail.neo4j.core.Neo4jEnvironment;
import spade.query.quickgrail.neo4j.core.Neo4jStorageHelper;
import spade.query.quickgrail.neo4j.entities.Neo4jGraph;
import spade.query.quickgrail.neo4j.entities.Neo4jGraphMetadata;
import spade.storage.Neo4j;

/**
 * Get the a set of edges in a graph.
 */
public class GetEdge 
	extends AbstractGetEdge<Neo4jGraph, Neo4jGraphMetadata, Neo4jEnvironment, Neo4j>{

	public GetEdge(Neo4jGraph targetGraph, Neo4jGraph subjectGraph, String field, String operation, String value){
		super(targetGraph, subjectGraph, field, operation, value);
	}

	@Override
	public void execute(Neo4jEnvironment env, ExecutionContext ctx, Neo4j storage){
		String subjectEdgeTable = subjectGraph.getEdgeTableName();
		String targetEdgeTable = targetGraph.getEdgeTableName();

		String condition = "";
		if(!env.IsBaseGraph(subjectGraph))
		{
			condition += " x.quickgrail_symbol CONTAINS " + formatSymbol(subjectEdgeTable);
			if(field != null)
			{
				// TODO: handle wild card columns
				if(!field.equals("*"))
				{
					condition += " AND x." + field + operation + formatString(value);
				}
			}
		}
		else
		{
			if(field != null)
			{
				if(!field.equals("*"))
				{
					condition += " x." + field + operation + formatString(value);
				}

			}
			else
			{
				condition = " TRUE ";
			}
		}
		String cypherQuery = Neo4jStorageHelper.edgeSymbolQuery(condition, targetEdgeTable);
		storage.executeQuery(cypherQuery);
	}
}
