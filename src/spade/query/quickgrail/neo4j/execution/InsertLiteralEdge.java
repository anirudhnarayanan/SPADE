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

import java.util.ArrayList;

import spade.query.quickgrail.core.execution.AbstractInsertLiteralEdge;
import spade.query.quickgrail.core.kernel.ExecutionContext;
import spade.query.quickgrail.neo4j.core.Neo4jEnvironment;
import spade.query.quickgrail.neo4j.entities.Neo4jGraph;
import spade.query.quickgrail.neo4j.entities.Neo4jGraphMetadata;
import spade.storage.Neo4j;

/**
 * NOT SUPPORTED BY NEO4J
 * TODO: Support execution of general cypher query
 * Insert a list of edges into a graph by hash.
 */
public class InsertLiteralEdge
	extends AbstractInsertLiteralEdge<Neo4jGraph, Neo4jGraphMetadata, Neo4jEnvironment, Neo4j>{

	public InsertLiteralEdge(Neo4jGraph targetGraph, ArrayList<String> edges){
		super(targetGraph, edges);
	}

	@Override
	public void execute(Neo4jEnvironment env, ExecutionContext ctx, Neo4j storage){}
}
