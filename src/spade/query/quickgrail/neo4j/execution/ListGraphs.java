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

import java.util.Map;

import spade.query.quickgrail.core.execution.AbstractListGraphs;
import spade.query.quickgrail.core.kernel.ExecutionContext;
import spade.query.quickgrail.core.types.LongType;
import spade.query.quickgrail.core.types.StringType;
import spade.query.quickgrail.core.utility.ResultTable;
import spade.query.quickgrail.core.utility.Schema;
import spade.query.quickgrail.neo4j.core.Neo4jEnvironment;
import spade.query.quickgrail.neo4j.core.Neo4jStorageHelper;
import spade.query.quickgrail.neo4j.entities.Neo4jGraph;
import spade.query.quickgrail.neo4j.entities.Neo4jGraphMetadata;
import spade.storage.Neo4j;

/**
 * List all existing graphs in Neo4j storage.
 */
public class ListGraphs 
	extends AbstractListGraphs<Neo4jGraph, Neo4jGraphMetadata, Neo4jEnvironment, Neo4j>{

	public ListGraphs(String style){
		super(style);
	}

	@Override
	public void execute(Neo4jEnvironment env, ExecutionContext ctx, Neo4j storage){
		ResultTable table = new ResultTable();

		Map<String, String> symbols = env.getSymbols();
		for(String symbol : symbols.keySet())
		{
			if(symbol.startsWith("$"))
			{
				addSymbol(storage, symbol, table);
			}
		}
		addSymbol(storage, "$base", table);

		Schema schema = new Schema();
		schema.addColumn("Graph Name", StringType.GetInstance());
		if(!style.equals("name"))
		{
			schema.addColumn("Number of Vertices", LongType.GetInstance());
			schema.addColumn("Number of Edges", LongType.GetInstance());
		}
		table.setSchema(schema);

		ctx.addResponse(table.toString());
	}

	private void addSymbol(Neo4j storage, String symbol, ResultTable table)
	{
		ResultTable.Row row = new ResultTable.Row();
		row.add(symbol);
		if(!style.equals("name"))
		{
			row.add(Neo4jStorageHelper.GetNumVertices(storage, symbol));
			row.add(Neo4jStorageHelper.GetNumEdges(storage, symbol));
		}
		table.addRow(row);
	}
}
