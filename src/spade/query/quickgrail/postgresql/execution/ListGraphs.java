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

import java.util.Map;
import java.util.Map.Entry;

import spade.query.quickgrail.core.execution.AbstractListGraphs;
import spade.query.quickgrail.core.kernel.ExecutionContext;
import spade.query.quickgrail.core.types.LongType;
import spade.query.quickgrail.core.types.StringType;
import spade.query.quickgrail.core.utility.ResultTable;
import spade.query.quickgrail.core.utility.Schema;
import spade.query.quickgrail.postgresql.core.PostgreSQLEnvironment;
import spade.query.quickgrail.postgresql.core.PostgresUtil;
import spade.query.quickgrail.postgresql.entities.PostgreSQLGraph;
import spade.query.quickgrail.postgresql.entities.PostgreSQLGraphMetadata;
import spade.storage.PostgreSQL;

/**
 * List all existing graphs in Postgres storage.
 */
public class ListGraphs
	extends AbstractListGraphs<PostgreSQLGraph, PostgreSQLGraphMetadata, PostgreSQLEnvironment, PostgreSQL>{
	
	public ListGraphs(String style){
		super(style);
	}

	@Override
	public void execute(PostgreSQLEnvironment env, ExecutionContext ctx, PostgreSQL storage){
		ResultTable table = new ResultTable();

		Map<String, String> symbols = env.getSymbols();
		for(Entry<String, String> entry : symbols.entrySet())
		{
			String symbol = entry.getKey();
			if(symbol.startsWith("$"))
			{
				addSymbol(storage, symbol, new PostgreSQLGraph(entry.getValue()), table);
			}
		}
		addSymbol(storage, "$base", PostgreSQLEnvironment.kBaseGraph, table);

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

	private void addSymbol(PostgreSQL storage, String symbol,
						   PostgreSQLGraph graph, ResultTable table)
	{
		ResultTable.Row row = new ResultTable.Row();
		row.add(symbol);
		if(!style.equals("name"))
		{
			row.add(PostgresUtil.GetNumVertices(storage, graph));
			row.add(PostgresUtil.GetNumEdges(storage, graph));
		}
		table.addRow(row);
	}
}
