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

import java.util.Map;
import java.util.Map.Entry;

import spade.query.quickgrail.core.execution.AbstractListGraphs;
import spade.query.quickgrail.core.kernel.ExecutionContext;
import spade.query.quickgrail.core.types.LongType;
import spade.query.quickgrail.core.types.StringType;
import spade.query.quickgrail.core.utility.ResultTable;
import spade.query.quickgrail.core.utility.Schema;
import spade.query.quickgrail.quickstep.core.QuickstepEnvironment;
import spade.query.quickgrail.quickstep.core.QuickstepUtil;
import spade.query.quickgrail.quickstep.entities.QuickstepGraph;
import spade.query.quickgrail.quickstep.entities.QuickstepGraphMetadata;
import spade.storage.Quickstep;

/**
 * List all existing graphs in QuickGrail storage.
 */
public class ListGraphs
	extends AbstractListGraphs<QuickstepGraph, QuickstepGraphMetadata, QuickstepEnvironment, Quickstep>{

	public ListGraphs(String style){
		super(style);
	}

	@Override
	public void execute(QuickstepEnvironment env, ExecutionContext ctx, Quickstep storage){
		ResultTable table = new ResultTable();

		Map<String, String> symbols = env.getSymbols();
		for(Entry<String, String> entry : symbols.entrySet())
		{
			String symbol = entry.getKey();
			if(symbol.startsWith("$"))
			{
				addSymbol(storage, symbol, new QuickstepGraph(entry.getValue()), table);
			}
		}
		addSymbol(storage, "$base", QuickstepEnvironment.kBaseGraph, table);

		Schema schema = new Schema();
		schema.addColumn("Graph Name", StringType.GetInstance());
		if(!style.equals("name"))
		{
			schema.addColumn("Number of Vertices", LongType.GetInstance());
			schema.addColumn("Number of Edges", LongType.GetInstance());
			if(style.equals("detail"))
			{
				schema.addColumn("Start Time", LongType.GetInstance());
				schema.addColumn("End Time", LongType.GetInstance());
			}
		}
		table.setSchema(schema);

		ctx.addResponse(table.toString());
	}

	private void addSymbol(Quickstep storage, String symbol,
						   QuickstepGraph graph, ResultTable table)
	{
		ResultTable.Row row = new ResultTable.Row();
		row.add(symbol);
		if(!style.equals("name"))
		{
			row.add(QuickstepUtil.GetNumVertices(storage, graph));
			row.add(QuickstepUtil.GetNumEdges(storage, graph));
			if(style.equals("detail"))
			{
				Long[] span = QuickstepUtil.GetTimestampRange(storage, graph);
				row.add(span[0]);
				row.add(span[1]);
			}
		}
		table.addRow(row);
	}
}
