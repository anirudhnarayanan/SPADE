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
package spade.query.quickgrail.postgresql.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.mysql.jdbc.StringUtils;

import spade.query.quickgrail.core.kernel.AbstractEnvironment;
import spade.query.quickgrail.postgresql.entities.PostgreSQLGraph;
import spade.query.quickgrail.postgresql.entities.PostgreSQLGraphMetadata;
import spade.storage.PostgreSQL;
import spade.storage.PostgreSQLSchema;

/**
 * QuickGrail compile-time environment (also used in runtime) mainly for
 * managing symbols (e.g. mapping from graph variables to underlying Postgres
 * tables).
 */
public class PostgreSQLEnvironment extends AbstractEnvironment<PostgreSQLGraph, PostgreSQLGraphMetadata, PostgreSQL>
{
	public final static PostgreSQLGraph kBaseGraph = new PostgreSQLGraph("trace_base");
	private static final Logger logger = Logger.getLogger(PostgreSQLEnvironment.class.getName());

	private HashMap<String, String> symbols;

	public PostgreSQLEnvironment(PostgreSQL storage)
	{
		super(storage);
		this.symbols = new HashMap<>();

		// Initialize the symbols table if it does not exist.
		storage.executeQuery("CREATE TABLE IF NOT EXISTS symbols (name VARCHAR(128), value VARCHAR(128));");

		// Initialize the symbols buffer.
		String lines = storage.executeCopy("COPY (SELECT * FROM symbols) TO stdout WITH (DELIMITER ',');");
		if(!StringUtils.isNullOrEmpty(lines))
		{
			for(String line : lines.split("\n"))
			{
				String[] items = line.split(",");
				if(items.length == 2)
				{
					symbols.put(items[0], items[1]);
				}
			}
		}
	}

	@Override
	public boolean IsBaseGraph(PostgreSQLGraph graph)
	{
		return graph.getName().equals(kBaseGraph.getName());
	}

	@Override
	public void clear()
	{
		storage.executeQuery("DROP TABLE IF EXISTS symbols;");
		storage.executeQuery("CREATE TABLE symbols (name VARCHAR(128), value VARCHAR(128));");
		symbols.clear();
		gc();
	}

	@Override
	public PostgreSQLGraph allocateGraph()
	{
		String idCounterStr = symbols.get("id_counter");
		if(idCounterStr == null)
		{
			idCounterStr = "0";
		}
		int idCounter = Integer.parseInt(idCounterStr);
		String nextIdStr = String.valueOf(++idCounter);
		addSymbol("id_counter", nextIdStr);
		return new PostgreSQLGraph("trace_" + nextIdStr);
	}

	@Override
	public PostgreSQLGraph allocateGraph(String graphName)
	{
		return new PostgreSQLGraph(graphName);
	}

	@Override
	public PostgreSQLGraphMetadata allocateGraphMetadata()
	{
		String idCounterStr = symbols.get("id_counter");
		if(idCounterStr == null)
		{
			idCounterStr = "0";
		}
		int idCounter = Integer.parseInt(idCounterStr);
		String nextIdStr = String.valueOf(++idCounter);
		addSymbol("id_counter", nextIdStr);
		return new PostgreSQLGraphMetadata("meta_" + nextIdStr);
	}

	@Override
	public PostgreSQLGraphMetadata allocateGraphMetadata(String graphName)
	{
		logger.log(Level.WARNING, "GraphMetadata operations not supported in SPADE yet");
		return new PostgreSQLGraphMetadata(graphName);
	}

	@Override
	public String lookup(String symbol)
	{
		switch(symbol)
		{
			case "$base":
				return kBaseGraph.getName();
		}
		return symbols.get(symbol);
	}

	@Override
	public void addSymbol(String symbol, String value)
	{
		switch(symbol)
		{
			case "$base":
				throw new RuntimeException("Cannot reassign reserved variables.");
		}
		if(symbols.containsKey(symbol))
		{
			storage.executeQuery("UPDATE symbols SET value = '" + value +
					"' WHERE name = '" + symbol + "';");
		}
		else
		{
			storage.executeQuery("INSERT INTO symbols VALUES('" + symbol + "', '" + value + "');");
		}
		symbols.put(symbol, value);
	}

	@Override
	public void eraseSymbol(String symbol){
		// TODO
		final PostgreSQL storage = null;
		switch(symbol){
			case "$base": throw new RuntimeException("Cannot erase reserved symbols.");
		}
		if(symbols.containsKey(symbol)){
			PostgreSQLSchema schema = storage.getSchema();
			
			symbols.remove(symbol);
			storage.executeQuery(schema.queryRemoveSymbol(symbol));
		}
	}

	private boolean isGarbageTable(HashSet<String> referencedTables, String table)
	{
		if(table.startsWith("m_"))
		{
			return true;
		}
		if(table.startsWith("trace") || table.startsWith("meta"))
		{
			return !referencedTables.contains(table);
		}
		return false;
	}

	@Override
	public final Map<String, String> getSymbols()
	{
		return symbols;
	}

	@Override
	public void gc()
	{
		HashSet<String> referencedTables = new HashSet<>();
		referencedTables.add(kBaseGraph.getVertexTableName());
		referencedTables.add(kBaseGraph.getEdgeTableName());
		for(String graphName : symbols.values())
		{
			PostgreSQLGraph graph = new PostgreSQLGraph(graphName);
			referencedTables.add(graph.getVertexTableName());
			referencedTables.add(graph.getEdgeTableName());
		}
		ArrayList<String> allTables = PostgresUtil.GetAllTableNames(storage);
		StringBuilder dropQuery = new StringBuilder();
		for(String table : allTables)
		{
			if(isGarbageTable(referencedTables, table))
			{
				dropQuery.append("DROP TABLE IF EXISTS ").append(table).append(";");
			}
		}
		if(dropQuery.length() > 0)
		{
			storage.executeQuery(dropQuery.toString());
		}
	}

	@Override
	public void setResultGraphName(String graphName)
	{
	}
}
