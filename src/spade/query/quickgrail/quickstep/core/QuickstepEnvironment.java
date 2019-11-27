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
package spade.query.quickgrail.quickstep.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import spade.query.quickgrail.core.kernel.AbstractEnvironment;
import spade.query.quickgrail.quickstep.entities.QuickstepGraph;
import spade.query.quickgrail.quickstep.entities.QuickstepGraphMetadata;
import spade.storage.Quickstep;

/**
 * QuickGrail compile-time environment (also used in runtime) mainly for
 * managing symbols (e.g. mapping from graph variables to underlying Quickstep
 * tables).
 */
public class QuickstepEnvironment extends AbstractEnvironment<QuickstepGraph, QuickstepGraphMetadata, Quickstep>{
	
	public final String symbolsTablesName = "symbols";
	public final String symbolsTableCreateQuery = "create table " + symbolsTablesName + " (name varchar(128), value varchar(128));";
	public final String symbolsTableDropQuery = "drop table " + symbolsTablesName + ";";
	
	public final static QuickstepGraph kBaseGraph = new QuickstepGraph("trace_base"); // TODO

	private HashMap<String, String> symbols;
	
	public QuickstepEnvironment(Quickstep storage){
		super(storage);
		this.symbols = new HashMap<>();

		// Initialize the symbols table if it does not exist.
		if(!QuickstepUtil.GetAllTableNames(storage).contains(symbolsTablesName)){
			storage.executeQuery(symbolsTableCreateQuery);
		}

		// Initialize the symbols buffer.
		String lines = String.valueOf(storage.executeQuery("COPY SELECT * FROM symbols TO stdout WITH (DELIMITER ',');"));
		for(String line : lines.split("\n")){
			String[] items = line.split(",");
			if(items.length == 2){
				symbols.put(items[0], items[1]);
			}
		}
	}

	@Override
	public boolean IsBaseGraph(QuickstepGraph graph){
		return graph.getName().equals(kBaseGraph.getName());
	}

	@Override
	public void clear(){
		symbols.clear();
		storage.executeQuery(symbolsTableDropQuery);
		storage.executeQuery(symbolsTableCreateQuery);
		gc();
	}

	public QuickstepGraph allocateGraph(){
		String idCounterStr = symbols.get("id_counter");
		if(idCounterStr == null){
			idCounterStr = "0";
		}
		int idCounter = Integer.parseInt(idCounterStr);
		String nextIdStr = String.valueOf(++idCounter);
		addSymbol("id_counter", nextIdStr);
		return new QuickstepGraph("trace_" + nextIdStr);
	}

	@Override
	public QuickstepGraph allocateGraph(String graphName){
		return new QuickstepGraph(graphName);
	}


	public QuickstepGraphMetadata allocateGraphMetadata(){
		String idCounterStr = symbols.get("id_counter");
		if(idCounterStr == null){
			idCounterStr = "0";
		}
		int idCounter = Integer.parseInt(idCounterStr);
		String nextIdStr = String.valueOf(++idCounter);
		addSymbol("id_counter", nextIdStr);
		return new QuickstepGraphMetadata("meta_" + nextIdStr);
	}

	@Override
	public QuickstepGraphMetadata allocateGraphMetadata(String graphName){
		return new QuickstepGraphMetadata(graphName);
	}

	@Override
	public String lookup(String symbol){
		switch(symbol){
			case "$base":
				return kBaseGraph.getName();
		}
		return symbols.get(symbol);
	}

	@Override
	public void addSymbol(String symbol, String value){
		switch(symbol){
			case "$base":
				throw new RuntimeException("Cannot reassign reserved variables.");
		}
		if(symbols.containsKey(symbol)){
			storage.executeQuery("UPDATE symbols SET value = '" + value +
					"' WHERE name = '" + symbol + "';");
		}else{
			storage.executeQuery("INSERT INTO symbols VALUES('" + symbol + "', '" + value + "');");
		}
		symbols.put(symbol, value);
	}

	@Override
	public void eraseSymbol(String symbol){
		switch(symbol){
			case "$base":
				throw new RuntimeException("Cannot erase reserved symbols.");
		}
		if(symbols.containsKey(symbol)){
			symbols.remove(symbol);
			storage.executeQuery("DELETE FROM symbols WHERE name = '" + symbol + "';");
		}
	}

	private boolean isGarbageTable(HashSet<String> referencedTables, String table){
		if(table.startsWith("m_")){
			return true;
		}
		if(table.startsWith("trace") || table.startsWith("meta")){
			return !referencedTables.contains(table);
		}
		return false;
	}

	@Override
	public final Map<String, String> getSymbols(){
		return symbols;
	}

	@Override
	public void gc(){
		HashSet<String> referencedTables = new HashSet<String>();
		referencedTables.add(kBaseGraph.getVertexTableName());
		referencedTables.add(kBaseGraph.getEdgeTableName());
		for(String graphName : symbols.values()){
			QuickstepGraph graph = new QuickstepGraph(graphName);
			referencedTables.add(graph.getVertexTableName());
			referencedTables.add(graph.getEdgeTableName());
		}
		ArrayList<String> allTables = QuickstepUtil.GetAllTableNames(storage);
		StringBuilder dropQuery = new StringBuilder();
		for(String table : allTables){
			if(isGarbageTable(referencedTables, table)){
				dropQuery.append("DROP TABLE " + table + ";\n");
			}
		}
		if(dropQuery.length() > 0){
			storage.executeQuery(dropQuery.toString());
		}
	}
	
	@Override
	public void setResultGraphName(String graphName){}
}
