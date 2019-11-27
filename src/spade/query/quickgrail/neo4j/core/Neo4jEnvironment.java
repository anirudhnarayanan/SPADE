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
package spade.query.quickgrail.neo4j.core;

import static spade.query.quickgrail.neo4j.core.CommonVariables.EDGE_ALIAS;
import static spade.query.quickgrail.neo4j.core.CommonVariables.VERTEX_ALIAS;
import static spade.query.quickgrail.neo4j.core.CommonVariables.NodeTypes.VERTEX;
import static spade.query.quickgrail.neo4j.core.CommonVariables.RelationshipTypes.EDGE;
import static spade.query.quickgrail.neo4j.core.Neo4jStorageHelper.removeDollar;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.collections.CollectionUtils;

import spade.query.quickgrail.core.kernel.AbstractEnvironment;
import spade.query.quickgrail.neo4j.entities.Neo4jGraph;
import spade.query.quickgrail.neo4j.entities.Neo4jGraphMetadata;
import spade.storage.Neo4j;

/**
 * QuickGrail compile-time environment (also used in runtime) mainly for
 * managing symbols (e.g. mapping from graph variables to underlying Neo4j).
 */
public class Neo4jEnvironment
	extends AbstractEnvironment<Neo4jGraph, Neo4jGraphMetadata, Neo4j>{
	
	private final static Neo4jGraph kBaseGraph = new Neo4jGraph("$base");
	private HashMap<String, String> symbols;
	// for access in query classes
	private static String resultGraphName;
	private static final Logger logger = Logger.getLogger(Neo4jEnvironment.class.getName());

	public Neo4jEnvironment(Neo4j storage)
	{
		super(storage);
		this.symbols = new HashMap<>();

		// Get the count of different graph variables/tables represented as labels.
		Set<String> tableNames = Neo4jStorageHelper.GetAllTableNames(storage);
		if(CollectionUtils.isNotEmpty(tableNames))
		{
			for(Object table : tableNames)
			{
				String tableName = (String) table;
				symbols.put("$" + tableName, tableName);
			}
		}
	}

	@Override
	public boolean IsBaseGraph(Neo4jGraph graph)
	{
		return graph.getName().equals(kBaseGraph.getName());
	}

	@Override
	public void clear()
	{
		if(symbols.size() > 0)
		{
			// remove all labels from all nodes
			StringBuilder removeLabelsQuery = new StringBuilder(100);
			removeLabelsQuery.append("MATCH (" + VERTEX_ALIAS + ":");
			removeLabelsQuery.append(VERTEX.toString());
			removeLabelsQuery.append(")");
			removeLabelsQuery.append("REMOVE ");
			removeLabelsQuery.append(VERTEX_ALIAS);
			for(String symbol : symbols.keySet())
			{
				removeLabelsQuery.append(":").append(removeDollar(symbol));
			}
			storage.executeQuery(removeLabelsQuery.toString());

			// remove all symbols from all relationships
			String removeSymbolsQuery = "MATCH ()-[" + EDGE_ALIAS + ":" + EDGE.toString() + "]->()" +
					"REMOVE " + EDGE_ALIAS + ".quickgrail_symbol";
			storage.executeQuery(removeSymbolsQuery);

			symbols.clear();
		}
	}

	@Override
	public Neo4jGraph allocateGraph()
	{
		return new Neo4jGraph(resultGraphName);
	}

	@Override
	public Neo4jGraph allocateGraph(String graphName)
	{
		return new Neo4jGraph(graphName);
	}

	@Override
	public Neo4jGraphMetadata allocateGraphMetadata()
	{
		logger.log(Level.WARNING, "GraphMetadata operations not supported in SPADE yet");
		return new Neo4jGraphMetadata(resultGraphName);
	}

	@Override
	public Neo4jGraphMetadata allocateGraphMetadata(String graphName)
	{
		logger.log(Level.WARNING, "GraphMetadata operations not supported in SPADE yet");
		return new Neo4jGraphMetadata(graphName);
	}

	@Override
	public String lookup(String symbol)
	{
		switch(symbol)
		{
			case "$base":
				return kBaseGraph.getName();
		}
		if(symbols.containsKey(symbol))
		{
			return symbol;
		}
		return null;
	}

	public void setResultGraphName(String resultGraphName)
	{
		Neo4jEnvironment.resultGraphName = resultGraphName;
	}

	@Override
	public void addSymbol(String symbol, String value)
	{
		switch(symbol)
		{
			case "$base":
				throw new RuntimeException("Cannot reassign reserved variables.");
		}
		symbols.put(symbol, removeDollar(symbol));
	}

	@Override
	public void eraseSymbol(String symbol)
	{
		switch(symbol)
		{
			case "$base":
				logger.log(Level.WARNING, "Cannot erase reserved symbols.");
				throw new RuntimeException("Cannot erase reserved symbols.");
		}
		if(symbols.containsKey(symbol))
		{
			// remove label from all nodes
			String removeLabelQuery = "MATCH (" + VERTEX_ALIAS + ":" + VERTEX.toString() + ")" +
					"REMOVE " + VERTEX_ALIAS + ":" + removeDollar(symbol);
			storage.executeQuery(removeLabelQuery);
			// remove label from all relationships
			String removeSymbolQuery = "MATCH ()-[" + EDGE_ALIAS + ":" + EDGE.toString() + "]->() " +
					"WHERE " + EDGE_ALIAS + ".quickgrail_symbol CONTAINS '," + removeDollar(symbol) + ",'" +
					"SET " + EDGE_ALIAS + ".quickgrail_symbol = " +
					"replace(" + EDGE_ALIAS + ".quickgrail_symbol, '," + removeDollar(symbol) + ",', '')";
			storage.executeQuery(removeSymbolQuery);
			symbols.remove(symbol);
		}
	}

	@Override
	public final Map<String, String> getSymbols()
	{
		return symbols;
	}

	@Override
	public void gc()
	{
	}
}
