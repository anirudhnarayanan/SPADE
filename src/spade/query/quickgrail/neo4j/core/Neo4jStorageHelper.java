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

import static spade.query.quickgrail.neo4j.core.CommonVariables.CHILD_VERTEX_KEY;
import static spade.query.quickgrail.neo4j.core.CommonVariables.EDGE_ALIAS;
import static spade.query.quickgrail.neo4j.core.CommonVariables.PARENT_VERTEX_KEY;
import static spade.query.quickgrail.neo4j.core.CommonVariables.PRIMARY_KEY;
import static spade.query.quickgrail.neo4j.core.CommonVariables.VERTEX_ALIAS;
import static spade.query.quickgrail.neo4j.core.CommonVariables.NodeTypes.VERTEX;
import static spade.query.quickgrail.neo4j.core.CommonVariables.RelationshipTypes.EDGE;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import spade.core.AbstractEdge;
import spade.core.AbstractVertex;
import spade.core.Edge;
import spade.core.Vertex;
import spade.storage.Neo4j;

/**
 * Convenient functions.
 */
public class Neo4jStorageHelper
{
	private static final Logger logger = Logger.getLogger(Neo4jStorageHelper.class.getName());

	public static Set<String> GetAllTableNames(Neo4j storage)
	{
		String query = "CALL db.labels() YIELD label WHERE label <> '" + VERTEX.toString() + "' RETURN label";
		return storage.executeQueryForLabels(query);
	}

	public static long GetNumVertices(Neo4j storage, String label)
	{
		switch(label)
		{
			case "$base":
				label = VERTEX.toString();
				break;
			default:
				label = removeDollar(label);
		}
		return storage.executeQueryForLongResult("MATCH (" + VERTEX_ALIAS + ":" + label + ")  " +
				" RETURN COUNT(*) as count");
	}

	public static long GetNumEdges(Neo4j storage, String label)
	{
		String query;
		query = "MATCH ()-[" + EDGE_ALIAS + ":" + EDGE.toString() + "]->() ";
		if(!label.equals("$base") && !label.equals(EDGE.toString()))
		{
			query += " WHERE " + EDGE_ALIAS + ".quickgrail_symbol CONTAINS '," + removeDollar(label) + ",' ";
		}
		query += " RETURN  count(*) as count";
		return storage.executeQueryForLongResult(query);
	}

	public static Map<String, AbstractVertex> prepareVertexMapFromNeo4jResult(Neo4j storage, String query)
	{
		logger.log(Level.INFO, "vertex query: " + query);
		Map<String, AbstractVertex> vertexMap = new HashMap<>();
		try
		{
			Set<Node> nodeSet = storage.executeQueryForNodeSetResult(query);
			if(CollectionUtils.isNotEmpty(nodeSet))
			{
				for(Node node : nodeSet)
				{
					AbstractVertex vertex = convertNodeToVertex(node);
					logger.log(Level.INFO, "vertex: " + vertex);
					if(!vertex.isEmpty())
						vertexMap.put(vertex.bigHashCode(), vertex);
				}
			}
		}
		catch(Exception ex)
		{
			logger.log(Level.SEVERE, "Preparing vertex map from Neo4j unsuccessful!", ex);
		}
		return vertexMap;
	}

	private static AbstractVertex convertNodeToVertex(Node node)
	{
		AbstractVertex resultVertex = new Vertex();
		for(String key : node.getPropertyKeys())
		{
			if(!StringUtils.isBlank(key))
			{
				Object value = node.getProperty(key);
				value = String.valueOf(value);
				if(!key.equalsIgnoreCase(PRIMARY_KEY))
				{
					resultVertex.addAnnotation(key, (String) value);
				}
			}
		}
		return resultVertex;
	}

	public static Set<AbstractEdge> prepareEdgeSetFromNeo4jResult(Neo4j storage, String query,
																  Map<String, AbstractVertex> vertexMap)
	{
		logger.log(Level.INFO, "edge query: " + query);
		Set<AbstractEdge> edgeSet = new HashSet<>();
		try
		{
			Set<Relationship> relationshipSet = storage.executeQueryForRelationshipSetResult(query);
			if(CollectionUtils.isNotEmpty(relationshipSet))
			{
				for(Relationship relationship : relationshipSet)
				{
					AbstractEdge edge = convertRelationshipToEdge(relationship, vertexMap);
					logger.log(Level.INFO, "edge: " + edge);
					if(!edge.isEmpty())
						edgeSet.add(edge);
				}
			}
		}
		catch(Exception ex)
		{
			logger.log(Level.SEVERE, "Preparing edge set from Neo4j unsuccessful!", ex);
		}
		return edgeSet;
	}

	private static AbstractEdge convertRelationshipToEdge(Relationship relationship,
														  Map<String, AbstractVertex> vertexMap)
	{
		AbstractEdge edge = new Edge(null, null);
		for(String key : relationship.getPropertyKeys())
		{
			if(!StringUtils.isBlank(key))
			{
				Object value = relationship.getProperty(key);
				value = String.valueOf(value);
				if(!key.equalsIgnoreCase(PRIMARY_KEY))
				{
					edge.addAnnotation(key, (String) value);
				}
				if(key.equals(CHILD_VERTEX_KEY))
				{
					AbstractVertex childVertex = vertexMap.get(value);
					edge.setChildVertex(childVertex);
				}
				if(key.equals(PARENT_VERTEX_KEY))
				{
					AbstractVertex parentVertex = vertexMap.get(value);
					edge.setParentVertex(parentVertex);
				}
			}
		}
		return edge;
	}

	public static String removeDollar(String str)
	{
		if(str != null)
		{
			if(str.startsWith("$"))
			{
				str = str.substring(1);
			}
		}
		return str;
	}

	public static String vertexLabelQuery(String condition, String subjectVertexTable, String targetVertexTable)
	{
		String cypherQuery = "MATCH (" + VERTEX_ALIAS + ":" + subjectVertexTable + ") " +
				" WITH REDUCE(s = {a:[], d:[]}, x IN COLLECT(" + VERTEX_ALIAS + ") | " +
				" CASE  WHEN " + condition + " THEN {a: s.a+x, d: s.d} " +
				" WHEN '" + targetVertexTable + "' IN labels(x) THEN {a: s.a, d: s.d+x} " +
				" ELSE {a:s.a, d:s.d} END) AS actions " +
				" FOREACH (d IN actions.d | REMOVE d:" + targetVertexTable + ")" +
				" FOREACH(a IN actions.a | SET a:" + targetVertexTable + ")";

		return cypherQuery;
	}

	public static String edgeSymbolQuery(String condition, String targetEdgeTable)
	{
		String addSymbol = " SET a.quickgrail_symbol = CASE WHEN NOT EXISTS(a.quickgrail_symbol) THEN " +
				formatSymbol(targetEdgeTable) + " WHEN a.quickgrail_symbol CONTAINS " +
				formatSymbol(targetEdgeTable) + " THEN a.quickgrail_symbol ELSE a.quickgrail_symbol + " +
				formatSymbol(targetEdgeTable) + " END";
		String removeSymbol = "SET d.quickgrail_symbol = " +
				"replace(d.quickgrail_symbol, " + formatSymbol(targetEdgeTable) + ", '')";

		String cypherQuery = "MATCH ()-[" + EDGE_ALIAS + ":" + EDGE.toString() + "]->() " +
				" WITH REDUCE(s = {a:[], d:[]}, x IN COLLECT(" + EDGE_ALIAS + ") | " +
				" CASE  WHEN " + condition + " THEN {a: s.a+x, d: s.d} " +
				" WHEN x.quickgrail_symbol CONTAINS " + formatSymbol(targetEdgeTable) + " THEN {a: s.a, d: s.d+x} " +
				" ELSE {a:s.a, d:s.d} END) AS actions " +
				" FOREACH (d IN actions.d | " + removeSymbol + ")" +
				" FOREACH(a IN actions.a | " + addSymbol + ")";

		return cypherQuery;
	}

	public static String formatSymbol(String symbol)
	{
		return "'," + symbol + ",'";
	}

	public static String formatString(String str)
	{
		if(str == null)
			return str;
		StringBuilder sb = new StringBuilder(100);
		boolean escaped = false;
		for(int i = 0; i < str.length(); ++i)
		{
			char c = str.charAt(i);
			if(c < 32)
			{
				switch(c)
				{
					case '\b':
						sb.append("\\b");
						break;
					case '\n':
						sb.append("\\n");
						break;
					case '\r':
						sb.append("\\r");
						break;
					case '\t':
						sb.append("\\t");
						break;
					default:
						sb.append("\\x" + Integer.toHexString(c));
						break;
				}
				escaped = true;
			}
			else
			{
				if(c == '\\')
				{
					sb.append('\\');
					escaped = true;
				}
				sb.append(c);
			}
		}
		return (escaped ? "e" : "") + "'" + sb.toString() + "'";
	}
}
