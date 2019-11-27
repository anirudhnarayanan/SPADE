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

import static spade.query.quickgrail.core.entities.Graph.kNonForceDumpLimit;
import static spade.query.quickgrail.core.entities.Graph.kNonForceVisualizeLimit;

import java.sql.ResultSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import spade.core.AbstractEdge;
import spade.core.AbstractVertex;
import spade.core.Vertex;
import spade.query.quickgrail.core.entities.Graph.ExportFormat;
import spade.query.quickgrail.core.execution.AbstractExportGraph;
import spade.query.quickgrail.core.kernel.ExecutionContext;
import spade.query.quickgrail.postgresql.core.PostgreSQLEnvironment;
import spade.query.quickgrail.postgresql.core.PostgresUtil;
import spade.query.quickgrail.postgresql.entities.PostgreSQLGraph;
import spade.query.quickgrail.postgresql.entities.PostgreSQLGraphMetadata;
import spade.storage.PostgreSQL;
import spade.storage.PostgreSQLSchema;

/**
 * Export a QuickGrail graph to spade.core.Graph or to DOT representation.
 */
public class ExportGraph
	extends AbstractExportGraph<PostgreSQLGraph, PostgreSQLGraphMetadata, PostgreSQLEnvironment, PostgreSQL>{
	private static final Logger logger = Logger.getLogger(ExportGraph.class.getName());
	
	public ExportGraph(PostgreSQLGraph targetGraph, ExportFormat format, boolean force){
		super(targetGraph, format, force);
	}

	@Override
	public void execute(PostgreSQLEnvironment env, ExecutionContext ctx, PostgreSQL storage){
		if(!force){
			long numVertices = PostgresUtil.GetNumVertices(storage, targetGraph);
			long numEdges = PostgresUtil.GetNumEdges(storage, targetGraph);
			long graphsize = numVertices + numEdges;
			if(format == ExportFormat.kNormal && (graphsize > kNonForceDumpLimit)){
				ctx.addResponse("It may take a long time to print the result data due to " +
						"too many vertices/edges: " + numVertices + "/" + numEdges + "" +
						"Please use 'force dump ...' to force the print");
				return;
			}else if(format == ExportFormat.kDot && (graphsize > kNonForceVisualizeLimit)){
				ctx.addResponse("It may take a long time to transfer the result data due to " +
						"too many vertices/edges: " + numVertices + "/" + numEdges + "" +
						"Please use 'force visualize ...' to force the transfer");
				return;
			}
		}

		PostgreSQLSchema schema = storage.getSchema();
		
		final String mVertexTable = "m_vertex",
				mEdgeTable = "m_edge";
		
		storage.executeQuery(
				schema.queriesDropAndCreateHashOnlyTable(mVertexTable) + 
				schema.queriesDropAndCreateHashOnlyTable(mEdgeTable));

		String targetVertexTable = targetGraph.getVertexTableName();
		String targetEdgeTable = targetGraph.getEdgeTableName();
		storage.executeQuery(
				"insert into " + schema.formatTableNameForQuery(mVertexTable) + 
				" select " + schema.formatColumnNameForQuery(schema.hashColumnName) + 
				" from " + schema.formatTableNameForQuery(targetVertexTable) + " group by " +
				schema.formatColumnNameForQuery(schema.hashColumnName) + ";" +
				"insert into " + schema.formatTableNameForQuery(mEdgeTable) + 
				" select " + schema.formatColumnNameForQuery(schema.hashColumnName) + 
				" from " + schema.formatTableNameForQuery(targetEdgeTable) + " group by " +
				schema.formatColumnNameForQuery(schema.hashColumnName) + ";");

		HashMap<String, AbstractVertex> vertices = exportVertices(storage, mVertexTable);
		Set<AbstractEdge> edges = exportEdges(storage, "m_edge", vertices);

		storage.executeQuery(
				schema.queryDropTableIfExists(mVertexTable) +
				schema.queryDropTableIfExists(mEdgeTable));

		spade.core.Graph graph = new spade.core.Graph();
		graph.vertexSet().addAll(vertices.values());
		graph.edgeSet().addAll(edges);

		if(format == ExportFormat.kNormal)
		{
			ctx.addResponse(graph.toString());
		}
		else
		{
			ctx.addResponse(graph.exportGraph());
		}
	}

	private HashMap<String, AbstractVertex> exportVertices(
			PostgreSQL storage, String targetVertexTable){
		HashMap<String, AbstractVertex> vertices = new HashMap<>();
		long numVertices = storage.getRowCountOfTableSafe(targetVertexTable);
		if(numVertices <= 0){
			return vertices;
		}else{
			PostgreSQLSchema schema = storage.getSchema();
			String mainVertexTableName = schema.mainVertexTableName;
			String hashColumnName = schema.hashColumnName;
			
			ResultSet resultSet = null;
			try{
				resultSet = storage.executeQuery(
						"select * from " + schema.formatTableNameForQuery(mainVertexTableName) + 
						" where " + schema.formatColumnNameForQuery(hashColumnName) + 
						" in (select " + schema.formatColumnNameForQuery(hashColumnName) + " from " +
						schema.formatTableNameForQuery(targetVertexTable) + ");");
				
				List<Map<String, String>> listOfMaps = PostgresUtil.sqlTableAsListOfKeyValuesMap(resultSet);
				if(listOfMaps == null){
					// do nothing
				}else{
					for(Map<String, String> annotations : listOfMaps){
						String hash = annotations.remove(hashColumnName); // remove if exists
						if(hash == null){ // do what? TODO
							
						}else{
							AbstractVertex vertex = inflateVertex(hash, annotations);
							if(vertex != null){
								vertices.put(hash, vertex);
							}
						}
					}
				}
			}
			catch(Exception ex){
				logger.log(Level.SEVERE, "Error fetching vertices from the database!", ex);
			}finally{
				if(resultSet != null){
					try{ resultSet.close(); }catch(Exception e){}
				}
			}
			return vertices;
		}
	}

	private Set<AbstractEdge> exportEdges(
			PostgreSQL storage, String targetEdgeTable, HashMap<String, AbstractVertex> vertices){
		Set<AbstractEdge> edges = new HashSet<>();
		long numEdges = storage.getRowCountOfTableSafe(targetEdgeTable);
		if(numEdges <= 0){
			return edges;
		}else{
			PostgreSQLSchema schema = storage.getSchema();
			String mainEdgeTableName = schema.mainEdgeTableName;
			String hashColumnName = schema.hashColumnName;
			String childHashColumnName = schema.childVertexHashColumnName;
			String parentHashColumnName = schema.parentVertexHashColumnName;
			
			ResultSet resultSet = null;
			try{
				resultSet = storage.executeQuery(
						"select * from " + schema.formatTableNameForQuery(mainEdgeTableName) + 
						" where " + schema.formatColumnNameForQuery(hashColumnName) + 
						" in (select " + schema.formatColumnNameForQuery(hashColumnName) + " from " +
						schema.formatTableNameForQuery(targetEdgeTable) + ");");
				
				List<Map<String, String>> listOfMaps = PostgresUtil.sqlTableAsListOfKeyValuesMap(resultSet);
				if(listOfMaps == null){
					// do nothing
				}else{
					for(Map<String, String> annotations : listOfMaps){
						String edgeHash = annotations.remove(hashColumnName);
						String childHash = annotations.remove(childHashColumnName);
						String parentHash = annotations.remove(parentHashColumnName);
						
						AbstractEdge edge = inflateEdge(edgeHash, annotations, 
								vertices.get(childHash), vertices.get(parentHash));
						if(edge != null){
							edges.add(edge);
						}
					}
				}
			}
			catch(Exception ex){
				logger.log(Level.SEVERE, "Error fetching edges from the database!", ex);
			}finally{
				if(resultSet != null){try{resultSet.close();}catch(Exception e){}}
			}
			return edges;
		}
	}
	
	public static AbstractVertex inflateVertex(String hash,
    		Map<String, String> annotations){
		if(annotations != null){
			AbstractVertex vertex = new Vertex();
			vertex.addAnnotations(annotations);
			return vertex;
		}
		return null;
    }
    
    public static AbstractEdge inflateEdge(String edgeHash, Map<String, String> edgeAnnotations,
    		AbstractVertex childVertex, AbstractVertex parentVertex){
    	if(childVertex != null && parentVertex != null){
	    	AbstractEdge edge = new spade.core.Edge(childVertex, parentVertex);
	    	if(edgeAnnotations != null){
	    		edge.addAnnotations(edgeAnnotations);
	    	}
	    	return edge;
    	}
    	return null;
    }
}
