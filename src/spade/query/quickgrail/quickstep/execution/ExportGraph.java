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

import static spade.query.quickgrail.core.entities.Graph.kNonForceDumpLimit;
import static spade.query.quickgrail.core.entities.Graph.kNonForceVisualizeLimit;

import java.util.HashMap;

import spade.core.AbstractEdge;
import spade.core.AbstractVertex;
import spade.core.Edge;
import spade.core.Vertex;
import spade.query.quickgrail.core.entities.Graph.ExportFormat;
import spade.query.quickgrail.core.execution.AbstractExportGraph;
import spade.query.quickgrail.core.kernel.ExecutionContext;
import spade.query.quickgrail.quickstep.core.QuickstepEnvironment;
import spade.query.quickgrail.quickstep.core.QuickstepUtil;
import spade.query.quickgrail.quickstep.entities.QuickstepGraph;
import spade.query.quickgrail.quickstep.entities.QuickstepGraphMetadata;
import spade.storage.Quickstep;

/**
 * Export a QuickGrail graph to spade.core.Graph or to DOT representation.
 */
public class ExportGraph
	extends AbstractExportGraph<QuickstepGraph, QuickstepGraphMetadata, QuickstepEnvironment, Quickstep>{

	public ExportGraph(QuickstepGraph targetGraph, ExportFormat format, boolean force){
		super(targetGraph, format, force);
	}

	@Override
	public void execute(QuickstepEnvironment env, ExecutionContext ctx, Quickstep storage){
		if(!force)
		{
			long numVertices = QuickstepUtil.GetNumVertices(storage, targetGraph);
			long numEdges = QuickstepUtil.GetNumEdges(storage, targetGraph);
			long graphsize = numVertices + numEdges;
			if(format == ExportFormat.kNormal && (graphsize > kNonForceDumpLimit))
			{
				ctx.addResponse("It may take a long time to print the result data due to " +
						"too many vertices/edges: " + numVertices + "/" + numEdges + "" +
						"Please use 'force dump ...' to force the print");
				return;
			}
			else if(format == ExportFormat.kDot && (graphsize > kNonForceVisualizeLimit))
			{
				ctx.addResponse("It may take a long time to transfer the result data due to " +
						"too many vertices/edges: " + numVertices + "/" + numEdges + "" +
						"Please use 'force visualize ...' to force the transfer");
				return;
			}
		}

		storage.executeQuery("DROP TABLE m_init_vertex;\n" +
				"DROP TABLE m_vertex;\n" +
				"DROP TABLE m_edge;\n" +
				"CREATE TABLE m_init_vertex(id INT);\n" +
				"CREATE TABLE m_vertex(id INT);\n" +
				"CREATE TABLE m_edge(id LONG);");

		String targetVertexTable = targetGraph.getVertexTableName();
		String targetEdgeTable = targetGraph.getEdgeTableName();
		storage.executeQuery("INSERT INTO m_init_vertex" +
				" SELECT id FROM " + targetVertexTable + ";\n" +
				"INSERT INTO m_init_vertex" +
				" SELECT src FROM edge WHERE id IN (SELECT id FROM " + targetEdgeTable + ");\n" +
				"INSERT INTO m_init_vertex" +
				" SELECT dst FROM edge WHERE id IN (SELECT id FROM " + targetEdgeTable + ");\n" +
				"\\analyzerange " + targetVertexTable + " " + targetEdgeTable + "\n" +
				"INSERT INTO m_vertex SELECT id FROM m_init_vertex GROUP BY id;\n" +
				"INSERT INTO m_edge SELECt id FROM " + targetEdgeTable + " GROUP BY id;");

		HashMap<Integer, AbstractVertex> vertices = exportVertices(storage, "m_vertex");
		HashMap<Long, AbstractEdge> edges = exportEdges(storage, "m_edge");

		storage.executeQuery("DROP TABLE m_init_vertex;\n" +
				"DROP TABLE m_vertex;\n" +
				"DROP TABLE m_edge;");

		spade.core.Graph graph = new spade.core.Graph();
		graph.vertexSet().addAll(vertices.values());
		graph.edgeSet().addAll(edges.values());

		if(format == ExportFormat.kNormal)
		{
			ctx.addResponse(graph.toString());
		}
		else
		{
			ctx.addResponse(graph.exportGraph());
		}
	}

	private HashMap<Integer, AbstractVertex> exportVertices(
			Quickstep storage, String targetVertexTable)
	{
		HashMap<Integer, AbstractVertex> vertices = new HashMap<Integer, AbstractVertex>();
		long numVertices = storage.executeQueryForLongResult(
				"COPY SELECT COUNT(*) FROM " + targetVertexTable + " TO stdout;");
		if(numVertices == 0)
		{
			return vertices;
		}

		storage.executeQuery("\\analyzerange " + targetVertexTable + "\n");

		String vertexAnnoStr = String.valueOf(storage.executeQuery(
				"COPY SELECT * FROM vertex_anno WHERE id IN (SELECT id FROM " +
						targetVertexTable + ") TO stdout WITH (DELIMITER e'\\n');"));
		String[] vertexAnnoLines = vertexAnnoStr.split("\n");
		vertexAnnoStr = null;

		assert vertexAnnoLines.length % 3 == 0;
		for(int i = 0; i < vertexAnnoLines.length; i += 3)
		{
			// TODO: accelerate with cache.
			Integer id = Integer.parseInt(vertexAnnoLines[i]);
			AbstractVertex vertex = vertices.get(id);
			if(vertex == null)
			{
				vertex = new Vertex();
				vertices.put(id, vertex);
			}
			vertex.addAnnotation(vertexAnnoLines[i + 1], vertexAnnoLines[i + 2]);
		}
		return vertices;
	}

	private HashMap<Long, AbstractEdge> exportEdges(
			Quickstep storage, String targetEdgeTable)
	{
		HashMap<Long, AbstractEdge> edges = new HashMap<Long, AbstractEdge>();

		long numEdges = storage.executeQueryForLongResult(
				"COPY SELECT COUNT(*) FROM " + targetEdgeTable + " TO stdout;");
		if(numEdges == 0)
		{
			return edges;
		}

		storage.executeQuery("DROP TABLE m_answer;\n" +
				"CREATE TABLE m_answer(id INT);\n" +
				"DROP TABLE m_answer_edge;\n" +
				"CREATE TABLE m_answer_edge(id LONG, src INT, dst INT);\n" +
				"\\analyzerange " + targetEdgeTable + "\n" +
				"INSERT INTO m_answer_edge SELECT * FROM edge" +
				" WHERE id IN (SELECT id FROM " + targetEdgeTable + ");\n" +
				"INSERT INTO m_answer SELECT src FROM m_answer_edge;\n" +
				"INSERT INTO m_answer SELECT dst FROM m_answer_edge;");

		HashMap<Integer, AbstractVertex> vertices = exportVertices(storage, "m_answer");

		String edgeStr = String.valueOf(storage.executeQuery(
				"COPY SELECT * FROM m_answer_edge TO stdout WITH (DELIMITER e'\\n');"));
		String[] edgeLines = edgeStr.split("\n");
		edgeStr = null;

		assert edgeLines.length % 3 == 0;
		for(int i = 0; i < edgeLines.length; i += 3)
		{
			Long id = Long.parseLong(edgeLines[i]);
			Integer src = Integer.parseInt(edgeLines[i + 1]);
			Integer dst = Integer.parseInt(edgeLines[i + 2]);
			edges.put(id, new Edge(vertices.get(src), vertices.get(dst)));
		}
		edgeLines = null;

		String edgeAnnoStr = String.valueOf(storage.executeQuery(
				"COPY SELECT * FROM edge_anno WHERE id IN (SELECT id FROM " +
						targetEdgeTable + ") TO stdout WITH (DELIMITER e'\\n');"));
		String[] edgeAnnoLines = edgeAnnoStr.split("\n");
		edgeAnnoStr = null;

		assert edgeAnnoLines.length % 3 == 0;
		for(int i = 0; i < edgeAnnoLines.length; i += 3)
		{
			// TODO: accelerate with cache.
			Long id = Long.parseLong(edgeAnnoLines[i]);
			AbstractEdge edge = edges.get(id);
			if(edge == null)
			{
				continue;
			}
			edge.addAnnotation(edgeAnnoLines[i + 1], edgeAnnoLines[i + 2]);
		}
		storage.executeQuery("DROP TABLE m_answer;\n" +
				"DROP TABLE m_answer_edge;");
		return edges;
	}
}
