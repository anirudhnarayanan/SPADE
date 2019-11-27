package spade.query.quickgrail.quickstep.core;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import spade.query.quickgrail.core.entities.Entity;
import spade.query.quickgrail.core.entities.Graph;
import spade.query.quickgrail.core.entities.Graph.Direction;
import spade.query.quickgrail.core.entities.Graph.EdgeComponent;
import spade.query.quickgrail.core.entities.Graph.ExportFormat;
import spade.query.quickgrail.core.entities.Graph.GraphComponent;
import spade.query.quickgrail.core.entities.GraphMetadata.GraphMetadataComponent;
import spade.query.quickgrail.core.execution.AbstractCollapseEdge;
import spade.query.quickgrail.core.execution.AbstractCreateEmptyGraph;
import spade.query.quickgrail.core.execution.AbstractCreateEmptyGraphMetadata;
import spade.query.quickgrail.core.execution.AbstractDistinctifyGraph;
import spade.query.quickgrail.core.execution.AbstractEraseSymbols;
import spade.query.quickgrail.core.execution.AbstractEvaluateQuery;
import spade.query.quickgrail.core.execution.AbstractExportGraph;
import spade.query.quickgrail.core.execution.AbstractGetEdge;
import spade.query.quickgrail.core.execution.AbstractGetEdgeEndpoint;
import spade.query.quickgrail.core.execution.AbstractGetLineage;
import spade.query.quickgrail.core.execution.AbstractGetLink;
import spade.query.quickgrail.core.execution.AbstractGetPath;
import spade.query.quickgrail.core.execution.AbstractGetShortestPath;
import spade.query.quickgrail.core.execution.AbstractGetSubgraph;
import spade.query.quickgrail.core.execution.AbstractGetVertex;
import spade.query.quickgrail.core.execution.AbstractInsertLiteralEdge;
import spade.query.quickgrail.core.execution.AbstractInsertLiteralVertex;
import spade.query.quickgrail.core.execution.AbstractIntersectGraph;
import spade.query.quickgrail.core.execution.AbstractLimitGraph;
import spade.query.quickgrail.core.execution.AbstractListGraphs;
import spade.query.quickgrail.core.execution.AbstractOverwriteGraphMetadata;
import spade.query.quickgrail.core.execution.AbstractSetGraphMetadata;
import spade.query.quickgrail.core.execution.AbstractStatGraph;
import spade.query.quickgrail.core.execution.AbstractSubtractGraph;
import spade.query.quickgrail.core.execution.AbstractUnionGraph;
import spade.query.quickgrail.core.kernel.AbstractResolver;
import spade.query.quickgrail.core.parser.ParseExpression;
import spade.query.quickgrail.quickstep.entities.QuickstepGraph;
import spade.query.quickgrail.quickstep.entities.QuickstepGraphMetadata;
import spade.query.quickgrail.quickstep.execution.CollapseEdge;
import spade.query.quickgrail.quickstep.execution.CreateEmptyGraph;
import spade.query.quickgrail.quickstep.execution.CreateEmptyGraphMetadata;
import spade.query.quickgrail.quickstep.execution.DistinctifyGraph;
import spade.query.quickgrail.quickstep.execution.EraseSymbols;
import spade.query.quickgrail.quickstep.execution.EvaluateQuery;
import spade.query.quickgrail.quickstep.execution.ExportGraph;
import spade.query.quickgrail.quickstep.execution.GetEdge;
import spade.query.quickgrail.quickstep.execution.GetEdgeEndpoint;
import spade.query.quickgrail.quickstep.execution.GetLineage;
import spade.query.quickgrail.quickstep.execution.GetLink;
import spade.query.quickgrail.quickstep.execution.GetPath;
import spade.query.quickgrail.quickstep.execution.GetShortestPath;
import spade.query.quickgrail.quickstep.execution.GetSubgraph;
import spade.query.quickgrail.quickstep.execution.GetVertex;
import spade.query.quickgrail.quickstep.execution.InsertLiteralEdge;
import spade.query.quickgrail.quickstep.execution.InsertLiteralVertex;
import spade.query.quickgrail.quickstep.execution.IntersectGraph;
import spade.query.quickgrail.quickstep.execution.LimitGraph;
import spade.query.quickgrail.quickstep.execution.ListGraphs;
import spade.query.quickgrail.quickstep.execution.OverwriteGraphMetadata;
import spade.query.quickgrail.quickstep.execution.SetGraphMetadata;
import spade.query.quickgrail.quickstep.execution.StatGraph;
import spade.query.quickgrail.quickstep.execution.SubtractGraph;
import spade.query.quickgrail.quickstep.execution.UnionGraph;
import spade.storage.Quickstep;

public class QuickstepResolver
	extends AbstractResolver<QuickstepGraph, QuickstepGraphMetadata, QuickstepEnvironment, Quickstep>{

	@Override
	public QuickstepGraph resolveAsVertexOrEdge(Graph.GraphComponent component,
			ArrayList<ParseExpression> arguments,
			QuickstepGraph outputGraph){

		// TODO: handle subject?
		if(arguments.size() != 1){
			throw new RuntimeException(
					"Invalid number of arguments for asVertex/asEdge: expected 1");
		}

		if(outputGraph == null){
			outputGraph = allocateEmptyGraph();
		}

		String rawQuery = resolveString(arguments.get(0));
		StringBuffer sb = new StringBuffer();
		sb.append("INSERT INTO " + outputGraph.getTableName(component) + " ");

		Pattern pattern = Pattern.compile("[$][^.]+[.](vertex|edge)");
		Matcher m = pattern.matcher(rawQuery);
		while(m.find()){
			String ref = m.group();
			String var;
			Graph.GraphComponent refComponent;
			if(ref.endsWith(".vertex")){
				refComponent = Graph.GraphComponent.kVertex;
				var = ref.substring(0, ref.length() - 7);
			}else{
				assert ref.endsWith(".edge");
				refComponent = Graph.GraphComponent.kEdge;
				var = ref.substring(0, ref.length() - 5);
			}
			String graphName = getCurrentEnvironment().lookup(var);
			if(graphName == null){
				throw new RuntimeException(
						"Cannot resolve variable " + var + " in the query at " +
								arguments.get(0).getLocationString());
			}
			m.appendReplacement(sb, allocateEmptyGraph(graphName).getTableName(refComponent));
		}
		m.appendTail(sb);

		addInstruction(newInstructionEvaluateQuery(sb.toString()));
		return outputGraph;
	}
	
	@Override
	public QuickstepGraph castEntityToGraph(Entity entity){
		if(entity.getClass().equals(QuickstepGraph.class)){
			return (QuickstepGraph)entity;
		}else{
			throw new RuntimeException("Expected '" + QuickstepGraph.class.getSimpleName() + 
					"' but is: " + entity.getClass());
		}
	}

	@Override
	public QuickstepGraphMetadata castEntityToGraphMetadata(Entity entity){
		if(entity.getClass().equals(QuickstepGraphMetadata.class)){
			return (QuickstepGraphMetadata)entity;
		}else{
			throw new RuntimeException("Expected '" + QuickstepGraphMetadata.class.getSimpleName() + 
					"' but is: " + entity.getClass());
		}
	}
	
	@Override
	public AbstractCollapseEdge<QuickstepGraph, QuickstepGraphMetadata, QuickstepEnvironment, Quickstep> newInstructionCollapseEdge(
			QuickstepGraph targetGraph, QuickstepGraph sourceGraph, ArrayList<String> fields){
		return new CollapseEdge(targetGraph, sourceGraph, fields);
	}

	@Override
	public AbstractCreateEmptyGraph<QuickstepGraph, QuickstepGraphMetadata, QuickstepEnvironment, Quickstep> newInstructionCreateEmptyGraph(
			QuickstepGraph graph){
		return new CreateEmptyGraph(graph);
	}

	@Override
	public AbstractCreateEmptyGraphMetadata<QuickstepGraph, QuickstepGraphMetadata, QuickstepEnvironment, Quickstep> newInstructionCreateEmptyGraphMetadata(
			QuickstepGraphMetadata metadata){
		return new CreateEmptyGraphMetadata(metadata);
	}

	@Override
	public AbstractDistinctifyGraph<QuickstepGraph, QuickstepGraphMetadata, QuickstepEnvironment, Quickstep> newInstructionDistinctifyGraph(
			QuickstepGraph targetGraph, QuickstepGraph sourceGraph){
		return new DistinctifyGraph(targetGraph, sourceGraph);
	}

	@Override
	public AbstractEraseSymbols<QuickstepGraph, QuickstepGraphMetadata, QuickstepEnvironment, Quickstep> newInstructionEraseSymbols(
			List<String> symbols){
		return new EraseSymbols(symbols);
	}

	@Override
	public AbstractEvaluateQuery<QuickstepGraph, QuickstepGraphMetadata, QuickstepEnvironment, Quickstep> newInstructionEvaluateQuery(
			String query){
		return new EvaluateQuery(query);
	}

	@Override
	public AbstractExportGraph<QuickstepGraph, QuickstepGraphMetadata, QuickstepEnvironment, Quickstep> newInstructionExportGraph(
			QuickstepGraph targetGraph, ExportFormat format, boolean force){
		return new ExportGraph(targetGraph, format, force);
	}

	@Override
	public AbstractGetEdge<QuickstepGraph, QuickstepGraphMetadata, QuickstepEnvironment, Quickstep> newInstructionGetEdge(
			QuickstepGraph targetGraph, QuickstepGraph subjectGraph, String field, String operation, String value){
		return new GetEdge(targetGraph, subjectGraph, field, operation, value);
	}

	@Override
	public AbstractGetEdgeEndpoint<QuickstepGraph, QuickstepGraphMetadata, QuickstepEnvironment, Quickstep> newInstructionGetEdgeEndpoint(
			QuickstepGraph targetGraph, QuickstepGraph subjectGraph, EdgeComponent component){
		return new GetEdgeEndpoint(targetGraph, subjectGraph, component);
	}

	@Override
	public AbstractGetLineage<QuickstepGraph, QuickstepGraphMetadata, QuickstepEnvironment, Quickstep> newInstructionGetLineage(
			QuickstepGraph targetGraph, QuickstepGraph subjectGraph, QuickstepGraph startGraph, Integer depth, Direction direction){
		return new GetLineage(targetGraph, subjectGraph, startGraph, depth, direction);
	}

	@Override
	public AbstractGetLink<QuickstepGraph, QuickstepGraphMetadata, QuickstepEnvironment, Quickstep> newInstructionGetLink(
			QuickstepGraph targetGraph, QuickstepGraph subjectGraph, QuickstepGraph sourceGraph, QuickstepGraph destinationGraph,
			Integer maxDepth){
		return new GetLink(targetGraph, subjectGraph, sourceGraph, destinationGraph, maxDepth);
	}

	@Override
	public AbstractGetPath<QuickstepGraph, QuickstepGraphMetadata, QuickstepEnvironment, Quickstep> newInstructionGetPath(
			QuickstepGraph targetGraph, QuickstepGraph subjectGraph, QuickstepGraph srcGraph, QuickstepGraph dstGraph,
			Integer maxDepth){
		return new GetPath(targetGraph, subjectGraph, srcGraph, dstGraph, maxDepth);
	}

	@Override
	public AbstractGetShortestPath<QuickstepGraph, QuickstepGraphMetadata, QuickstepEnvironment, Quickstep> newInstructionGetShortestPath(
			QuickstepGraph targetGraph, QuickstepGraph subjectGraph, QuickstepGraph srcGraph, QuickstepGraph dstGraph,
			Integer maxDepth){
		return new GetShortestPath(targetGraph, subjectGraph, srcGraph, dstGraph, maxDepth);
	}

	@Override
	public AbstractGetSubgraph<QuickstepGraph, QuickstepGraphMetadata, QuickstepEnvironment, Quickstep> newInstructionGetSubgraph(
			QuickstepGraph targetGraph, QuickstepGraph subjectGraph, QuickstepGraph skeletonGraph){
		return new GetSubgraph(targetGraph, subjectGraph, skeletonGraph);
	}

	@Override
	public AbstractGetVertex<QuickstepGraph, QuickstepGraphMetadata, QuickstepEnvironment, Quickstep> newInstructionGetVertex(
			QuickstepGraph targetGraph, QuickstepGraph subjectGraph, String field, String operation, String value){
		return new GetVertex(targetGraph, subjectGraph, field, operation, value);
	}

	@Override
	public AbstractInsertLiteralEdge<QuickstepGraph, QuickstepGraphMetadata, QuickstepEnvironment, Quickstep> newInstructionInsertLiteralEdge(
			QuickstepGraph targetGraph, ArrayList<String> edges){
		return new InsertLiteralEdge(targetGraph, edges);
	}

	@Override
	public AbstractInsertLiteralVertex<QuickstepGraph, QuickstepGraphMetadata, QuickstepEnvironment, Quickstep> newInstructionInsertLiteralVertex(
			QuickstepGraph targetGraph, ArrayList<String> vertices){
		return new InsertLiteralVertex(targetGraph, vertices);
	}

	@Override
	public AbstractIntersectGraph<QuickstepGraph, QuickstepGraphMetadata, QuickstepEnvironment, Quickstep> newInstructionIntersectGraph(
			QuickstepGraph outputGraph, QuickstepGraph lhsGraph, QuickstepGraph rhsGraph){
		return new IntersectGraph(outputGraph, lhsGraph, rhsGraph);
	}

	@Override
	public AbstractLimitGraph<QuickstepGraph, QuickstepGraphMetadata, QuickstepEnvironment, Quickstep> newInstructionLimitGraph(
			QuickstepGraph targetGraph, QuickstepGraph sourceGraph, int limit){
		return new LimitGraph(targetGraph, sourceGraph, limit);
	}

	@Override
	public AbstractListGraphs<QuickstepGraph, QuickstepGraphMetadata, QuickstepEnvironment, Quickstep> newInstructionListGraph(String style){
		return new ListGraphs(style);
	}

	@Override
	public AbstractOverwriteGraphMetadata<QuickstepGraph, QuickstepGraphMetadata, QuickstepEnvironment, Quickstep> newInstructionOverwriteGraphMetadata(
			QuickstepGraphMetadata targetMetadata, QuickstepGraphMetadata lhsMetadata, QuickstepGraphMetadata rhsMetadata){
		return new OverwriteGraphMetadata(targetMetadata, lhsMetadata, rhsMetadata);
	}

	@Override
	public AbstractSetGraphMetadata<QuickstepGraph, QuickstepGraphMetadata, QuickstepEnvironment, Quickstep> newInstructionSetGraphMetadata(
			QuickstepGraphMetadata targetMetadata, GraphMetadataComponent component, QuickstepGraph sourceGraph, String name,
			String value){
		return new SetGraphMetadata(targetMetadata, component, sourceGraph, name, value);
	}

	@Override
	public AbstractStatGraph<QuickstepGraph, QuickstepGraphMetadata, QuickstepEnvironment, Quickstep> newInstructionStatGraph(
			QuickstepGraph targetGraph){
		return new StatGraph(targetGraph);
	}

	@Override
	public AbstractSubtractGraph<QuickstepGraph, QuickstepGraphMetadata, QuickstepEnvironment, Quickstep> newInstructionSubtractGraph(
			QuickstepGraph outputGraph, QuickstepGraph minuendGraph, QuickstepGraph subtrahendGraph, GraphComponent component){
		return new SubtractGraph(outputGraph, minuendGraph, subtrahendGraph, component);
	}

	@Override
	public AbstractUnionGraph<QuickstepGraph, QuickstepGraphMetadata, QuickstepEnvironment, Quickstep> newInstructionUnionGraph(
			QuickstepGraph targetGraph, QuickstepGraph sourceGraph){
		return new UnionGraph(targetGraph, sourceGraph);
	}

}
