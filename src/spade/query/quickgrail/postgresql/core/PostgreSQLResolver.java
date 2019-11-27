package spade.query.quickgrail.postgresql.core;

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
import spade.query.quickgrail.postgresql.entities.PostgreSQLGraph;
import spade.query.quickgrail.postgresql.entities.PostgreSQLGraphMetadata;
import spade.query.quickgrail.postgresql.execution.CollapseEdge;
import spade.query.quickgrail.postgresql.execution.CreateEmptyGraph;
import spade.query.quickgrail.postgresql.execution.CreateEmptyGraphMetadata;
import spade.query.quickgrail.postgresql.execution.DistinctifyGraph;
import spade.query.quickgrail.postgresql.execution.EraseSymbols;
import spade.query.quickgrail.postgresql.execution.EvaluateQuery;
import spade.query.quickgrail.postgresql.execution.ExportGraph;
import spade.query.quickgrail.postgresql.execution.GetEdge;
import spade.query.quickgrail.postgresql.execution.GetEdgeEndpoint;
import spade.query.quickgrail.postgresql.execution.GetLineage;
import spade.query.quickgrail.postgresql.execution.GetLink;
import spade.query.quickgrail.postgresql.execution.GetPath;
import spade.query.quickgrail.postgresql.execution.GetShortestPath;
import spade.query.quickgrail.postgresql.execution.GetSubgraph;
import spade.query.quickgrail.postgresql.execution.GetVertex;
import spade.query.quickgrail.postgresql.execution.InsertLiteralEdge;
import spade.query.quickgrail.postgresql.execution.InsertLiteralVertex;
import spade.query.quickgrail.postgresql.execution.IntersectGraph;
import spade.query.quickgrail.postgresql.execution.LimitGraph;
import spade.query.quickgrail.postgresql.execution.ListGraphs;
import spade.query.quickgrail.postgresql.execution.OverwriteGraphMetadata;
import spade.query.quickgrail.postgresql.execution.SetGraphMetadata;
import spade.query.quickgrail.postgresql.execution.StatGraph;
import spade.query.quickgrail.postgresql.execution.SubtractGraph;
import spade.query.quickgrail.postgresql.execution.UnionGraph;
import spade.storage.PostgreSQL;

public class PostgreSQLResolver
	extends AbstractResolver<PostgreSQLGraph, PostgreSQLGraphMetadata, PostgreSQLEnvironment, PostgreSQL>{

	@Override
	public PostgreSQLGraph resolveAsVertexOrEdge(Graph.GraphComponent component,
			ArrayList<ParseExpression> arguments,
			PostgreSQLGraph outputGraph){

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
	public PostgreSQLGraph castEntityToGraph(Entity entity){
		if(entity.getClass().equals(PostgreSQLGraph.class)){
			return (PostgreSQLGraph)entity;
		}else{
			throw new RuntimeException("Expected '" + PostgreSQLGraph.class.getSimpleName() + 
					"' but is: " + entity.getClass());
		}
	}

	@Override
	public PostgreSQLGraphMetadata castEntityToGraphMetadata(Entity entity){
		if(entity.getClass().equals(PostgreSQLGraphMetadata.class)){
			return (PostgreSQLGraphMetadata)entity;
		}else{
			throw new RuntimeException("Expected '" + PostgreSQLGraphMetadata.class.getSimpleName() + 
					"' but is: " + entity.getClass());
		}
	}
	
	@Override
	public AbstractCollapseEdge<PostgreSQLGraph, PostgreSQLGraphMetadata, PostgreSQLEnvironment, PostgreSQL> newInstructionCollapseEdge(
			PostgreSQLGraph targetGraph, PostgreSQLGraph sourceGraph, ArrayList<String> fields){
		return new CollapseEdge(targetGraph, sourceGraph, fields);
	}

	@Override
	public AbstractCreateEmptyGraph<PostgreSQLGraph, PostgreSQLGraphMetadata, PostgreSQLEnvironment, PostgreSQL> newInstructionCreateEmptyGraph(
			PostgreSQLGraph graph){
		return new CreateEmptyGraph(graph);
	}

	@Override
	public AbstractCreateEmptyGraphMetadata<PostgreSQLGraph, PostgreSQLGraphMetadata, PostgreSQLEnvironment, PostgreSQL> newInstructionCreateEmptyGraphMetadata(
			PostgreSQLGraphMetadata metadata){
		return new CreateEmptyGraphMetadata(metadata);
	}

	@Override
	public AbstractDistinctifyGraph<PostgreSQLGraph, PostgreSQLGraphMetadata, PostgreSQLEnvironment, PostgreSQL> newInstructionDistinctifyGraph(
			PostgreSQLGraph targetGraph, PostgreSQLGraph sourceGraph){
		return new DistinctifyGraph(targetGraph, sourceGraph);
	}

	@Override
	public AbstractEraseSymbols<PostgreSQLGraph, PostgreSQLGraphMetadata, PostgreSQLEnvironment, PostgreSQL> newInstructionEraseSymbols(
			List<String> symbols){
		return new EraseSymbols(symbols);
	}

	@Override
	public AbstractEvaluateQuery<PostgreSQLGraph, PostgreSQLGraphMetadata, PostgreSQLEnvironment, PostgreSQL> newInstructionEvaluateQuery(
			String query){
		return new EvaluateQuery(query);
	}

	@Override
	public AbstractExportGraph<PostgreSQLGraph, PostgreSQLGraphMetadata, PostgreSQLEnvironment, PostgreSQL> newInstructionExportGraph(
			PostgreSQLGraph targetGraph, ExportFormat format, boolean force){
		return new ExportGraph(targetGraph, format, force);
	}

	@Override
	public AbstractGetEdge<PostgreSQLGraph, PostgreSQLGraphMetadata, PostgreSQLEnvironment, PostgreSQL> newInstructionGetEdge(
			PostgreSQLGraph targetGraph, PostgreSQLGraph subjectGraph, String field, String operation, String value){
		return new GetEdge(targetGraph, subjectGraph, field, operation, value);
	}

	@Override
	public AbstractGetEdgeEndpoint<PostgreSQLGraph, PostgreSQLGraphMetadata, PostgreSQLEnvironment, PostgreSQL> newInstructionGetEdgeEndpoint(
			PostgreSQLGraph targetGraph, PostgreSQLGraph subjectGraph, EdgeComponent component){
		return new GetEdgeEndpoint(targetGraph, subjectGraph, component);
	}

	@Override
	public AbstractGetLineage<PostgreSQLGraph, PostgreSQLGraphMetadata, PostgreSQLEnvironment, PostgreSQL> newInstructionGetLineage(
			PostgreSQLGraph targetGraph, PostgreSQLGraph subjectGraph, PostgreSQLGraph startGraph, Integer depth, Direction direction){
		return new GetLineage(targetGraph, subjectGraph, startGraph, depth, direction);
	}

	@Override
	public AbstractGetLink<PostgreSQLGraph, PostgreSQLGraphMetadata, PostgreSQLEnvironment, PostgreSQL> newInstructionGetLink(
			PostgreSQLGraph targetGraph, PostgreSQLGraph subjectGraph, PostgreSQLGraph sourceGraph, PostgreSQLGraph destinationGraph,
			Integer maxDepth){
		return new GetLink(targetGraph, subjectGraph, sourceGraph, destinationGraph, maxDepth);
	}

	@Override
	public AbstractGetPath<PostgreSQLGraph, PostgreSQLGraphMetadata, PostgreSQLEnvironment, PostgreSQL> newInstructionGetPath(
			PostgreSQLGraph targetGraph, PostgreSQLGraph subjectGraph, PostgreSQLGraph srcGraph, PostgreSQLGraph dstGraph,
			Integer maxDepth){
		return new GetPath(targetGraph, subjectGraph, srcGraph, dstGraph, maxDepth);
	}

	@Override
	public AbstractGetShortestPath<PostgreSQLGraph, PostgreSQLGraphMetadata, PostgreSQLEnvironment, PostgreSQL> newInstructionGetShortestPath(
			PostgreSQLGraph targetGraph, PostgreSQLGraph subjectGraph, PostgreSQLGraph srcGraph, PostgreSQLGraph dstGraph,
			Integer maxDepth){
		return new GetShortestPath(targetGraph, subjectGraph, srcGraph, dstGraph, maxDepth);
	}

	@Override
	public AbstractGetSubgraph<PostgreSQLGraph, PostgreSQLGraphMetadata, PostgreSQLEnvironment, PostgreSQL> newInstructionGetSubgraph(
			PostgreSQLGraph targetGraph, PostgreSQLGraph subjectGraph, PostgreSQLGraph skeletonGraph){
		return new GetSubgraph(targetGraph, subjectGraph, skeletonGraph);
	}

	@Override
	public AbstractGetVertex<PostgreSQLGraph, PostgreSQLGraphMetadata, PostgreSQLEnvironment, PostgreSQL> newInstructionGetVertex(
			PostgreSQLGraph targetGraph, PostgreSQLGraph subjectGraph, String field, String operation, String value){
		return new GetVertex(targetGraph, subjectGraph, field, operation, value);
	}

	@Override
	public AbstractInsertLiteralEdge<PostgreSQLGraph, PostgreSQLGraphMetadata, PostgreSQLEnvironment, PostgreSQL> newInstructionInsertLiteralEdge(
			PostgreSQLGraph targetGraph, ArrayList<String> edges){
		return new InsertLiteralEdge(targetGraph, edges);
	}

	@Override
	public AbstractInsertLiteralVertex<PostgreSQLGraph, PostgreSQLGraphMetadata, PostgreSQLEnvironment, PostgreSQL> newInstructionInsertLiteralVertex(
			PostgreSQLGraph targetGraph, ArrayList<String> vertices){
		return new InsertLiteralVertex(targetGraph, vertices);
	}

	@Override
	public AbstractIntersectGraph<PostgreSQLGraph, PostgreSQLGraphMetadata, PostgreSQLEnvironment, PostgreSQL> newInstructionIntersectGraph(
			PostgreSQLGraph outputGraph, PostgreSQLGraph lhsGraph, PostgreSQLGraph rhsGraph){
		return new IntersectGraph(outputGraph, lhsGraph, rhsGraph);
	}

	@Override
	public AbstractLimitGraph<PostgreSQLGraph, PostgreSQLGraphMetadata, PostgreSQLEnvironment, PostgreSQL> newInstructionLimitGraph(
			PostgreSQLGraph targetGraph, PostgreSQLGraph sourceGraph, int limit){
		return new LimitGraph(targetGraph, sourceGraph, limit);
	}

	@Override
	public AbstractListGraphs<PostgreSQLGraph, PostgreSQLGraphMetadata, PostgreSQLEnvironment, PostgreSQL> newInstructionListGraph(String style){
		return new ListGraphs(style);
	}

	@Override
	public AbstractOverwriteGraphMetadata<PostgreSQLGraph, PostgreSQLGraphMetadata, PostgreSQLEnvironment, PostgreSQL> newInstructionOverwriteGraphMetadata(
			PostgreSQLGraphMetadata targetMetadata, PostgreSQLGraphMetadata lhsMetadata, PostgreSQLGraphMetadata rhsMetadata){
		return new OverwriteGraphMetadata(targetMetadata, lhsMetadata, rhsMetadata);
	}

	@Override
	public AbstractSetGraphMetadata<PostgreSQLGraph, PostgreSQLGraphMetadata, PostgreSQLEnvironment, PostgreSQL> newInstructionSetGraphMetadata(
			PostgreSQLGraphMetadata targetMetadata, GraphMetadataComponent component, PostgreSQLGraph sourceGraph, String name,
			String value){
		return new SetGraphMetadata(targetMetadata, component, sourceGraph, name, value);
	}

	@Override
	public AbstractStatGraph<PostgreSQLGraph, PostgreSQLGraphMetadata, PostgreSQLEnvironment, PostgreSQL> newInstructionStatGraph(
			PostgreSQLGraph targetGraph){
		return new StatGraph(targetGraph);
	}

	@Override
	public AbstractSubtractGraph<PostgreSQLGraph, PostgreSQLGraphMetadata, PostgreSQLEnvironment, PostgreSQL> newInstructionSubtractGraph(
			PostgreSQLGraph outputGraph, PostgreSQLGraph minuendGraph, PostgreSQLGraph subtrahendGraph, GraphComponent component){
		return new SubtractGraph(outputGraph, minuendGraph, subtrahendGraph, component);
	}

	@Override
	public AbstractUnionGraph<PostgreSQLGraph, PostgreSQLGraphMetadata, PostgreSQLEnvironment, PostgreSQL> newInstructionUnionGraph(
			PostgreSQLGraph targetGraph, PostgreSQLGraph sourceGraph){
		return new UnionGraph(targetGraph, sourceGraph);
	}
}
