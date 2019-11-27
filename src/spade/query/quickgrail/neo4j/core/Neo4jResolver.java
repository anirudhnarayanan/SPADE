package spade.query.quickgrail.neo4j.core;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

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
import spade.query.quickgrail.core.parser.ParseAssignment;
import spade.query.quickgrail.core.parser.ParseExpression;
import spade.query.quickgrail.core.parser.ParseString;
import spade.query.quickgrail.core.types.TypeID;
import spade.query.quickgrail.neo4j.entities.Neo4jGraph;
import spade.query.quickgrail.neo4j.entities.Neo4jGraphMetadata;
import spade.query.quickgrail.neo4j.execution.CollapseEdge;
import spade.query.quickgrail.neo4j.execution.CreateEmptyGraph;
import spade.query.quickgrail.neo4j.execution.CreateEmptyGraphMetadata;
import spade.query.quickgrail.neo4j.execution.DistinctifyGraph;
import spade.query.quickgrail.neo4j.execution.EraseSymbols;
import spade.query.quickgrail.neo4j.execution.EvaluateQuery;
import spade.query.quickgrail.neo4j.execution.ExportGraph;
import spade.query.quickgrail.neo4j.execution.GetEdge;
import spade.query.quickgrail.neo4j.execution.GetEdgeEndpoint;
import spade.query.quickgrail.neo4j.execution.GetLineage;
import spade.query.quickgrail.neo4j.execution.GetLink;
import spade.query.quickgrail.neo4j.execution.GetPath;
import spade.query.quickgrail.neo4j.execution.GetShortestPath;
import spade.query.quickgrail.neo4j.execution.GetSubgraph;
import spade.query.quickgrail.neo4j.execution.GetVertex;
import spade.query.quickgrail.neo4j.execution.InsertLiteralEdge;
import spade.query.quickgrail.neo4j.execution.InsertLiteralVertex;
import spade.query.quickgrail.neo4j.execution.IntersectGraph;
import spade.query.quickgrail.neo4j.execution.LimitGraph;
import spade.query.quickgrail.neo4j.execution.ListGraphs;
import spade.query.quickgrail.neo4j.execution.OverwriteGraphMetadata;
import spade.query.quickgrail.neo4j.execution.SetGraphMetadata;
import spade.query.quickgrail.neo4j.execution.StatGraph;
import spade.query.quickgrail.neo4j.execution.SubtractGraph;
import spade.query.quickgrail.neo4j.execution.UnionGraph;
import spade.storage.Neo4j;

public class Neo4jResolver 
	extends AbstractResolver<Neo4jGraph, Neo4jGraphMetadata, Neo4jEnvironment, Neo4j>{

	@Override
	public void resolveGraphAssignment(ParseAssignment parseAssignment){
		if(parseAssignment.getLhs().getType().getTypeID() != TypeID.kGraph){
			throw new RuntimeException("Expected 'Graph' type but is: " + parseAssignment.getLhs().getType().getTypeID());
		}

		Neo4jEnvironment currentEnv = getCurrentEnvironment();
		
		ParseString var = parseAssignment.getLhs().getName();
		ParseExpression rhs = parseAssignment.getRhs();
		ParseAssignment.AssignmentType atype = parseAssignment.getAssignmentType();

		Neo4jGraph resultGraph;
		// this is done in order to propagate the resulting label/variable
		// to the Neo4j query classes for adding to the nodes and relationships
		// TODO
		String lhs = var.getValue();
		if(lhs.equals("$base")){
			throw new RuntimeException("Cannot reassign reserved variables.");
		}
		currentEnv.setResultGraphName(lhs);		

		if(atype == ParseAssignment.AssignmentType.kEqual){
			resultGraph = resolveGraphExpression(rhs, null, true);
		}else{
			String lhsGraphName = currentEnv.lookup(var.getValue());
			if(lhsGraphName == null){
				throw new RuntimeException(
						"Cannot resolve Graph variable " + var.getValue() +
						" at " + var.getLocationString());
			}
			Neo4jGraph lhsGraph = allocateEmptyGraph(lhsGraphName);
			switch(atype){
				case kPlusEqual:{
					if(!currentEnv.IsBaseGraph(lhsGraph)){
						resultGraph = lhsGraph;
					}else{
						resultGraph = allocateEmptyGraph();
						addInstruction(newInstructionUnionGraph(resultGraph, lhsGraph));
					}
					resolveGraphExpression(rhs, resultGraph, true);
					break;
				}
				case kMinusEqual:{
					Neo4jGraph rhsGraph = resolveGraphExpression(rhs, null, true);
					resultGraph = allocateEmptyGraph();
					addInstruction(newInstructionSubtractGraph(resultGraph, lhsGraph, rhsGraph, null));
					break;
				}
				case kIntersectEqual:{
					Neo4jGraph rhsGraph = resolveGraphExpression(rhs, null, true);
					resultGraph = allocateEmptyGraph();
					addInstruction(newInstructionIntersectGraph(resultGraph, lhsGraph, rhsGraph));
					break;
				}
				default:
					throw new RuntimeException(
							"Unsupported assignment " +
									parseAssignment.getAssignmentType().name().substring(1) +
									" at " + parseAssignment.getLocationString());
			}
		}
		Neo4jGraph distinctifiedGraph = allocateEmptyGraph();
		addInstruction(newInstructionDistinctifyGraph(distinctifiedGraph, resultGraph));
		currentEnv.addSymbol(var.getValue(), distinctifiedGraph.getName());
	}
	
	@Override
	public Neo4jGraph resolveAsVertexOrEdge(Graph.GraphComponent component,
			ArrayList<ParseExpression> arguments,
			Neo4jGraph outputGraph){
		logger.log(Level.SEVERE, "This feature is not present for Neo4j.");
		throw new RuntimeException("This feature is not present for Neo4j.");
	}
	
	@Override
	public Neo4jGraph castEntityToGraph(Entity entity){
		if(entity.getClass().equals(Neo4jGraph.class)){
			return (Neo4jGraph)entity;
		}else{
			throw new RuntimeException("Expected '" + Neo4jGraph.class.getSimpleName() + 
					"' but is: " + entity.getClass());
		}
	}

	@Override
	public Neo4jGraphMetadata castEntityToGraphMetadata(Entity entity){
		if(entity.getClass().equals(Neo4jGraphMetadata.class)){
			return (Neo4jGraphMetadata)entity;
		}else{
			throw new RuntimeException("Expected '" + Neo4jGraphMetadata.class.getSimpleName() + 
					"' but is: " + entity.getClass());
		}
	}
	
	@Override
	public AbstractCollapseEdge<Neo4jGraph, Neo4jGraphMetadata, Neo4jEnvironment, Neo4j> newInstructionCollapseEdge(
			Neo4jGraph targetGraph, Neo4jGraph sourceGraph, ArrayList<String> fields){
		return new CollapseEdge(targetGraph, sourceGraph, fields);
	}

	@Override
	public AbstractCreateEmptyGraph<Neo4jGraph, Neo4jGraphMetadata, Neo4jEnvironment, Neo4j> newInstructionCreateEmptyGraph(
			Neo4jGraph graph){
		return new CreateEmptyGraph(graph);
	}

	@Override
	public AbstractCreateEmptyGraphMetadata<Neo4jGraph, Neo4jGraphMetadata, Neo4jEnvironment, Neo4j> newInstructionCreateEmptyGraphMetadata(
			Neo4jGraphMetadata metadata){
		return new CreateEmptyGraphMetadata(metadata);
	}

	@Override
	public AbstractDistinctifyGraph<Neo4jGraph, Neo4jGraphMetadata, Neo4jEnvironment, Neo4j> newInstructionDistinctifyGraph(
			Neo4jGraph targetGraph, Neo4jGraph sourceGraph){
		return new DistinctifyGraph(targetGraph, sourceGraph);
	}

	@Override
	public AbstractEraseSymbols<Neo4jGraph, Neo4jGraphMetadata, Neo4jEnvironment, Neo4j> newInstructionEraseSymbols(
			List<String> symbols){
		return new EraseSymbols(symbols);
	}

	@Override
	public AbstractEvaluateQuery<Neo4jGraph, Neo4jGraphMetadata, Neo4jEnvironment, Neo4j> newInstructionEvaluateQuery(
			String query){
		return new EvaluateQuery(query);
	}

	@Override
	public AbstractExportGraph<Neo4jGraph, Neo4jGraphMetadata, Neo4jEnvironment, Neo4j> newInstructionExportGraph(
			Neo4jGraph targetGraph, ExportFormat format, boolean force){
		return new ExportGraph(targetGraph, format, force);
	}

	@Override
	public AbstractGetEdge<Neo4jGraph, Neo4jGraphMetadata, Neo4jEnvironment, Neo4j> newInstructionGetEdge(
			Neo4jGraph targetGraph, Neo4jGraph subjectGraph, String field, String operation, String value){
		return new GetEdge(targetGraph, subjectGraph, field, operation, value);
	}

	@Override
	public AbstractGetEdgeEndpoint<Neo4jGraph, Neo4jGraphMetadata, Neo4jEnvironment, Neo4j> newInstructionGetEdgeEndpoint(
			Neo4jGraph targetGraph, Neo4jGraph subjectGraph, EdgeComponent component){
		return new GetEdgeEndpoint(targetGraph, subjectGraph, component);
	}

	@Override
	public AbstractGetLineage<Neo4jGraph, Neo4jGraphMetadata, Neo4jEnvironment, Neo4j> newInstructionGetLineage(
			Neo4jGraph targetGraph, Neo4jGraph subjectGraph, Neo4jGraph startGraph, Integer depth, Direction direction){
		return new GetLineage(targetGraph, subjectGraph, startGraph, depth, direction);
	}

	@Override
	public AbstractGetLink<Neo4jGraph, Neo4jGraphMetadata, Neo4jEnvironment, Neo4j> newInstructionGetLink(
			Neo4jGraph targetGraph, Neo4jGraph subjectGraph, Neo4jGraph sourceGraph, Neo4jGraph destinationGraph,
			Integer maxDepth){
		return new GetLink(targetGraph, subjectGraph, sourceGraph, destinationGraph, maxDepth);
	}

	@Override
	public AbstractGetPath<Neo4jGraph, Neo4jGraphMetadata, Neo4jEnvironment, Neo4j> newInstructionGetPath(
			Neo4jGraph targetGraph, Neo4jGraph subjectGraph, Neo4jGraph srcGraph, Neo4jGraph dstGraph,
			Integer maxDepth){
		return new GetPath(targetGraph, subjectGraph, srcGraph, dstGraph, maxDepth);
	}

	@Override
	public AbstractGetShortestPath<Neo4jGraph, Neo4jGraphMetadata, Neo4jEnvironment, Neo4j> newInstructionGetShortestPath(
			Neo4jGraph targetGraph, Neo4jGraph subjectGraph, Neo4jGraph srcGraph, Neo4jGraph dstGraph,
			Integer maxDepth){
		return new GetShortestPath(targetGraph, subjectGraph, srcGraph, dstGraph, maxDepth);
	}

	@Override
	public AbstractGetSubgraph<Neo4jGraph, Neo4jGraphMetadata, Neo4jEnvironment, Neo4j> newInstructionGetSubgraph(
			Neo4jGraph targetGraph, Neo4jGraph subjectGraph, Neo4jGraph skeletonGraph){
		return new GetSubgraph(targetGraph, subjectGraph, skeletonGraph);
	}

	@Override
	public AbstractGetVertex<Neo4jGraph, Neo4jGraphMetadata, Neo4jEnvironment, Neo4j> newInstructionGetVertex(
			Neo4jGraph targetGraph, Neo4jGraph subjectGraph, String field, String operation, String value){
		return new GetVertex(targetGraph, subjectGraph, field, operation, value);
	}

	@Override
	public AbstractInsertLiteralEdge<Neo4jGraph, Neo4jGraphMetadata, Neo4jEnvironment, Neo4j> newInstructionInsertLiteralEdge(
			Neo4jGraph targetGraph, ArrayList<String> edges){
		return new InsertLiteralEdge(targetGraph, edges);
	}

	@Override
	public AbstractInsertLiteralVertex<Neo4jGraph, Neo4jGraphMetadata, Neo4jEnvironment, Neo4j> newInstructionInsertLiteralVertex(
			Neo4jGraph targetGraph, ArrayList<String> vertices){
		return new InsertLiteralVertex(targetGraph, vertices);
	}

	@Override
	public AbstractIntersectGraph<Neo4jGraph, Neo4jGraphMetadata, Neo4jEnvironment, Neo4j> newInstructionIntersectGraph(
			Neo4jGraph outputGraph, Neo4jGraph lhsGraph, Neo4jGraph rhsGraph){
		return new IntersectGraph(outputGraph, lhsGraph, rhsGraph);
	}

	@Override
	public AbstractLimitGraph<Neo4jGraph, Neo4jGraphMetadata, Neo4jEnvironment, Neo4j> newInstructionLimitGraph(
			Neo4jGraph targetGraph, Neo4jGraph sourceGraph, int limit){
		return new LimitGraph(targetGraph, sourceGraph, limit);
	}

	@Override
	public AbstractListGraphs<Neo4jGraph, Neo4jGraphMetadata, Neo4jEnvironment, Neo4j> newInstructionListGraph(String style){
		return new ListGraphs(style);
	}

	@Override
	public AbstractOverwriteGraphMetadata<Neo4jGraph, Neo4jGraphMetadata, Neo4jEnvironment, Neo4j> newInstructionOverwriteGraphMetadata(
			Neo4jGraphMetadata targetMetadata, Neo4jGraphMetadata lhsMetadata, Neo4jGraphMetadata rhsMetadata){
		return new OverwriteGraphMetadata(targetMetadata, lhsMetadata, rhsMetadata);
	}

	@Override
	public AbstractSetGraphMetadata<Neo4jGraph, Neo4jGraphMetadata, Neo4jEnvironment, Neo4j> newInstructionSetGraphMetadata(
			Neo4jGraphMetadata targetMetadata, GraphMetadataComponent component, Neo4jGraph sourceGraph, String name,
			String value){
		return new SetGraphMetadata(targetMetadata, component, sourceGraph, name, value);
	}

	@Override
	public AbstractStatGraph<Neo4jGraph, Neo4jGraphMetadata, Neo4jEnvironment, Neo4j> newInstructionStatGraph(
			Neo4jGraph targetGraph){
		return new StatGraph(targetGraph);
	}

	@Override
	public AbstractSubtractGraph<Neo4jGraph, Neo4jGraphMetadata, Neo4jEnvironment, Neo4j> newInstructionSubtractGraph(
			Neo4jGraph outputGraph, Neo4jGraph minuendGraph, Neo4jGraph subtrahendGraph, GraphComponent component){
		return new SubtractGraph(outputGraph, minuendGraph, subtrahendGraph, component);
	}

	@Override
	public AbstractUnionGraph<Neo4jGraph, Neo4jGraphMetadata, Neo4jEnvironment, Neo4j> newInstructionUnionGraph(
			Neo4jGraph targetGraph, Neo4jGraph sourceGraph){
		return new UnionGraph(targetGraph, sourceGraph);
	}

}
