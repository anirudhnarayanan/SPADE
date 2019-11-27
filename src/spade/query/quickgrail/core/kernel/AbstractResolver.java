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
package spade.query.quickgrail.core.kernel;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import spade.core.AbstractStorage;
import spade.query.quickgrail.core.entities.Entity;
import spade.query.quickgrail.core.entities.EntityType;
import spade.query.quickgrail.core.entities.Graph;
import spade.query.quickgrail.core.entities.Graph.Direction;
import spade.query.quickgrail.core.entities.Graph.EdgeComponent;
import spade.query.quickgrail.core.entities.Graph.ExportFormat;
import spade.query.quickgrail.core.entities.Graph.GraphComponent;
import spade.query.quickgrail.core.entities.GraphMetadata;
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
import spade.query.quickgrail.core.parser.ParseAssignment;
import spade.query.quickgrail.core.parser.ParseCommand;
import spade.query.quickgrail.core.parser.ParseExpression;
import spade.query.quickgrail.core.parser.ParseExpression.ExpressionType;
import spade.query.quickgrail.core.parser.ParseLiteral;
import spade.query.quickgrail.core.parser.ParseName;
import spade.query.quickgrail.core.parser.ParseOperation;
import spade.query.quickgrail.core.parser.ParseProgram;
import spade.query.quickgrail.core.parser.ParseStatement;
import spade.query.quickgrail.core.parser.ParseString;
import spade.query.quickgrail.core.parser.ParseVariable;
import spade.query.quickgrail.core.types.Type;
import spade.query.quickgrail.core.types.TypeID;
import spade.query.quickgrail.core.types.TypedValue;

/**
 * Resolver that transforms a parse tree into a QuickGrail low-level program.
 */
public abstract class AbstractResolver
	<G extends Graph, GM extends GraphMetadata,
	E extends AbstractEnvironment<G, GM, S>, S extends AbstractStorage>{

	public final Logger logger = Logger.getLogger(this.getClass().getName());

	private ArrayList<Instruction<G, GM, E, S>> instructions;
	private E env;
	
	public E getCurrentEnvironment(){
		return env;
	}
	
	/**
	 * Top-level API for resolving parse trees (that represent a list of
	 * QuickGrail queries) into a low-level program (a list of primitive
	 * instructions ready to be executed).
	 */
	public final QuickGrailProgram<G, GM, E, S> resolveProgram(ParseProgram parseProgram, E env){
		// Initialize
		this.instructions = new ArrayList<>();
		this.env = env;

		// Resolve statements.
		for(ParseStatement parseStatement : parseProgram.getStatements()){
			resolveStatement(parseStatement);
		}

		QuickGrailProgram<G, GM, E, S> program = new QuickGrailProgram<G, GM, E, S>(instructions, env);

		// Cleanup and return.
		this.instructions = null;
		this.env = null;
		return program;
	}
	
	public final void addInstruction(Instruction<G, GM, E, S> instruction){
		if(instructions == null){
			throw new RuntimeException("Resolver in invalid state");
		}else{
			if(instruction == null){
				throw new RuntimeException("NULL instruction");
			}else{
				instructions.add(instruction);
			}
		}
	}

	public void resolveStatement(ParseStatement parseStatement){
		switch(parseStatement.getStatementType()){
			case kAssignment:
				resolveAssignment((ParseAssignment) parseStatement);
				break;
			case kCommand:
				resolveCommand((ParseCommand) parseStatement);
				break;
			default:
				String msg = "Unsupported statement type: " + parseStatement.getStatementType().name();
				throw new RuntimeException(msg);
		}
	}

	public void resolveAssignment(ParseAssignment parseAssignment){
		Type varType = parseAssignment.getLhs().getType();
		switch(varType.getTypeID()){
			case kGraph:
				resolveGraphAssignment(parseAssignment);
				break;
			case kGraphMetadata:
				resolveGraphMetadataAssignment(parseAssignment);
				break;
			default:
				throw new RuntimeException(
						"Unsupported variable type " + varType.getName() + " at " +
								parseAssignment.getLhs().getLocationString());
		}
	}

	public void resolveGraphAssignment(ParseAssignment parseAssignment){
		if(parseAssignment.getLhs().getType().getTypeID() != TypeID.kGraph){
			throw new RuntimeException("Expected 'Graph' type but is: " + parseAssignment.getLhs().getType().getTypeID());
		}

		ParseString var = parseAssignment.getLhs().getName();
		ParseExpression rhs = parseAssignment.getRhs();
		ParseAssignment.AssignmentType atype = parseAssignment.getAssignmentType();

		G resultGraph = null;
		if(atype == ParseAssignment.AssignmentType.kEqual){
			resultGraph = resolveGraphExpression(rhs, null, true);
		}else{
			String lhsGraphName = env.lookup(var.getValue());
			if(lhsGraphName == null){
				throw new RuntimeException(
						"Cannot resolve Graph variable " + var.getValue() +
						" at " + var.getLocationString());
			}
			G lhsGraph = allocateEmptyGraph(lhsGraphName);
			switch(atype){
				case kPlusEqual:{
					if(!env.IsBaseGraph(lhsGraph)){
						resultGraph = lhsGraph;
					}else{
						resultGraph = allocateEmptyGraph();
						addInstruction(newInstructionUnionGraph(resultGraph, lhsGraph));
					}
					resolveGraphExpression(rhs, resultGraph, true);
					break;
				}
				case kMinusEqual:{
					G rhsGraph = resolveGraphExpression(rhs, null, true);
					resultGraph = allocateEmptyGraph();
					addInstruction(newInstructionSubtractGraph(resultGraph, lhsGraph, rhsGraph, null));
					break;
				}
				case kIntersectEqual:{
					G rhsGraph = resolveGraphExpression(rhs, null, true);
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
		G distinctifiedGraph = allocateEmptyGraph();
		addInstruction(newInstructionDistinctifyGraph(distinctifiedGraph, resultGraph));
		env.addSymbol(var.getValue(), distinctifiedGraph.getName());
	}

	public void resolveGraphMetadataAssignment(ParseAssignment parseAssignment){
		logger.log(Level.SEVERE, "GraphMetadata operations are not supported.");
	}

	public void resolveCommand(ParseCommand parseCommand){
		ParseString cmdName = parseCommand.getCommandName();
		ArrayList<ParseExpression> arguments = parseCommand.getArguments();
		switch(cmdName.getValue().toLowerCase()){
			case "dump":
				resolveDumpCommand(arguments);
				break;
			case "visualize":
				resolveVisualizeCommand(arguments);
				break;
			case "stat":
				resolveStatCommand(arguments);
				break;
			case "list":
				resolveListCommand(arguments);
				break;
			case "reset":
				resolveResetCommand(arguments);
				break;
			case "erase":
				resolveEraseCommand(arguments);
				break;
			default:
				throw new RuntimeException(
						"Unsupported command \"" + cmdName.getValue() +
						"\" at " + cmdName.getLocationString());
		}
	}

	public void resolveDumpCommand(ArrayList<ParseExpression> arguments){
		if(arguments.isEmpty()){
			throw new RuntimeException(
					"Invalid number of arguments for dump: expected at least 1");
		}

		boolean force = false;
		int idx = 0;
		ParseExpression expression = arguments.get(idx);
		if(expression.getExpressionType() == ExpressionType.kName){
			String forceStr = ((ParseName) expression).getName().getValue();
			if(forceStr.equalsIgnoreCase("force")){
				force = true;
			}else{
				throw new RuntimeException("Invalid argument for dump: " + forceStr);
			}
			if(++idx >= arguments.size()){
				throw new RuntimeException("Invalid arguments for dump: expected 1 graph argument");
			}
			expression = arguments.get(idx);
		}

		G targetGraph = resolveGraphExpression(expression, null, true);
		addInstruction(newInstructionExportGraph(targetGraph, ExportFormat.kNormal, force));
	}

	public void resolveVisualizeCommand(ArrayList<ParseExpression> arguments){
		if(arguments.isEmpty()){
			throw new RuntimeException(
					"Invalid number of arguments for visualize: expected at least 1");
		}

		boolean force = false;
		int idx = 0;
		ParseExpression expression = arguments.get(idx);
		if(expression.getExpressionType() == ExpressionType.kName){
			String forceStr = ((ParseName) expression).getName().getValue();
			if(forceStr.equalsIgnoreCase("force")){
				force = true;
			}else{
				throw new RuntimeException("Invalid argument for visualize: " + forceStr);
			}
			expression = arguments.get(++idx);
		}

		G targetGraph = resolveGraphExpression(expression, null, true);
		addInstruction(newInstructionExportGraph(targetGraph, ExportFormat.kDot, force));
	}

	public void resolveStatCommand(ArrayList<ParseExpression> arguments){
		if(arguments.size() != 1){
			throw new RuntimeException("Invalid number of arguments for stat: expected 1");
		}

		G targetGraph = resolveGraphExpression(arguments.get(0), null, true);
		G distinctifiedGraph = allocateEmptyGraph();
		addInstruction(newInstructionDistinctifyGraph(distinctifiedGraph, targetGraph));
		addInstruction(newInstructionStatGraph(distinctifiedGraph));
	}

	public void resolveListCommand(ArrayList<ParseExpression> arguments){
		ExpressionStream stream = new ExpressionStream(arguments);
		String style = stream.tryGetNextNameAsString();
		if(style == null){
			style = "standard";
		}
		addInstruction(newInstructionListGraph(style));
	}

	public void resolveEraseCommand(ArrayList<ParseExpression> arguments){
		ArrayList<String> symbols = new ArrayList<>();
		for(ParseExpression argument : arguments){
			if(argument.getExpressionType() != ExpressionType.kVariable){
				throw new RuntimeException("Invalid arguments: expected variables");
			}
			ParseVariable var = (ParseVariable) argument;
			symbols.add(var.getName().getValue());
		}
		addInstruction(newInstructionEraseSymbols(symbols));
	}

	public void resolveResetCommand(ArrayList<ParseExpression> arguments){
		if(arguments.size() == 1){
			String target = resolveNameAsString(arguments.get(0));
			if(target.equals("workspace")){
				env.clear();
			}
		}
	}

	public Entity resolveExpression(ParseExpression parseExpression,
			Entity outputEntity,
			boolean isConstReference){
		switch(parseExpression.getExpressionType()){
			case kOperation:
				return resolveOperation((ParseOperation) parseExpression, outputEntity);
			case kVariable:
				return resolveVariable((ParseVariable) parseExpression, outputEntity, isConstReference);
			default:
				break;
		}
		throw new RuntimeException(
				"Unsupported expression type: " + parseExpression.getExpressionType().name());
	}

	public G resolveGraphExpression(ParseExpression parseExpression,
			G outputGraph,
			boolean isConstReference){
		return ToGraph(resolveExpression(parseExpression, outputGraph, isConstReference));
	}

	public GM resolveGraphMetadataExpression(ParseExpression parseExpression,
			GraphMetadata outputMetadata){
		logger.log(Level.SEVERE, "GraphMetadata operations are not supported.");
		return ToGraphMetadata(resolveExpression(parseExpression, outputMetadata, true));
	}

	public Entity resolveOperation(ParseOperation parseOperation,
			Entity outputEntity){
		ParseExpression parseSubject = parseOperation.getSubject();
		ParseString op = parseOperation.getOperator();
		ArrayList<ParseExpression> operands = parseOperation.getOperands();
		if(parseSubject != null){
			Entity subject = resolveExpression(parseSubject, null, true);
			switch(subject.getEntityType()){
				case kGraph:
					return resolveGraphMethod(ToGraph(subject), op, operands, outputEntity);
				case kGraphMetadata:
					return resolveGraphMetadataMethod(ToGraphMetadata(subject), op.getValue(), operands);
				default:
					throw new RuntimeException(
							"Invalid subject type " +
									subject.getEntityType().name().substring(1) + " at " +
									parseSubject.getLocationString());
			}
		}

		// Pure functions.
		switch(op.getValue()){
			case "+":{
				if(operands.size() != 2){
					throw new RuntimeException("Only '2' operands allowed for '"+op.getValue()+"' but found: '" + operands.size() + "'");
				}
				Entity lhsEntity = resolveExpression(operands.get(0), outputEntity, false);
				return resolveExpression(operands.get(1), lhsEntity, true);
			}
			case "&":  // Fall through
			case "-":{
				if(operands.size() != 2){
					throw new RuntimeException("Only '2' operands allowed for '"+op.getValue()+"' but found: '" + operands.size() + "'");
				}
				return resolveGraphBinaryOperation(op, operands.get(0), operands.get(1), ToGraph(outputEntity));
			}
			case "vertices": return resolveInsertLiteralVertex(operands, ToGraph(outputEntity));
			case "edges": return resolveInsertLiteralEdge(operands, ToGraph(outputEntity));
			case "asVertex": return resolveAsVertexOrEdge(Graph.GraphComponent.kVertex, operands, ToGraph(outputEntity));
			case "asEdge": return resolveAsVertexOrEdge(Graph.GraphComponent.kEdge, operands, ToGraph(outputEntity));
			default: break;
		}
		throw new RuntimeException(
				"Unsupported operation " + op.getValue() +
				" at " + op.getLocationString());
	}

	public G resolveGraphBinaryOperation(ParseString op,
			ParseExpression lhs,
			ParseExpression rhs,
			G outputGraph){
		G lhsGraph = resolveGraphExpression(lhs, null, true);
		G rhsGraph = resolveGraphExpression(rhs, null, true);

		if(outputGraph == null){
			outputGraph = allocateEmptyGraph();
		}

		switch(op.getValue()){
			case "&": addInstruction(newInstructionIntersectGraph(outputGraph, lhsGraph, rhsGraph)); break;
			case "-": addInstruction(newInstructionSubtractGraph(outputGraph, lhsGraph, rhsGraph, null)); break;
			default: throw new RuntimeException(
						"Unsupported graph binary operator " + op.getValue() +
						" at " + op.getLocationString());
		}
		return outputGraph;
	}

	public Entity resolveGraphMethod(G subject,
			ParseString methodName,
			ArrayList<ParseExpression> arguments,
			Entity outputEntity){
		switch(methodName.getValue()){
			case "getVertex": return resolveGetVertex(Graph.GraphComponent.kVertex, subject, arguments, ToGraph(outputEntity));
			case "getEdge": {
				G edges = resolveGetEdge(Graph.GraphComponent.kEdge, subject, arguments, ToGraph(outputEntity));
				G outputGraph = ToGraph(outputEntity);
				if(outputGraph == null){
					outputGraph = allocateEmptyGraph();
				}
				addInstruction(newInstructionGetEdgeEndpoint(outputGraph, edges, EdgeComponent.kBoth));
				addInstruction(newInstructionUnionGraph(outputGraph, edges));
				return outputGraph;
			}
			case "getLineage": return resolveGetLineage(subject, arguments, ToGraph(outputEntity));
			case "getLink": return resolveGetLink(subject, arguments, ToGraph(outputEntity));
			case "getPath": return resolveGetPath(subject, arguments, ToGraph(outputEntity));
			case "getShortestPath": return resolveGetShortestPath(subject, arguments, ToGraph(outputEntity));
			case "getSubgraph": return resolveGetSubgraph(subject, arguments, ToGraph(outputEntity));
			case "getEdgeSource": return resolveGetEdgeEndpoint(EdgeComponent.kSource, subject, arguments, ToGraph(outputEntity));
			case "getEdgeDestination": return resolveGetEdgeEndpoint(EdgeComponent.kDestination, subject, arguments, ToGraph(outputEntity));
			case "getEdgeEndpoints": return resolveGetEdgeEndpoint(EdgeComponent.kBoth, subject, arguments, ToGraph(outputEntity));
			case "collapseEdge": return resolveCollapseEdge(subject, arguments, ToGraph(outputEntity));
			case "attr": return resolveSetMetadata(GraphMetadataComponent.kBoth, subject, arguments, ToGraphMetadata(outputEntity));
			case "attrVertex": return resolveSetMetadata(GraphMetadataComponent.kVertex, subject, arguments, ToGraphMetadata(outputEntity));
			case "attrEdge": return resolveSetMetadata(GraphMetadataComponent.kEdge, subject, arguments, ToGraphMetadata(outputEntity));
			case "span": return resolveSpan(subject, arguments, ToGraph(outputEntity));
			case "limit": return resolveLimit(subject, arguments, ToGraph(outputEntity));
			default: break;
		}
		throw new RuntimeException(
				"Unsupported Graph method " + methodName.getValue() +
				" at " + methodName.getLocationString());
	}

	public Entity resolveGraphMetadataMethod(GM subject,
			String methodName,
			ArrayList<ParseExpression> arguments){
		logger.log(Level.SEVERE, "GraphMetadata operations are not supported.");
		throw new RuntimeException("No GraphMetadata method is supported yet");
	}

	public Entity resolveVariable(ParseVariable var,
			Entity outputEntity,
			boolean isConstReference){
		switch(var.getType().getTypeID()){
			case kGraph: return resolveGraphVariable(var, ToGraph(outputEntity), isConstReference);
			case kGraphMetadata: return resolveGraphMetadataVariable(var, ToGraphMetadata(outputEntity));
			default: break;
		}
		throw new RuntimeException(
				"Unsupported variable type " + var.getType().getName() +
				" at " + var.getLocationString());
	}

	public G resolveGraphVariable(ParseVariable var,
			G outputGraph,
			boolean isConstReference){
		if(var.getType().getTypeID() != TypeID.kGraph){
			throw new RuntimeException("Expected 'Graph' variable type but is: " + var.getType().getTypeID());
		}
		String varGraph = env.lookup(var.getName().getValue());
		if(varGraph == null){
			throw new RuntimeException("Cannot resolve Graph variable " + var.getName().getValue() +
					" at " + var.getLocationString());
		}
		if(outputGraph == null){
			if(isConstReference){
				return allocateEmptyGraph(varGraph);
			}
			outputGraph = allocateEmptyGraph();
		}
		instructions.add(newInstructionUnionGraph(outputGraph, allocateEmptyGraph(varGraph)));
		return outputGraph;
	}

	public GM resolveGraphMetadataVariable(ParseVariable var, GM lhsMetadata){
		logger.log(Level.SEVERE, "GraphMetadata operations are not supported.");
		if(var.getType().getTypeID() != TypeID.kGraphMetadata){
			throw new RuntimeException("Expected 'GraphMetadata' type but is: " + var.getType().getTypeID());
		}
		String varGraphMetadata = env.lookup(var.getName().getValue());
		if(varGraphMetadata == null){
			throw new RuntimeException(
					"Cannot resolve GraphMetadata variable " + var.getName().getValue() +
					" at " + var.getLocationString());
		}

		GM rhsMetadata = allocateEmptyGraphMetadata(varGraphMetadata);
		if(lhsMetadata == null){
			return rhsMetadata;
		}

		GM outputMetadata = allocateEmptyGraphMetadata();
		addInstruction(newInstructionOverwriteGraphMetadata(outputMetadata, lhsMetadata, rhsMetadata));
		return outputMetadata;
	}

	public G resolveInsertLiteralVertex(ArrayList<ParseExpression> operands,
			G outputGraph){
		if(outputGraph == null){
			outputGraph = allocateEmptyGraph();
		}
		ArrayList<String> vertices = new ArrayList<>();
		for(ParseExpression e : operands){
			if(e.getExpressionType() != ExpressionType.kLiteral){
				throw new RuntimeException(
						"Invalid argument at " + e.getLocationString() + ": expected integer literal");
			}
			TypedValue value = ((ParseLiteral) e).getLiteralValue();
			if(value.getType().getTypeID() != TypeID.kInteger){
				throw new RuntimeException(
						"Invalid argument type at " + e.getLocationString() + ": expected integer");
			}
			vertices.add(String.valueOf(value.getValue()));
		}
		addInstruction(newInstructionInsertLiteralVertex(outputGraph, vertices));
		return outputGraph;
	}

	public G resolveInsertLiteralEdge(ArrayList<ParseExpression> operands, G outputGraph){
		if(outputGraph == null){
			outputGraph = allocateEmptyGraph();
		}
		ArrayList<String> edges = new ArrayList<String>();
		for(ParseExpression e : operands){
			if(e.getExpressionType() != ExpressionType.kLiteral){
				throw new RuntimeException(
						"Invalid argument at " + e.getLocationString() + ": expected integer literal");
			}
			TypedValue value = ((ParseLiteral) e).getLiteralValue();
			if(value.getType().getTypeID() != TypeID.kInteger){
				throw new RuntimeException(
						"Invalid argument type at " + e.getLocationString() + ": expected integer");
			}
			edges.add(String.valueOf(value.getValue()));
		}
		addInstruction(newInstructionInsertLiteralEdge(outputGraph, edges));
		return outputGraph;
	}

	public G resolveGetEdge(Graph.GraphComponent component,
			G subjectGraph,
			ArrayList<ParseExpression> arguments,
			G outputGraph){
		if(arguments.size() > 1){
			throw new RuntimeException("Invalid number of arguments for GetEdge: expected 0 or 1");
		}

		if(arguments.isEmpty()){
			// Get all the edges.
			if(outputGraph == null){
				outputGraph = allocateEmptyGraph();
			}
			addInstruction(newInstructionGetEdge(outputGraph, subjectGraph, null, null, null));
			return outputGraph;
		}else{
			return resolveGetVertexOrEdgePredicate(component, subjectGraph, arguments.get(0), outputGraph);
		}
	}

	public G resolveGetVertex(Graph.GraphComponent component,
			G subjectGraph,
			ArrayList<ParseExpression> arguments,
			G outputGraph){
		if(arguments.size() > 1){
			throw new RuntimeException("Invalid number of arguments for GetVertex: expected 0 or 1");
		}

		if(arguments.isEmpty()){
			// Get all the vertices.
			if(outputGraph == null){
				outputGraph = allocateEmptyGraph();
			}
			addInstruction(newInstructionGetVertex(outputGraph, subjectGraph, null, null, null));
			return outputGraph;
		}else{
			return resolveGetVertexOrEdgePredicate(component, subjectGraph, arguments.get(0), outputGraph);
		}
	}

	public G resolveGetVertexOrEdgePredicate(GraphComponent component,
			G subjectGraph,
			ParseExpression expression,
			G outputGraph){
		if(expression.getExpressionType() != ExpressionType.kOperation){
			throw new RuntimeException(
					"Unexpected expression at " + expression.getLocationString());
		}
		ParseOperation predicate = (ParseOperation) expression;
		ParseString op = predicate.getOperator();
		ArrayList<ParseExpression> operands = predicate.getOperands();
		switch(op.getValue().toLowerCase()){
			case "or":{
				if(operands.size() != 2){
					throw new RuntimeException("Expected '2' operands for '"+op.getValue().toLowerCase()+"' but found: '" + operands.size() + "'");
				}
				for(int i = 0; i < 2; ++i){
					outputGraph = resolveGetVertexOrEdgePredicate(
							component, subjectGraph, operands.get(i), outputGraph);
				}
				return outputGraph;
			}
			case "and":{
				if(operands.size() != 2){
					throw new RuntimeException("Expected '2' operands for '"+op.getValue().toLowerCase()+"' but found: '" + operands.size() + "'");
				}
				G lhsGraph =
						resolveGetVertexOrEdgePredicate(component, subjectGraph, operands.get(0), null);
				G rhsGraph =
						resolveGetVertexOrEdgePredicate(component, lhsGraph, operands.get(1), outputGraph);
				return rhsGraph;
			}
			case "not":{
				if(operands.size() != 1){
					throw new RuntimeException("Expected '1' operand for '"+op.getValue().toLowerCase()+"' but found: '" + operands.size() + "'");
				}
				G subtrahendGraph = resolveGetVertexOrEdgePredicate(component, subjectGraph, operands.get(0), null);
				if(outputGraph == null){
					outputGraph = allocateEmptyGraph();
				}
				addInstruction(newInstructionSubtractGraph(outputGraph, subjectGraph, subtrahendGraph, component));
				return outputGraph;
			}
			default: break;
		}
		return resolveGetVertexOrEdgeComparison(component, subjectGraph, op, operands, outputGraph);
	}

	public G resolveGetVertexOrEdgeComparison(Graph.GraphComponent component,
			G subjectGraph,
			ParseString comparator,
			ArrayList<ParseExpression> operands,
			G outputGraph){
		final String cp = comparator.getValue().toLowerCase();
		String op;
		switch(cp){
			case "=":
			case "==":
				op = "=";
				break;
			case "<>":
			case "!=":
				op = "<>";
				break;
			case "<":
			case "<=":
			case ">":
			case ">=":
				op = cp;
				break;
			case "like":
				op = "LIKE";
				break;
			case "~":
			case "regexp":
				op = "REGEXP";
				break;
			default:
				throw new RuntimeException(
						"Unexpected comparator " + comparator.getValue() +
						" at " + comparator.getLocationString());
		}

		if(operands.size() != 2){
			throw new RuntimeException(
					"Invalid number of operands at " +
							comparator.getLocationString() + ": expected 2");
		}

		ParseExpression lhs = operands.get(0);
		ParseExpression rhs = operands.get(1);
		if(lhs.getExpressionType() != ExpressionType.kName){
			throw new RuntimeException("Unexpected operand at " + lhs.getLocationString());
		}
		if(rhs.getExpressionType() != ExpressionType.kLiteral){
			throw new RuntimeException("Unexpected operand at " + rhs.getLocationString());
		}
		String field = ((ParseName) lhs).getName().getValue();
		TypedValue literal = ((ParseLiteral) rhs).getLiteralValue();
		String value = literal.getType().printValueToString(literal.getValue());

		if(outputGraph == null){
			outputGraph = allocateEmptyGraph();
		}
		if(component == Graph.GraphComponent.kVertex){
			addInstruction(newInstructionGetVertex(outputGraph, subjectGraph, field, op, value));
		}else if(component == Graph.GraphComponent.kEdge){
			addInstruction(newInstructionGetEdge(outputGraph, subjectGraph, field, op, value));
		}
		return outputGraph;
	}

	public G resolveGetLineage(G subjectGraph,
			ArrayList<ParseExpression> arguments,
			G outputGraph){
		if(arguments.size() != 3){
			throw new RuntimeException("Invalid number of arguments for getLineage: expected 3");
		}

		G startGraph = resolveGraphExpression(arguments.get(0), null, true);
		Integer depth = resolveInteger(arguments.get(1));

		String dirStr = resolveString(arguments.get(2));
		Direction direction;
		if(dirStr.startsWith("a")){
			direction = Direction.kAncestor;
		}else if(dirStr.startsWith("d")){
			direction = Direction.kDescendant;
		}else{
			direction = Direction.kBoth;
		}

		if(outputGraph == null){
			outputGraph = allocateEmptyGraph();
		}

		addInstruction(newInstructionGetLineage(outputGraph, subjectGraph, startGraph, depth, direction));
		return outputGraph;
	}

	public G resolveGetLink(G subjectGraph,
			ArrayList<ParseExpression> arguments,
			G outputGraph){
		if(arguments.size() != 3){
			throw new RuntimeException(
					"Invalid number of arguments for getLink: expected 3");
		}

		G srcGraph = resolveGraphExpression(arguments.get(0), null, true);
		G dstGraph = resolveGraphExpression(arguments.get(1), null, true);
		Integer maxDepth = resolveInteger(arguments.get(2));

		if(outputGraph == null){
			outputGraph = allocateEmptyGraph();
		}

		addInstruction(newInstructionGetLink(outputGraph, subjectGraph, srcGraph, dstGraph, maxDepth));
		return outputGraph;
	}

	public G resolveGetPath(G subjectGraph,
			ArrayList<ParseExpression> arguments,
			G outputGraph){
		if(arguments.size() != 3){
			throw new RuntimeException("Invalid number of arguments for getPath: expected 3");
		}

		G srcGraph = resolveGraphExpression(arguments.get(0), null, true);
		G dstGraph = resolveGraphExpression(arguments.get(1), null, true);
		Integer maxDepth = resolveInteger(arguments.get(2));

		if(outputGraph == null){
			outputGraph = allocateEmptyGraph();
		}

		addInstruction(newInstructionGetPath(outputGraph, subjectGraph, srcGraph, dstGraph, maxDepth));
		return outputGraph;
	}

	public G resolveCollapseEdge(G subjectGraph,
			ArrayList<ParseExpression> arguments,
			G outputGraph){
		ArrayList<String> fields = new ArrayList<>();
		for(ParseExpression e : arguments){
			fields.add(resolveString(e));
		}

		if(outputGraph == null){
			outputGraph = allocateEmptyGraph();
		}

		addInstruction(newInstructionCollapseEdge(outputGraph, subjectGraph, fields));
		return outputGraph;
	}

	public G resolveGetSubgraph(G subjectGraph,
			ArrayList<ParseExpression> arguments,
			G outputGraph){
		if(arguments.size() != 1){
			throw new RuntimeException("Invalid number of arguments for getSubgraph: expected 1");
		}

		if(outputGraph == null){
			outputGraph = allocateEmptyGraph();
		}

		G skeletonGraph = resolveGraphExpression(arguments.get(0), null, true);
		addInstruction(newInstructionGetSubgraph(outputGraph, subjectGraph, skeletonGraph));
		return outputGraph;
	}

	public G resolveGetEdgeEndpoint(EdgeComponent component,
			G subjectGraph,
			ArrayList<ParseExpression> arguments,
			G outputGraph){
		if(!arguments.isEmpty()){
			throw new RuntimeException(
					"Invalid number of arguments at " +
							arguments.get(0).getLocationString() + ": expected 0");
		}

		if(outputGraph == null){
			outputGraph = allocateEmptyGraph();
		}

		addInstruction(newInstructionGetEdgeEndpoint(outputGraph, subjectGraph, component));
		return outputGraph;
	}

	public GM resolveSetMetadata(GraphMetadataComponent component,
			G subjectGraph,
			ArrayList<ParseExpression> arguments,
			GM outputMetadata){
		logger.log(Level.SEVERE, "GraphMetadata operations are not supported.");
		if(arguments.size() != 2){
			throw new RuntimeException("Invalid number of arguments for attr: expected 2");
		}

		String name = resolveString(arguments.get(0));
		String value = resolveString(arguments.get(1));

		GM metadata = allocateEmptyGraphMetadata();
		addInstruction(newInstructionSetGraphMetadata(metadata, component, subjectGraph, name, value));

		if(outputMetadata == null){
			return metadata;
		}else{
			GM combinedMetadata = allocateEmptyGraphMetadata();
			addInstruction(newInstructionOverwriteGraphMetadata(combinedMetadata, outputMetadata, metadata));
			return combinedMetadata;
		}
	}

	public G resolveSpan(G subjectGraph,
			ArrayList<ParseExpression> arguments,
			G outputGraph){
		if(arguments.size() != 1){
			throw new RuntimeException(
					"Invalid number of arguments for span: expected 1");
		}

		if(outputGraph == null){
			outputGraph = allocateEmptyGraph();
		}

		G sourceGraph = resolveGraphExpression(arguments.get(0), null, true);
		addInstruction(newInstructionGetSubgraph(outputGraph, sourceGraph, subjectGraph));
		return outputGraph;
	}

	public G resolveLimit(G subjectGraph,
			ArrayList<ParseExpression> arguments,
			G outputGraph){
		if(arguments.size() != 1){
			throw new RuntimeException(
					"Invalid number of arguments for limit: expected 1");
		}

		Integer limit = resolveInteger(arguments.get(0));

		if(outputGraph == null){
			outputGraph = allocateEmptyGraph();
		}

		addInstruction(newInstructionLimitGraph(outputGraph, subjectGraph, limit));
		return outputGraph;
	}

	public G resolveGetShortestPath(G subjectGraph,
			ArrayList<ParseExpression> arguments,
			G outputGraph){
		if(arguments.size() != 3){
			throw new RuntimeException(
					"Invalid number of arguments for getPath: expected 3");
		}

		G srcGraph = resolveGraphExpression(arguments.get(0), null, true);
		G dstGraph = resolveGraphExpression(arguments.get(1), null, true);
		Integer maxDepth = resolveInteger(arguments.get(2));

		if(outputGraph == null){
			outputGraph = allocateEmptyGraph();
		}

		addInstruction(newInstructionGetShortestPath(outputGraph, subjectGraph, srcGraph, dstGraph, maxDepth));
		return outputGraph;
	}

	public Integer resolveInteger(ParseExpression expression){
		if(expression.getExpressionType() != ExpressionType.kLiteral){
			throw new RuntimeException(
					"Invalid value at " + expression.getLocationString() +
					": expected integer literal");
		}
		TypedValue value = ((ParseLiteral) expression).getLiteralValue();
		if(value.getType().getTypeID() != TypeID.kInteger){
			throw new RuntimeException(
					"Invalid value type at " + expression.getLocationString() +
					": expected integer");
		}
		return (Integer) value.getValue();
	}

	public String resolveString(ParseExpression expression){
		if(expression.getExpressionType() != ExpressionType.kLiteral){
			throw new RuntimeException(
					"Invalid value at " + expression.getLocationString() +
					": expected string literal");
		}
		TypedValue value = ((ParseLiteral) expression).getLiteralValue();
		if(value.getType().getTypeID() != TypeID.kString){
			throw new RuntimeException(
					"Invalid value type at " + expression.getLocationString() +
					": expected string");
		}
		return (String) value.getValue();
	}

	public String resolveNameAsString(ParseExpression expression){
		if(expression.getExpressionType() != ExpressionType.kName){
			throw new RuntimeException(
					"Invalid value at " + expression.getLocationString() +
					": expected name");
		}
		return ((ParseName) expression).getName().getValue();
	}

	public G allocateEmptyGraph(){
		G graph = env.allocateGraph();
		addInstruction(newInstructionCreateEmptyGraph(graph));
		return graph;
	}

	public G allocateEmptyGraph(String graphName){
		return env.allocateGraph(graphName);
	}

	public GM allocateEmptyGraphMetadata(){
		logger.log(Level.SEVERE, "GraphMetadata operations are not supported.");
		GM metadata = env.allocateGraphMetadata();
		addInstruction(newInstructionCreateEmptyGraphMetadata(metadata));
		return metadata;
	}

	public GM allocateEmptyGraphMetadata(String graphName){
		return env.allocateGraphMetadata(graphName);
	}


	class ExpressionStream
	{
		private ArrayList<ParseExpression> stream;
		private int position = 0;

		public ExpressionStream(ArrayList<ParseExpression> stream)
		{
			this.stream = stream;
			this.position = 0;
		}

		public boolean hasNext()
		{
			return position < stream.size();
		}

		public ParseExpression getNextExpression()
		{
			if(!hasNext())
			{
				throw new RuntimeException("Require more arguments");
			}
			return stream.get(position++);
		}

		public String tryGetNextNameAsString()
		{
			if(!hasNext())
			{
				return null;
			}
			ParseExpression expression = stream.get(position);
			if(expression.getExpressionType() != ExpressionType.kName)
			{
				return null;
			}
			++position;
			return ((ParseName) expression).getName().getValue();
		}

		public String getNextString()
		{
			if(!hasNext())
			{
				throw new RuntimeException("Require more arguments");
			}
			return resolveString(stream.get(position++));
		}

		public String getNextNameOrString()
		{
			if(!hasNext())
			{
				throw new RuntimeException("Require more arguments");
			}
			ParseExpression expression = stream.get(position++);
			switch(expression.getExpressionType())
			{
			case kName:
				return resolveNameAsString(expression);
			case kLiteral:
				return resolveString(expression);
			default:
				break;
			}
			throw new RuntimeException("Expected name or string literal");
		}
	}

	// TODO
	public abstract G resolveAsVertexOrEdge(Graph.GraphComponent component,
			ArrayList<ParseExpression> arguments,
			G outputGraph);

	public final G ToGraph(Entity entity){
		if(entity == null){
			return null;
		}
		if(entity.getEntityType() != EntityType.kGraph){
			throw new RuntimeException(
					"Invalid casting from an instance of " +
							entity.getEntityType().name().substring(1) + " to Graph");
		}
		return castEntityToGraph(entity);
	}
	
	public final GM ToGraphMetadata(Entity entity){
		logger.log(Level.SEVERE, "GraphMetadata operations are not supported.");
		if(entity == null){
			return null;
		}
		if(entity.getEntityType() != EntityType.kGraphMetadata){
			throw new RuntimeException(
					"Invalid casting from an instance of " +
							entity.getEntityType().name().substring(1) + " to Graph");
		}
		return castEntityToGraphMetadata(entity);
	}

	/**
	 * Casts an Entity type object into quickgrail Graph object
	 * 
	 * @param entity either a graph or graph metadata (which is not supported). Argument never null.
	 * @return
	 */
	public abstract G castEntityToGraph(Entity entity);
	public abstract GM castEntityToGraphMetadata(Entity entity);
	
	public abstract AbstractCollapseEdge<G, GM, E, S> 
	newInstructionCollapseEdge(G targetGraph, G sourceGraph, ArrayList<String> fields);
	public abstract AbstractCreateEmptyGraph<G, GM, E, S> 
	newInstructionCreateEmptyGraph(G graph);
	public abstract AbstractCreateEmptyGraphMetadata<G, GM, E, S>
	newInstructionCreateEmptyGraphMetadata(GM metadata);
	public abstract AbstractDistinctifyGraph<G, GM, E, S>
	newInstructionDistinctifyGraph(G targetGraph, G sourceGraph);
	public abstract AbstractEraseSymbols<G, GM, E, S>
	newInstructionEraseSymbols(List<String> symbols);
	public abstract AbstractEvaluateQuery<G, GM, E, S>
	newInstructionEvaluateQuery(String query);
	public abstract AbstractExportGraph<G, GM, E, S>
	newInstructionExportGraph(G targetGraph, ExportFormat format, boolean force);
	public abstract AbstractGetEdge<G, GM, E, S>
	newInstructionGetEdge(G targetGraph, G subjectGraph, String field, String operation, String value);
	public abstract AbstractGetEdgeEndpoint<G, GM, E, S>
	newInstructionGetEdgeEndpoint(G targetGraph, G subjectGraph, EdgeComponent component);
	public abstract AbstractGetLineage<G, GM, E, S>
	newInstructionGetLineage(G targetGraph, G subjectGraph, G startGraph, Integer depth, Direction direction);
	public abstract AbstractGetLink<G, GM, E, S>
	newInstructionGetLink(G targetGraph, G subjectGraph, G sourceGraph, G destinationGraph, Integer maxDepth);
	public abstract AbstractGetPath<G, GM, E, S>
	newInstructionGetPath(G targetGraph, G subjectGraph, G srcGraph, G dstGraph, Integer maxDepth);
	public abstract AbstractGetShortestPath<G, GM, E, S>
	newInstructionGetShortestPath(G targetGraph, G subjectGraph, G srcGraph, G dstGraph, Integer maxDepth);
	public abstract AbstractGetSubgraph<G, GM, E, S>
	newInstructionGetSubgraph(G targetGraph, G subjectGraph, G skeletonGraph);
	public abstract AbstractGetVertex<G, GM, E, S>
	newInstructionGetVertex(G targetGraph, G subjectGraph, String field, String operation, String value);
	public abstract AbstractInsertLiteralEdge<G, GM, E, S>
	newInstructionInsertLiteralEdge(G targetGraph, ArrayList<String> edges);
	public abstract AbstractInsertLiteralVertex<G, GM, E, S>
	newInstructionInsertLiteralVertex(G targetGraph, ArrayList<String> vertices);
	public abstract AbstractIntersectGraph<G, GM, E, S>
	newInstructionIntersectGraph(G outputGraph, G lhsGraph, G rhsGraph);
	public abstract AbstractLimitGraph<G, GM, E, S>
	newInstructionLimitGraph(G targetGraph, G sourceGraph, int limit);
	public abstract AbstractListGraphs<G, GM, E, S>
	newInstructionListGraph(String style);
	public abstract AbstractOverwriteGraphMetadata<G, GM, E, S>
	newInstructionOverwriteGraphMetadata(GM targetMetadata, GM lhsMetadata, GM rhsMetadata);
	public abstract AbstractSetGraphMetadata<G, GM, E, S>
	newInstructionSetGraphMetadata(GM targetMetadata, GraphMetadataComponent component, G sourceGraph, String name, String value);
	public abstract AbstractStatGraph<G, GM, E, S>
	newInstructionStatGraph(G targetGraph);
	public abstract AbstractSubtractGraph<G, GM, E, S>
	newInstructionSubtractGraph(G outputGraph, G minuendGraph, G subtrahendGraph, GraphComponent component);
	public abstract AbstractUnionGraph<G, GM, E, S>
	newInstructionUnionGraph(G targetGraph, G sourceGraph);
}
