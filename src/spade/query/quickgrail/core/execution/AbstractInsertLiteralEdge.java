package spade.query.quickgrail.core.execution;

import java.util.ArrayList;

import spade.core.AbstractStorage;
import spade.query.quickgrail.core.entities.Graph;
import spade.query.quickgrail.core.entities.GraphMetadata;
import spade.query.quickgrail.core.kernel.AbstractEnvironment;
import spade.query.quickgrail.core.kernel.Instruction;
import spade.query.quickgrail.core.utility.TreeStringSerializable;

public abstract class AbstractInsertLiteralEdge
	<
	G extends Graph, GM extends GraphMetadata,
	E extends AbstractEnvironment<G, GM, S>, S extends AbstractStorage
	>
	extends Instruction<G, GM, E, S>{
	
	public final G targetGraph;
	private final ArrayList<String> edges;

	public AbstractInsertLiteralEdge(G targetGraph, ArrayList<String> edges){
		super();
		this.targetGraph = targetGraph;
		this.edges = edges;
	}
	
	public ArrayList<String> getEdges(){
		return edges;
	}
	
	@Override
	public final String getLabel(){
		return "InsertLiteralEdge";
	}

	@Override
	public final void getFieldStringItems(
			ArrayList<String> inline_field_names,
			ArrayList<String> inline_field_values,
			ArrayList<String> non_container_child_field_names,
			ArrayList<TreeStringSerializable> non_container_child_fields,
			ArrayList<String> container_child_field_names,
			ArrayList<ArrayList<? extends TreeStringSerializable>> container_child_fields){
		inline_field_names.add("targetGraph");
		inline_field_values.add(targetGraph.getName());
		inline_field_names.add("edges");
		inline_field_values.add("{" + String.join(",", edges) + "}");
	}
}
