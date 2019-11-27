package spade.query.quickgrail.core.execution;

import java.util.ArrayList;

import spade.core.AbstractStorage;
import spade.query.quickgrail.core.entities.Graph;
import spade.query.quickgrail.core.entities.GraphMetadata;
import spade.query.quickgrail.core.kernel.AbstractEnvironment;
import spade.query.quickgrail.core.kernel.Instruction;
import spade.query.quickgrail.core.utility.TreeStringSerializable;

public abstract class AbstractGetShortestPath
	<
	G extends Graph, GM extends GraphMetadata,
	E extends AbstractEnvironment<G, GM, S>, S extends AbstractStorage
	>
	extends Instruction<G, GM, E, S>{
	
	public final G targetGraph;
	public final G subjectGraph;
	public final G sourceGraph;
	public final G destinationGraph;
	public final Integer maxDepth;

	public AbstractGetShortestPath(G targetGraph, G subjectGraph,
								 G srcGraph, G dstGraph,
								 Integer maxDepth){
		super();
		this.targetGraph = targetGraph;
		this.subjectGraph = subjectGraph;
		this.sourceGraph = srcGraph;
		this.destinationGraph = dstGraph;
		this.maxDepth = maxDepth;
	}
	
	@Override
	public final String getLabel()
	{
		return "GetShortestPath";
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
		inline_field_names.add("subjectGraph");
		inline_field_values.add(subjectGraph.getName());
		inline_field_names.add("sourceGraph");
		inline_field_values.add(sourceGraph.getName());
		inline_field_names.add("destinationGraph");
		inline_field_values.add(destinationGraph.getName());
		inline_field_names.add("maxDepth");
		inline_field_values.add(String.valueOf(maxDepth));
	}
}
