package spade.query.quickgrail.core.execution;

import java.util.ArrayList;

import spade.core.AbstractStorage;
import spade.query.quickgrail.core.entities.Graph;
import spade.query.quickgrail.core.entities.GraphMetadata;
import spade.query.quickgrail.core.entities.GraphMetadata.GraphMetadataComponent;
import spade.query.quickgrail.core.kernel.AbstractEnvironment;
import spade.query.quickgrail.core.kernel.Instruction;
import spade.query.quickgrail.core.utility.TreeStringSerializable;

public abstract class AbstractSetGraphMetadata
	<
	G extends Graph, GM extends GraphMetadata,
	E extends AbstractEnvironment<G, GM, S>, S extends AbstractStorage
	>
	extends Instruction<G, GM, E, S>{
	
	public final GM targetMetadata;
	public final GraphMetadataComponent component;
	public final G sourceGraph;
	public final String name;
	public final String value;

	public AbstractSetGraphMetadata(GM targetMetadata, GraphMetadataComponent component,
								  G sourceGraph, String name, String value){
		super();
		this.targetMetadata = targetMetadata;
		this.component = component;
		this.sourceGraph = sourceGraph;
		this.name = name;
		this.value = value;
	}

	@Override
	public final String getLabel(){
		return "SetGraphMetadata";
	}

	@Override
	public final void getFieldStringItems(
			ArrayList<String> inline_field_names,
			ArrayList<String> inline_field_values,
			ArrayList<String> non_container_child_field_names,
			ArrayList<TreeStringSerializable> non_container_child_fields,
			ArrayList<String> container_child_field_names,
			ArrayList<ArrayList<? extends TreeStringSerializable>> container_child_fields){
		inline_field_names.add("targetMetadata");
		inline_field_values.add(targetMetadata.getName());
		inline_field_names.add("component");
		inline_field_values.add(component.name().substring(1));
		inline_field_names.add("sourceGraph");
		inline_field_values.add(sourceGraph.getName());
		inline_field_names.add("name");
		inline_field_values.add(name);
		inline_field_names.add("value");
		inline_field_values.add(value);
	}
	
}
