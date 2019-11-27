package spade.query.quickgrail.core.execution;

import java.util.ArrayList;

import spade.core.AbstractStorage;
import spade.query.quickgrail.core.entities.Graph;
import spade.query.quickgrail.core.entities.GraphMetadata;
import spade.query.quickgrail.core.kernel.AbstractEnvironment;
import spade.query.quickgrail.core.kernel.Instruction;
import spade.query.quickgrail.core.utility.TreeStringSerializable;

public abstract class AbstractOverwriteGraphMetadata
	<
	G extends Graph, GM extends GraphMetadata,
	E extends AbstractEnvironment<G, GM, S>, S extends AbstractStorage
	>
	extends Instruction<G, GM, E, S>{
	
	public final GM targetMetadata;
	public final GM lhsMetadata;
	public final GM rhsMetadata;

	public AbstractOverwriteGraphMetadata(GM targetMetadata,
			GM lhsMetadata, GM rhsMetadata){
		super();
		this.targetMetadata = targetMetadata;
		this.lhsMetadata = lhsMetadata;
		this.rhsMetadata = rhsMetadata;
	}

	@Override
	public final String getLabel(){
		return "OverwritehGraphMetadata";
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
		inline_field_names.add("lhsMetadata");
		inline_field_values.add(lhsMetadata.getName());
		inline_field_names.add("rhsMetadata");
		inline_field_values.add(rhsMetadata.getName());
	}
}
