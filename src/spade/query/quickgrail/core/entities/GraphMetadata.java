package spade.query.quickgrail.core.entities;

import java.util.ArrayList;

import spade.query.quickgrail.core.utility.TreeStringSerializable;

public abstract class GraphMetadata extends Entity
{
	public static final String kDigits = "0123456789ABCDEF";

	public abstract String getName();

	public enum GraphMetadataComponent
	{
		kVertex,
		kEdge,
		kBoth
	}
	
	@Override
	public final EntityType getEntityType(){
		return EntityType.kGraphMetadata;
	}
	
	@Override
	public final String getLabel(){
		return "GraphMetadata";
	}

	@Override
	public final void getFieldStringItems(
			ArrayList<String> inline_field_names,
			ArrayList<String> inline_field_values,
			ArrayList<String> non_container_child_field_names,
			ArrayList<TreeStringSerializable> non_container_child_fields,
			ArrayList<String> container_child_field_names,
			ArrayList<ArrayList<? extends TreeStringSerializable>> container_child_fields){
		inline_field_names.add("name");
		inline_field_values.add(getName());
	}
}
