package spade.query.quickgrail.core.entities;

import java.util.ArrayList;

import spade.query.quickgrail.core.utility.TreeStringSerializable;

public abstract class Graph extends Entity
{
	// limits for exporting graph to dot and text files
	public static final int kNonForceVisualizeLimit = 1000;
	public static final int kNonForceDumpLimit = 100;

	public abstract String getName();

	public enum GraphComponent
	{
		kVertex,
		kEdge
	}

	public enum ExportFormat
	{
		kNormal,
		kDot
	}

	public enum EdgeComponent
	{
		kSource,
		kDestination,
		kBoth
	}

	public enum Direction
	{
		kAncestor,
		kDescendant,
		kBoth
	}
	
	@Override
	public final EntityType getEntityType(){
		return EntityType.kGraph;
	}
	
	@Override
	public final String getLabel(){
		return "Graph";
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
