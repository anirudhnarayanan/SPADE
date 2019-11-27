package spade.query.quickgrail.core.execution;

import java.util.ArrayList;
import java.util.List;

import spade.core.AbstractStorage;
import spade.query.quickgrail.core.entities.Graph;
import spade.query.quickgrail.core.entities.GraphMetadata;
import spade.query.quickgrail.core.kernel.AbstractEnvironment;
import spade.query.quickgrail.core.kernel.Instruction;
import spade.query.quickgrail.core.utility.TreeStringSerializable;

public abstract class AbstractEraseSymbols
	<
	G extends Graph, GM extends GraphMetadata,
	E extends AbstractEnvironment<G, GM, S>, S extends AbstractStorage
	>
	extends Instruction<G, GM, E, S>{
	
	private final List<String> symbols;

	public AbstractEraseSymbols(List<String> symbols){
		super();
		this.symbols = symbols;
	}
	
	public List<String> getSymbols(){
		return symbols;
	}
	
	@Override
	public final String getLabel(){
		return "EraseSymbols";
	}

	@Override
	public final void getFieldStringItems(
			ArrayList<String> inline_field_names,
			ArrayList<String> inline_field_values,
			ArrayList<String> non_container_child_field_names,
			ArrayList<TreeStringSerializable> non_container_child_fields,
			ArrayList<String> container_child_field_names,
			ArrayList<ArrayList<? extends TreeStringSerializable>> container_child_fields){
		inline_field_names.add("symbols");
		inline_field_values.add(String.join(",", this.symbols));
	}
}
