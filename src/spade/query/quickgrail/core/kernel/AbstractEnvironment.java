package spade.query.quickgrail.core.kernel;

import java.util.ArrayList;
import java.util.Map;

import spade.core.AbstractStorage;
import spade.query.quickgrail.core.entities.Graph;
import spade.query.quickgrail.core.entities.GraphMetadata;
import spade.query.quickgrail.core.utility.TreeStringSerializable;

public abstract class AbstractEnvironment
	<
	G extends Graph,
	GM extends GraphMetadata,
	S extends AbstractStorage
	>  extends TreeStringSerializable{
	
	public final S storage;
	
	public AbstractEnvironment(S storage){
		this.storage = storage;
	}
	
	public abstract boolean IsBaseGraph(G graph);

	public abstract void clear();

	public abstract void gc();

	public abstract String lookup(String symbol);

	public abstract void eraseSymbol(String symbol);

	public abstract void addSymbol(String symbol, String value);

	public abstract Map<String, String> getSymbols();

	public abstract void setResultGraphName(String graphName);

	public abstract G allocateGraph();

	public abstract GM allocateGraphMetadata();

	public abstract G allocateGraph(String graphName);

	public abstract GM allocateGraphMetadata(String graphName);
	
	@Override
	public final String getLabel(){
		return "Environment";
	}

	@Override
	public final void getFieldStringItems(
			ArrayList<String> inline_field_names,
			ArrayList<String> inline_field_values,
			ArrayList<String> non_container_child_field_names,
			ArrayList<TreeStringSerializable> non_container_child_fields,
			ArrayList<String> container_child_field_names,
			ArrayList<ArrayList<? extends TreeStringSerializable>> container_child_fields){
		Map<String, String> symbols = getSymbols();
		if(symbols != null){
			for(Map.Entry<String, String> entry : symbols.entrySet()){
				inline_field_names.add(entry.getKey());
				inline_field_values.add(entry.getValue());
			}
		}
	}
}
