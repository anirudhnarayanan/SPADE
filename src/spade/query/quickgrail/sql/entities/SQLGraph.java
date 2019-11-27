package spade.query.quickgrail.sql.entities;

import spade.query.quickgrail.core.entities.Graph;

public abstract class SQLGraph extends Graph{

	public abstract String getTableName(GraphComponent component);

	public abstract String getVertexTableName();

	public abstract String getEdgeTableName();
	
}
