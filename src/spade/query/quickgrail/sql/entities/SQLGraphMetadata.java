package spade.query.quickgrail.sql.entities;

import spade.query.quickgrail.core.entities.GraphMetadata;

public abstract class SQLGraphMetadata extends GraphMetadata{
	
	public abstract String getVertexTableName();

	public abstract String getEdgeTableName();
	
}
