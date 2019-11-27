package spade.query.quickgrail.neo4j.core;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;

public class CommonVariables
{
	public static final String EDGE_TABLE = "edge";
	public static final String VERTEX_TABLE = "vertex";
	public static final String PRIMARY_KEY = "hash";
	public static final String CHILD_VERTEX_KEY = "childVertexHash";
	public static final String PARENT_VERTEX_KEY = "parentVertexHash";
	// vertex alias to use while querying neo4j
	public static final String VERTEX_ALIAS = "v";
	// edge alias to use while querying neo4j
	public static final String EDGE_ALIAS = "e";

	public enum RelationshipTypes implements RelationshipType
	{
		EDGE
	}

	public enum NodeTypes implements Label
	{
		VERTEX
	}

	public enum Direction
	{
		kAncestor,
		kDescendant,
		kBoth
	}
}
