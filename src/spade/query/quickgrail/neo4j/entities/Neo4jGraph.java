/*
 --------------------------------------------------------------------------------
 SPADE - Support for Provenance Auditing in Distributed Environments.
 Copyright (C) 2018 SRI International

 This program is free software: you can redistribute it and/or
 modify it under the terms of the GNU General Public License as
 published by the Free Software Foundation, either version 3 of the
 License, or (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program. If not, see <http://www.gnu.org/licenses/>.
 --------------------------------------------------------------------------------
 */
package spade.query.quickgrail.neo4j.entities;

import static spade.query.quickgrail.neo4j.core.Neo4jStorageHelper.removeDollar;

import spade.query.quickgrail.core.entities.Graph;
import spade.query.quickgrail.neo4j.core.CommonVariables;

/**
 * Intermediate representation for a graph in QuickGrail optimizer.
 */
public class Neo4jGraph extends Graph
{
	private String name;

	public Neo4jGraph(String name)
	{
		this.name = name;
	}

	public static String GetBaseVertexTableName()
	{
		return CommonVariables.NodeTypes.VERTEX.toString();
	}

	public static String GetBaseVertexAnnotationTableName()
	{
		return CommonVariables.NodeTypes.VERTEX.toString();
	}

	public static String GetBaseEdgeTableName()
	{
		return CommonVariables.RelationshipTypes.EDGE.toString();
	}

	public static String GetBaseEdgeAnnotationTableName()
	{
		return CommonVariables.RelationshipTypes.EDGE.toString();
	}

	public static String GetBaseTableName(GraphComponent component)
	{
		return component == GraphComponent.kVertex ? GetBaseVertexTableName() : GetBaseEdgeTableName();
	}

	public static String GetBaseAnnotationTableName(GraphComponent component)
	{
		return component == GraphComponent.kVertex
				? GetBaseVertexAnnotationTableName() :
				GetBaseEdgeAnnotationTableName();
	}

	@Override
	public String getName()
	{
		return name;
	}

	public void setName(String name)
	{
		this.name = name;
	}

//	@Override
	public String getVertexTableName()
	{
		if(name.equals("$base"))
			return CommonVariables.NodeTypes.VERTEX.toString();
		return removeDollar(name);
	}

//	@Override
	public String getEdgeTableName()
	{
		if(name.equals("$base"))
			return CommonVariables.RelationshipTypes.EDGE.toString();
		return removeDollar(name);
	}

//	@Override
	public String getTableName(GraphComponent component)
	{
		return component == GraphComponent.kVertex ? getVertexTableName() : getEdgeTableName();
	}


	
}
