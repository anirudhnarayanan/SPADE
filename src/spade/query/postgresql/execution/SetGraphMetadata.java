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
package spade.query.postgresql.execution;

import spade.query.postgresql.entities.Graph;
import spade.query.postgresql.entities.GraphMetadata;
import spade.query.postgresql.kernel.Environment;
import spade.query.postgresql.utility.TreeStringSerializable;
import spade.storage.postgresql.PostgresExecutor;

import java.util.ArrayList;

import static spade.query.postgresql.utility.CommonVariables.PRIMARY_KEY;

/**
 * This class is not yet used in the SPADE integrated QuickGrail.
 */
public class SetGraphMetadata extends Instruction
{
	private static final String kDigits = "0123456789ABCDEF";
	private GraphMetadata targetMetadata;
	private Component component;
	private Graph sourceGraph;
	private String name;
	private String value;

	public SetGraphMetadata(GraphMetadata targetMetadata,
							Component component,
							Graph sourceGraph,
							String name,
							String value)
	{
		this.targetMetadata = targetMetadata;
		this.component = component;
		this.sourceGraph = sourceGraph;
		this.name = name;
		this.value = value;
	}

	private static String FormatStringLiteral(String input)
	{
		StringBuilder sb = new StringBuilder();
		sb.append("e'");
		for(int i = 0; i < input.length(); ++i)
		{
			char c = input.charAt(i);
			if(c >= 32)
			{
				if(c == '\\' || c == '\'')
				{
					sb.append(c);
				}
				sb.append(c);
				continue;
			}
			switch(c)
			{
				case '\b':
					sb.append("\\b");
					break;
				case '\n':
					sb.append("\\n");
					break;
				case '\r':
					sb.append("\\r");
					break;
				case '\t':
					sb.append("\\t");
					break;
				case '\\':
					sb.append("\\\\");
					break;
				default:
					// Use hexidecimal representation.
					sb.append("\\x");
					sb.append(kDigits.charAt(c >> 4));
					sb.append(kDigits.charAt(c & 0xF));
					break;
			}
		}
		sb.append("'");
		return sb.toString();
	}

	@Override
	public void execute(Environment env, ExecutionContext ctx)
	{
		PostgresExecutor qs = ctx.getExecutor();

		String targetVertexTable = targetMetadata.getVertexTableName();
		String targetEdgeTable = targetMetadata.getEdgeTableName();
		String sourceVertexTable = sourceGraph.getVertexTableName();
		String sourceEdgeTable = sourceGraph.getEdgeTableName();

		if(component == Component.kVertex || component == Component.kBoth)
		{
			qs.executeQuery("INSERT INTO " + targetVertexTable +
					" SELECT  " + PRIMARY_KEY + ", " + FormatStringLiteral(name) + ", " + FormatStringLiteral(value) +
					" FROM " + sourceVertexTable + " GROUP BY " + PRIMARY_KEY + ";");
		}

		if(component == Component.kEdge || component == Component.kBoth)
		{
			qs.executeQuery("INSERT INTO " + targetEdgeTable +
					" SELECT " + PRIMARY_KEY + ", " + FormatStringLiteral(name) + ", " + FormatStringLiteral(value) +
					" FROM " + sourceEdgeTable + " GROUP BY " + PRIMARY_KEY + ";");
		}
	}

	@Override
	public String getLabel()
	{
		return "SetGraphMetadata";
	}

	@Override
	protected void getFieldStringItems(
			ArrayList<String> inline_field_names,
			ArrayList<String> inline_field_values,
			ArrayList<String> non_container_child_field_names,
			ArrayList<TreeStringSerializable> non_container_child_fields,
			ArrayList<String> container_child_field_names,
			ArrayList<ArrayList<? extends TreeStringSerializable>> container_child_fields)
	{
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

	public enum Component
	{
		kVertex,
		kEdge,
		kBoth
	}
}
