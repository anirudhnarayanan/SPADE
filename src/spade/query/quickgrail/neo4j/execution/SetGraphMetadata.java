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
package spade.query.quickgrail.neo4j.execution;

import spade.query.quickgrail.core.entities.GraphMetadata.GraphMetadataComponent;
import spade.query.quickgrail.core.execution.AbstractSetGraphMetadata;
import spade.query.quickgrail.core.kernel.ExecutionContext;
import spade.query.quickgrail.neo4j.core.Neo4jEnvironment;
import spade.query.quickgrail.neo4j.entities.Neo4jGraph;
import spade.query.quickgrail.neo4j.entities.Neo4jGraphMetadata;
import spade.storage.Neo4j;

/**
 * This class is not yet used in the SPADE integrated QuickGrail.
 */
public class SetGraphMetadata
	extends AbstractSetGraphMetadata<Neo4jGraph, Neo4jGraphMetadata, Neo4jEnvironment, Neo4j>{
	
	private static final String kDigits = "0123456789ABCDEF";

	public SetGraphMetadata(Neo4jGraphMetadata targetMetadata,
							GraphMetadataComponent component,
							Neo4jGraph sourceGraph,
							String name,
							String value){
		super(targetMetadata, component, sourceGraph, name, value);
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
	public void execute(Neo4jEnvironment env, ExecutionContext ctx, Neo4j storage){
	}

}
