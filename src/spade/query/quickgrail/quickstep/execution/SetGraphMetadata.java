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
package spade.query.quickgrail.quickstep.execution;

import static spade.query.quickgrail.core.entities.GraphMetadata.kDigits;

import spade.query.quickgrail.core.entities.GraphMetadata.GraphMetadataComponent;
import spade.query.quickgrail.core.execution.AbstractSetGraphMetadata;
import spade.query.quickgrail.core.kernel.ExecutionContext;
import spade.query.quickgrail.quickstep.core.QuickstepEnvironment;
import spade.query.quickgrail.quickstep.entities.QuickstepGraph;
import spade.query.quickgrail.quickstep.entities.QuickstepGraphMetadata;
import spade.storage.Quickstep;

/**
 * This class is not yet used in the SPADE integrated QuickGrail.
 */
public class SetGraphMetadata
	extends AbstractSetGraphMetadata<QuickstepGraph, QuickstepGraphMetadata, QuickstepEnvironment, Quickstep>{

	public SetGraphMetadata(QuickstepGraphMetadata targetMetadata,
							GraphMetadataComponent component,
							QuickstepGraph sourceGraph,
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
	public void execute(QuickstepEnvironment env, ExecutionContext ctx, Quickstep storage){
		String targetVertexTable = targetMetadata.getVertexTableName();
		String targetEdgeTable = targetMetadata.getEdgeTableName();
		String sourceVertexTable = sourceGraph.getVertexTableName();
		String sourceEdgeTable = sourceGraph.getEdgeTableName();

		storage.executeQuery("\\analyzerange " + sourceVertexTable + " " + sourceEdgeTable + "\n");

		if(component == GraphMetadataComponent.kVertex || component == GraphMetadataComponent.kBoth)
		{
			storage.executeQuery("INSERT INTO " + targetVertexTable +
					" SELECT id, " + FormatStringLiteral(name) + ", " + FormatStringLiteral(value) +
					" FROM " + sourceVertexTable + " GROUP BY id;");
		}

		if(component == GraphMetadataComponent.kEdge || component == GraphMetadataComponent.kBoth)
		{
			storage.executeQuery("INSERT INTO " + targetEdgeTable +
					" SELECT id, " + FormatStringLiteral(name) + ", " + FormatStringLiteral(value) +
					" FROM " + sourceEdgeTable + " GROUP BY id;");
		}
	}
}
