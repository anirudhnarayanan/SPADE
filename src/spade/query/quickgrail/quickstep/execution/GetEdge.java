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

import static spade.query.quickgrail.quickstep.core.QuickstepUtil.formatString;

import spade.query.quickgrail.core.execution.AbstractGetEdge;
import spade.query.quickgrail.core.kernel.ExecutionContext;
import spade.query.quickgrail.quickstep.core.QuickstepEnvironment;
import spade.query.quickgrail.quickstep.entities.QuickstepGraph;
import spade.query.quickgrail.quickstep.entities.QuickstepGraphMetadata;
import spade.storage.Quickstep;

/**
 * Get the a set of edges in a graph.
 */
public class GetEdge
	extends AbstractGetEdge<QuickstepGraph, QuickstepGraphMetadata, QuickstepEnvironment, Quickstep>{

	public GetEdge(QuickstepGraph targetGraph, QuickstepGraph subjectGraph, 
			String field, String operation, String value){
		super(targetGraph, subjectGraph, field, operation, value);
	}

	@Override
	public void execute(QuickstepEnvironment env, ExecutionContext ctx, Quickstep storage){
		StringBuilder sqlQuery = new StringBuilder(100);
		sqlQuery.append("INSERT INTO " + targetGraph.getEdgeTableName() +
				" SELECT id FROM " + QuickstepGraph.GetBaseEdgeAnnotationTableName());
		if(field != null)
		{
			sqlQuery.append(" WHERE");
			if(!field.equals("*"))
			{
				sqlQuery.append(" field = " + formatString(field) + " AND");
			}
			sqlQuery.append(" value " + operation + " " + formatString(value));
			if(!env.IsBaseGraph(subjectGraph))
			{
				sqlQuery.append("\\analyzerange " + subjectGraph.getEdgeTableName() + "\n");
				sqlQuery.append(" AND id IN (SELECT id FROM " +
						subjectGraph.getEdgeTableName() + ")");
			}
		}
		sqlQuery.append(" GROUP BY id;");
		storage.executeQuery(sqlQuery.toString());
	}
}