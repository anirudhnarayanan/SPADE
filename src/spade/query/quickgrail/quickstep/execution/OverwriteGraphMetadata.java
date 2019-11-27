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

import spade.query.quickgrail.core.execution.AbstractOverwriteGraphMetadata;
import spade.query.quickgrail.core.kernel.ExecutionContext;
import spade.query.quickgrail.quickstep.core.QuickstepEnvironment;
import spade.query.quickgrail.quickstep.entities.QuickstepGraph;
import spade.query.quickgrail.quickstep.entities.QuickstepGraphMetadata;
import spade.storage.Quickstep;

/**
 * This class is not yet used in the SPADE integrated QuickGrail.
 */
public class OverwriteGraphMetadata
	extends AbstractOverwriteGraphMetadata<QuickstepGraph, QuickstepGraphMetadata, QuickstepEnvironment, Quickstep>{

	public OverwriteGraphMetadata(QuickstepGraphMetadata targetMetadata,
								  QuickstepGraphMetadata lhsMetadata,
								  QuickstepGraphMetadata rhsMetadata){
		super(targetMetadata, lhsMetadata, rhsMetadata);
	}

	@Override
	public void execute(QuickstepEnvironment env, ExecutionContext ctx, Quickstep storage){
		String targetVertexTable = targetMetadata.getVertexTableName();
		String targetEdgeTable = targetMetadata.getEdgeTableName();
		String lhsVertexTable = lhsMetadata.getVertexTableName();
		String lhsEdgeTable = lhsMetadata.getEdgeTableName();
		String rhsVertexTable = rhsMetadata.getVertexTableName();
		String rhsEdgeTable = rhsMetadata.getEdgeTableName();

		storage.executeQuery("\\analyzerange " + rhsVertexTable + " " + rhsEdgeTable + "\n" +
				"INSERT INTO " + targetVertexTable +
				" SELECT id, name, value FROM " + lhsVertexTable + " l" +
				" WHERE NOT EXISTS (SELECT * FROM " + rhsVertexTable + " r" +
				" WHERE l.id = r.id AND l.name = r.name);\n" +
				"INSERT INTO " + targetEdgeTable +
				" SELECT id, name, value FROM " + lhsEdgeTable + " l" +
				" WHERE NOT EXISTS (SELECT * FROM " + rhsEdgeTable + " r" +
				" WHERE l.id = r.id AND l.name = r.name);\n" +
				"INSERT INTO " + targetVertexTable +
				" SELECT id, name, value FROM " + rhsVertexTable + ";\n" +
				"INSERT INTO " + targetEdgeTable +
				" SELECT id, name, value FROM " + rhsEdgeTable + ";");
	}
}
