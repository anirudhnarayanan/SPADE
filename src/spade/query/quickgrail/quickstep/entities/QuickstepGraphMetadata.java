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
package spade.query.quickgrail.quickstep.entities;

import spade.query.quickgrail.sql.entities.SQLGraphMetadata;

/**
 * This class is not yet used in the SPADE integrated QuickGrail.
 */
public class QuickstepGraphMetadata extends SQLGraphMetadata
{
	private String name;

	public QuickstepGraphMetadata(String name)
	{
		this.name = name;
	}

	@Override
	public String getName()
	{
		return name;
	}

	@Override
	public String getVertexTableName()
	{
		return name + "_vertex";
	}

	@Override
	public String getEdgeTableName()
	{
		return name + "_edge";
	}

}
