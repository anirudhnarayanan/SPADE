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
package spade.query.quickgrail.core.kernel;

import java.util.ArrayList;

import spade.core.AbstractStorage;
import spade.query.quickgrail.core.entities.Graph;
import spade.query.quickgrail.core.entities.GraphMetadata;
import spade.query.quickgrail.core.utility.TreeStringSerializable;

/**
 * A compiled QuickGrail program that is composed of a list of primitive
 * instructions.
 */
public class QuickGrailProgram
	<G extends Graph, GM extends GraphMetadata,
	E extends AbstractEnvironment<G, GM, S>, S extends AbstractStorage>
	extends TreeStringSerializable{
	
	private ArrayList<Instruction<G, GM, E, S>> instructions;
	private E environment;

	public QuickGrailProgram(
			ArrayList<Instruction<G, GM, E, S>> instructions, E environment){
		this.instructions = instructions;
		this.environment = environment;
	}

	public ArrayList<Object> execute(S storage){
		ExecutionContext ctx = new ExecutionContext();
		for(Instruction<G, GM, E, S> instruction : instructions){
			instruction.execute(environment, ctx, storage);
		}
		return ctx.getResponses();
	}

	@Override
	public final String getLabel(){
		return "Program";
	}

	@Override
	public final void getFieldStringItems(
			ArrayList<String> inline_field_names,
			ArrayList<String> inline_field_values,
			ArrayList<String> non_container_child_field_names,
			ArrayList<TreeStringSerializable> non_container_child_fields,
			ArrayList<String> container_child_field_names,
			ArrayList<ArrayList<? extends TreeStringSerializable>> container_child_fields){
		container_child_field_names.add("instructions");
		container_child_fields.add(instructions);
	}
}
