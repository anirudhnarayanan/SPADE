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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.exception.ExceptionUtils;

import spade.core.AbstractStorage;
import spade.core.Kernel;
import spade.query.quickgrail.core.entities.Graph;
import spade.query.quickgrail.core.entities.GraphMetadata;
import spade.query.quickgrail.core.parser.DSLParserWrapper;
import spade.query.quickgrail.core.parser.ParseProgram;

/**
 * Top level class for the QuickGrail graph query storageExecutor.
 */
public class QuickGrailExecutor<
	G extends Graph, GM extends GraphMetadata,
	E extends AbstractEnvironment<G, GM, S>, S extends AbstractStorage>{
	private final Logger logger = Logger.getLogger(this.getClass().getName());
	
	private final S storage;
	private final AbstractResolver<G, GM, E, S> resolver;
	private final E environment;
	
	public QuickGrailExecutor(S storage,
			AbstractResolver<G, GM, E, S> resolver,
			E environment){
		this.storage = storage;
		this.resolver = resolver;
		this.environment = environment;
		if(this.storage == null){
			throw new RuntimeException("NULL storage for query executor");
		}
		if(this.resolver == null){
			throw new RuntimeException("NULL resolver for query executor");
		}
		if(this.environment == null){
			throw new RuntimeException("NULL environment for query executor");
		}
	}
	
	public final boolean isTheStorageSame(AbstractStorage storage){
		return storage == this.storage; // Only reference check
	}
	
	public final boolean isStorageShutdown(){
		return !Kernel.isStoragePresentShallowCheck(storage);
	}
	
	public final String getStorageSimpleName(){
		return storage.getClass().getSimpleName();
	}

	public String execute(String query){
		ArrayList<Object> responses;
		try{
			DSLParserWrapper parserWrapper = new DSLParserWrapper();
			ParseProgram parseProgram = parserWrapper.fromText(query);

			logger.log(Level.INFO, "Parse tree:\n" + parseProgram.toString());

			QuickGrailProgram<G, GM, E, S> program = resolver.resolveProgram(parseProgram, environment);

			logger.log(Level.INFO, "Execution plan:\n" + program.toString());

			try{
				responses = program.execute(storage);
			}finally{
				environment.gc();
			}
		}catch(Exception ex){
			responses = new ArrayList<>();
			StringWriter stackTrace = new StringWriter();
			PrintWriter pw = new PrintWriter(stackTrace);
			pw.println("Error evaluating QuickGrail command:");
			pw.println("------------------------------------------------------------");
			logger.log(Level.SEVERE, ExceptionUtils.getStackTrace(ex));
			pw.println(ex.getMessage());
			pw.println("------------------------------------------------------------");
			responses.add(stackTrace.toString());
		}

		if(responses == null || responses.isEmpty()){
			return "OK";
		}else{
			// Currently only return the last response.
			Object response = responses.get(responses.size() - 1);
			return response == null ? "" : response.toString();
		}
	}
}
