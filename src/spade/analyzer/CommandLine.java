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
package spade.analyzer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.logging.Level;
import java.util.logging.Logger;

import spade.core.AbstractAnalyzer;
import spade.core.AbstractStorage;
import spade.core.Kernel;
import spade.query.quickgrail.core.entities.Graph;
import spade.query.quickgrail.core.entities.GraphMetadata;
import spade.query.quickgrail.core.kernel.AbstractEnvironment;
import spade.query.quickgrail.core.kernel.QuickGrailExecutor;

public class CommandLine extends AbstractAnalyzer
{
	private static Logger logger = Logger.getLogger(CommandLine.class.getName());

	public CommandLine()
	{
		QUERY_PORT = "commandline_query_port";
	}

	private class SocketListener implements Runnable
	{
		private ServerSocket serverSocket;

		public SocketListener(ServerSocket serverSocket)
		{
			this.serverSocket = serverSocket;
		}

		@Override
		public void run()
		{
			try
			{
				while(!Kernel.isShutdown() && !SHUTDOWN)
				{
					Socket querySocket = serverSocket.accept();
					QueryConnection thisConnection = new QueryConnection(querySocket);
					Thread connectionThread = new Thread(thisConnection);
					connectionThread.start();
				}
			}
			catch(SocketException ex)
			{
				logger.log(Level.INFO, "Stopping socket listener");
			}
			catch(Exception ex)
			{
				logger.log(Level.SEVERE, null, ex);
			}
			finally
			{
				try
				{
					serverSocket.close();
					logger.log(Level.INFO, "Server socket closed");
				}
				catch(Exception ex)
				{
					logger.log(Level.SEVERE, "Unable to close server socket", ex);
				}
			}
		}
	}

	@Override
	public boolean initialize()
	{
		ServerSocket serverSocket = AbstractAnalyzer.getServerSocket(QUERY_PORT);
		if(serverSocket == null)
		{
			logger.log(Level.SEVERE, "Server Socket not initialized");
			return false;
		}
		new Thread(new SocketListener(serverSocket), "SocketListener-Thread").start();
		return true;
	}

	private class QueryConnection extends AbstractAnalyzer.QueryConnection{
		
		private QuickGrailExecutor<? extends Graph, ? extends GraphMetadata,
				? extends AbstractEnvironment<? extends Graph, ? extends GraphMetadata, ? extends AbstractStorage>,
				? extends AbstractStorage> executor = null;

		public QueryConnection(Socket socket){
			super(socket);
		}

		@Override
		public void run(){
			
			try{
				InputStream inStream = querySocket.getInputStream();
				OutputStream outStream = querySocket.getOutputStream();
				BufferedReader queryInputStream = new BufferedReader(new InputStreamReader(inStream));
				ObjectOutputStream responseOutputStream = new ObjectOutputStream(outStream);

				boolean exit = false;
				while(!exit && !SHUTDOWN)
				{
					exit = processRequest(queryInputStream, responseOutputStream);
				}

				queryInputStream.close();
				responseOutputStream.close();
				inStream.close();
				outStream.close();
			}catch(Exception ex){
				logger.log(Level.SEVERE, "Error processing request!", ex);
			}finally{
				try
				{
					querySocket.close();
				}
				catch(Exception ex)
				{
					logger.log(Level.SEVERE, "Unable to close query socket", ex);
				}
			}
		}

		private boolean safeWriteToClient(ObjectOutputStream outputStream, Object object){
			try{
				outputStream.writeObject(object);
				return true;
			}catch(Exception e){
				logger.log(Level.SEVERE, "Failed to write to client: '" + object + "'", e);
				return false;
			}
		}
		
		private QuickGrailExecutor<? extends Graph, ? extends GraphMetadata,
			? extends AbstractEnvironment<? extends Graph, ? extends GraphMetadata, ? extends AbstractStorage>,
			? extends AbstractStorage> 
			getExecutor(ObjectOutputStream outputStream, AbstractStorage storage){
			
			try{
				QuickGrailExecutor<? extends Graph, ? extends GraphMetadata,
						? extends AbstractEnvironment<? extends Graph, ? extends GraphMetadata, ? extends AbstractStorage>,
						? extends AbstractStorage> newExecutor = storage.getExecutor();
				if(newExecutor == null){
					logger.log(Level.SEVERE, "NULL executor for storage '"+storage.getClass().getName()+"' !!!");
					safeWriteToClient(outputStream, "Invalid implementation of storage '"+storage.getClass().getName()+"'");
				}
				return newExecutor;
			}catch(Exception e){
				logger.log(Level.SEVERE, "Failed to create executor for storage '"+storage.getClass().getName()+"'", e);
				safeWriteToClient(outputStream, "Failed to set storage '"+storage.getClass().getSimpleName()+"': " + 
						e.getMessage());
				return null;
			}
		}

		private void handleSetStorage(ObjectOutputStream outputStream, final String storageName){
			AbstractStorage newStorageToSet = Kernel.getStorageByName(storageName);
			if(newStorageToSet == null){
				safeWriteToClient(outputStream, "No '"+storageName+"' storage found");
			}else{
				QuickGrailExecutor<? extends Graph, ? extends GraphMetadata,
				? extends AbstractEnvironment<? extends Graph, ? extends GraphMetadata, ? extends AbstractStorage>,
				? extends AbstractStorage> newExecutor = null;
				
				if(executor != null){
					if(executor.isTheStorageSame(newStorageToSet)){
						safeWriteToClient(outputStream, "'"+storageName+"' already set as the current storage");
					}else{
						newExecutor = getExecutor(outputStream, newStorageToSet);
					}
				}else{
					newExecutor = getExecutor(outputStream, newStorageToSet);
				}
				
				if(newExecutor != null){
					if(this.executor != null){
						// TODO in case any cleanup needs to be done. Nothing at the moment
					}
					this.executor = newExecutor;
					safeWriteToClient(outputStream, "Successfully set storage '"+storageName+"'");
				}
			}
		}
		
		private void handleQuery(ObjectOutputStream outputStream, String query){
			if(executor == null){
				safeWriteToClient(outputStream, "No storage set for querying. " +
						"Use command: 'set storage <storage_name>'");
			}else{
				// Check if the current storage is shutdown or not
				if(executor.isStorageShutdown()){
					safeWriteToClient(outputStream, "Storage '"+executor.getStorageSimpleName()+"' "
							+ "has been shutdown. Use command: 'set storage <storage_name>'");
				}else{
					final String trimmedQuery = query.trim();
					// Finally can try and execute the query
					final String executableQuery = 
							trimmedQuery.toLowerCase().startsWith("export ") ?
									trimmedQuery.substring("export ".length()) :
									trimmedQuery;

					Object result = executor.execute(executableQuery);
					safeWriteToClient(outputStream, result);
				}
			}
		}
		
		private void handleRequest(ObjectOutputStream outputStream, final String query){
			if(query.trim().toLowerCase().startsWith("set ")){
				String tokens[] = query.split("\\s+", 3);
				if(tokens.length != 3){
					safeWriteToClient(outputStream, "Unexpected 'set' command. Expected 'set storage <storage_name>'");
				}else{
					if(!tokens[0].equalsIgnoreCase("set") || !tokens[1].equalsIgnoreCase("storage")){
						safeWriteToClient(outputStream, "Unexpected 'set' command. Expected 'set storage <storage_name>'");
					}else{
						final String storageName = tokens[2];
						handleSetStorage(outputStream, storageName);
					}
				}
			}else{
				handleQuery(outputStream, query);
			}
		}
		
		private boolean processRequest(BufferedReader inputStream,
				ObjectOutputStream outputStream) throws IOException{
			final String originalQuery = inputStream.readLine();
			if(originalQuery == null){ // End of stream
				return true; // exit
			}else{
				final String trimmedQuery = originalQuery.trim();
				if(trimmedQuery.equalsIgnoreCase("exit") || trimmedQuery.equalsIgnoreCase("quit")){
					return true; // exit
				}else{
					handleRequest(outputStream, originalQuery);
					return false;
				}
			}
		}

		@Override
		protected boolean parseQuery(String line){
			return true;
		}
	}
}
