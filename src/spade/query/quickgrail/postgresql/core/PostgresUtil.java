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
package spade.query.quickgrail.postgresql.core;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import spade.query.quickgrail.postgresql.entities.PostgreSQLGraph;
import spade.query.quickgrail.postgresql.entities.PostgreSQLGraphMetadata;
import spade.storage.PostgreSQL;
import spade.storage.PostgreSQLSchema;

/**
 * Convenient functions.
 */
public class PostgresUtil
{
	private static final Logger logger = Logger.getLogger(PostgresUtil.class.getName());

	public static void CreateEmptyGraph(PostgreSQL storage, PostgreSQLGraph graph){
		PostgreSQLSchema schema = storage.getSchema();
		
		String vertexTable = graph.getVertexTableName();
		String edgeTable = graph.getEdgeTableName();

		final String dropVertexTableQuery = schema.queryDropTableIfExists(vertexTable);
		final String dropEdgeTableQuery = schema.queryDropTableIfExists(edgeTable);
		final String createVertexTableQuery = schema.queryCreateHashOnlyVertexTable(vertexTable);
		final String createEdgeTableQuery = schema.queryCreateHashOnlyEdgeTable(edgeTable);
		
		String queries = 
				dropVertexTableQuery + ";" +
				dropEdgeTableQuery + ";" +
				createVertexTableQuery + ";" +
				createEdgeTableQuery + ";";
		
		storage.executeQuery(queries);
	}

	// Note: GraphMetadata operations are not supported in SPADE anywhere
	public static void CreateEmptyGraphMetadata(PostgreSQL storage, PostgreSQLGraphMetadata metadata)
	{
		logger.log(Level.SEVERE, "GraphMetadata operations are not supported.");
		throw new UnsupportedOperationException("GraphMetadata operations are not supported.");
	}

	public static ArrayList<String> GetAllTableNames(PostgreSQL storage)
	{
		String output = storage.executeCopy("COPY (SELECT table_name " +
				"FROM information_schema.tables " +
				"WHERE table_type='BASE TABLE' " +
				"AND table_schema='public') TO stdout;");
		return new ArrayList<>(Arrays.asList(output.split("\n")));
	}

	public static long GetNumVertices(PostgreSQL storage, PostgreSQLGraph graph){
		return storage.getRowCountOfTableSafe(graph.getVertexTableName());
	}

	public static long GetNumEdges(PostgreSQL storage, PostgreSQLGraph graph){
		return storage.getRowCountOfTableSafe(graph.getEdgeTableName());
	}

	public static long GetNumTimestamps(PostgreSQL storage, PostgreSQLGraph graph)
	{
		logger.log(Level.SEVERE, "GetNumTimestamps is not supported.");
		throw new UnsupportedOperationException("GetNumTimestamps is not supported.");
	}

	public static Long[] GetTimestampRange(PostgreSQL storage, PostgreSQLGraph graph)
	{
		logger.log(Level.SEVERE, "GetTimestampRange is not supported.");
		throw new UnsupportedOperationException("GetTimestampRange is not supported.");
	}

	public static String formatString(String str, boolean field)
	{
		if(str == null)
			return str;
		StringBuilder sb = new StringBuilder(100);
		boolean escaped = false;
		for(int i = 0; i < str.length(); ++i)
		{
			char c = str.charAt(i);
			if(c < 32)
			{
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
					default:
						sb.append("\\x" + Integer.toHexString(c));
						break;
				}
				escaped = true;
			}
			else
			{
				if(c == '\\')
				{
					sb.append('\\');
					escaped = true;
				}
				sb.append(c);
			}
		}
		return (escaped ? "e" : "") + (field ? "\"" : "'") + sb.toString() + (field ? "\"" : "'");
	}
	
	public static List<Map<String, String>> sqlTableAsListOfKeyValuesMap(ResultSet resultSet)
		throws Exception{
		if(resultSet == null){
			return null;
		}else{
			List<Map<String, String>> listOfMaps = new ArrayList<Map<String, String>>();
			
			ResultSetMetaData metadata = resultSet.getMetaData();
			int columnCount = metadata.getColumnCount();
			Map<Integer, String> columnNames = new HashMap<>();
			for(int i = 1; i <= columnCount; i++){
				columnNames.put(i, metadata.getColumnName(i)); // Don't get label because that can be an alias
			}
			while(resultSet.next()){
				Map<String, String> annotations = new HashMap<String, String>();
				for(int i = 1; i <= columnCount; i++){
					String columnName = columnNames.get(i);
					Object objectColumnValue = resultSet.getObject(i);
					String stringColumnValue = null;
					if(!resultSet.wasNull()){
						stringColumnValue = objectColumnValue.toString();
					}
					annotations.put(columnName, stringColumnValue);
				}
				listOfMaps.add(annotations);
			}
			return listOfMaps;
		}
	}
}
