package spade.storage;

import java.util.ArrayList;
import java.util.List;

public class PostgreSQLSchema{

	public final String mainVertexTableName = SQL.VERTEX_TABLE,
			mainEdgeTableName = SQL.EDGE_TABLE,
			hashColumnName = SQL.PRIMARY_KEY,
			childVertexHashColumnName = SQL.CHILD_VERTEX_KEY,
			parentVertexHashColumnName = SQL.PARENT_VERTEX_KEY,
			symbolTableName = "symbols",
			symbolTableNameColumnName = "name";
	
	private final List<String> vertexColumnNames;
	private final List<String> edgeColumnNames;
	
	public PostgreSQLSchema(List<String> vertexColumnNames, List<String> edgeColumnNames){
		this.vertexColumnNames = vertexColumnNames;
		this.edgeColumnNames = edgeColumnNames;
	}
	
	public List<String> getVertexTableColumnNames(){
		return new ArrayList<String>(vertexColumnNames);
	}
	
	public List<String> getEdgeTableColumnNames(){
		return new ArrayList<String>(edgeColumnNames);
	}
	
	public String formatColumnNameForQuery(String columnName){
		return '"' + columnName + '"'; // wrapped in ""
	}
	
	public String formatTableNameForQuery(String tableName){
		return '"' + tableName + '"';// wrapped in ""
	}
	
	public String formatStringAsVarCharForQuery(String value){
		return "'" + value + "'"; // wrapped in ''
	}
	
	public String queryRemoveSymbol(String rawSymbolName){
		return "delete from " +formatTableNameForQuery(symbolTableName) + " WHERE " +
				formatColumnNameForQuery(symbolTableNameColumnName)+" = " +
				formatStringAsVarCharForQuery(rawSymbolName) + ";";
	}
	
	// not formatted name
	public String queryDropTableIfExists(String rawTableName){
		return "drop table if exists " + formatTableNameForQuery(rawTableName) + ";";
	}
	
	// not formatted name
	public String queryCreateHashOnlyTable(String rawTableName){
		return "create table " + formatTableNameForQuery(rawTableName) + " (" + formatColumnNameForQuery(hashColumnName) + " uuid);";
	}
	
	// not formatted name
	public String queryCreateHashOnlyVertexTable(String rawTableName){
		return "create table " + formatTableNameForQuery(rawTableName) + " (" + formatColumnNameForQuery(hashColumnName) + " uuid);";
	}
	
	// not formatted name
	public String queryCreateHashOnlyEdgeTable(String rawTableName){
		return "create table " + formatTableNameForQuery(rawTableName) + " (" + formatColumnNameForQuery(hashColumnName) + " uuid);";
	}
	
	public String queryGetRowCount(String rawTableName){
		return "select count(*) from " + formatTableNameForQuery(rawTableName) + ";";
	}
	
	public String queriesDropAndCreateHashOnlyTable(String rawTableName){
		return queryDropTableIfExists(rawTableName) + queryCreateHashOnlyTable(rawTableName);
	}
}
