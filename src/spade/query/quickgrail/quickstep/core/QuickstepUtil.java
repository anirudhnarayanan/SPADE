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
package spade.query.quickgrail.quickstep.core;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import spade.query.quickgrail.quickstep.entities.QuickstepGraph;
import spade.query.quickgrail.quickstep.entities.QuickstepGraphMetadata;
import spade.storage.Quickstep;

/**
 * Convenient functions.
 */
public class QuickstepUtil
{
	private static Pattern tableNamePattern = Pattern.compile("([^ \n]+)[ |].*table.*");

	public static void CreateEmptyGraph(Quickstep storage, QuickstepGraph graph)
	{
		String vertexTable = graph.getVertexTableName();
		String edgeTable = graph.getEdgeTableName();

		StringBuilder sb = new StringBuilder();
		sb.append("DROP TABLE " + vertexTable + ";\n");
		sb.append("DROP TABLE " + edgeTable + ";\n");
		sb.append("CREATE TABLE " + vertexTable + " (id INT) " +
				"WITH BLOCKPROPERTIES (TYPE columnstore, SORT id, BLOCKSIZEMB 4);\n");
		sb.append("CREATE TABLE " + edgeTable + " (id LONG) " +
				"WITH BLOCKPROPERTIES (TYPE columnstore, SORT id, BLOCKSIZEMB 4);\n");
		storage.executeQuery(sb.toString());
	}

	public static void CreateEmptyGraphMetadata(Quickstep storage, QuickstepGraphMetadata metadata)
	{
		String vertexTable = metadata.getVertexTableName();
		String edgeTable = metadata.getEdgeTableName();

		StringBuilder sb = new StringBuilder();
		sb.append("DROP TABLE " + vertexTable + ";\n");
		sb.append("DROP TABLE " + edgeTable + ";\n");
		sb.append("CREATE TABLE " + vertexTable + " (id INT, name VARCHAR(64), value VARCHAR(256));");
		sb.append("CREATE TABLE " + edgeTable + " (id LONG, name VARCHAR(64), value VARCHAR(256));");
		storage.executeQuery(sb.toString());
	}

	public static ArrayList<String> GetAllTableNames(Quickstep storage)
	{
		ArrayList<String> tableNames = new ArrayList<>();
		Object objectOutput = storage.executeQuery("\\d\n");
		if(objectOutput == null){
			return tableNames;
		}else{
			Matcher matcher = tableNamePattern.matcher(objectOutput.toString());
			while(matcher.find())
			{
				tableNames.add(matcher.group(1));
			}
			return tableNames;
		}
	}

	public static long GetNumVertices(Quickstep storage, QuickstepGraph graph)
	{
		return storage.executeQueryForLongResult(
				"COPY SELECT COUNT(*) FROM " + graph.getVertexTableName() + " TO stdout;");
	}

	public static long GetNumEdges(Quickstep storage, QuickstepGraph graph)
	{
		return storage.executeQueryForLongResult(
				"COPY SELECT COUNT(*) FROM " + graph.getEdgeTableName() + " TO stdout;");
	}

	public static long GetNumTimestamps(Quickstep storage, QuickstepGraph graph)
	{
		return storage.executeQueryForLongResult(
				"COPY SELECT COUNT(*) FROM edge_anno" +
						" WHERE id IN (SELECT id FROM " + graph.getEdgeTableName() + ")" +
						" AND field = 'timestampNanos' TO stdout;");
	}

	public static Long[] GetTimestampRange(Quickstep storage, QuickstepGraph graph)
	{
		// TODO(jianqiao): Fix the return type problem in Quickstep.
		if(GetNumTimestamps(storage, graph) == 0)
		{
			return new Long[]{0L, 0L};
		}

		Object spanObject = storage.executeQuery(
				"COPY SELECT MIN(value), MAX(value) FROM edge_anno" +
						" WHERE id IN (SELECT id FROM " + graph.getEdgeTableName() + ")" +
						" AND field = 'timestampNanos' TO stdout WITH (DELIMITER '|');");
		String spanString = String.valueOf(spanObject);
		String[] timestamps = spanString.trim().split("\\|");
		return new Long[]{Long.parseLong(timestamps[0]),
				Long.parseLong(timestamps[1])};
	}

	public static String[] GetTimestampRangeString(Quickstep storage, QuickstepGraph graph)
	{
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("E, dd MMM yyyy HH:mm:ss z");
		Long[] span = spade.query.quickgrail.quickstep.core.QuickstepUtil.GetTimestampRange(storage, graph);
		String startDateStr = "";
		String endDateStr = "";
		if(span[0] != 0)
		{
			final ZonedDateTime startDate =
					ZonedDateTime.ofInstant(Instant.ofEpochMilli(span[0] / 1000000),
							ZoneId.systemDefault());
			startDateStr = startDate.format(formatter);
		}
		if(span[1] != 0)
		{
			final ZonedDateTime endDate =
					ZonedDateTime.ofInstant(Instant.ofEpochMilli(span[1] / 1000000),
							ZoneId.systemDefault());
			endDateStr = endDate.format(formatter);
		}
		return new String[]{startDateStr, endDateStr};
	}

	public static String formatString(String str)
	{
		StringBuilder sb = new StringBuilder();
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
		return (escaped ? "e" : "") + "'" + sb.toString() + "'";
	}
}
