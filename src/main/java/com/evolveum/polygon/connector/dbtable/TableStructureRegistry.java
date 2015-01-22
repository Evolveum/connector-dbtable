/**
 * Copyright (c) 2015 Evolveum
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.evolveum.polygon.connector.dbtable;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.identityconnectors.framework.common.exceptions.ConfigurationException;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.objects.AttributeInfoBuilder;
import org.identityconnectors.framework.common.objects.Name;
import org.identityconnectors.framework.common.objects.ObjectClassInfoBuilder;
import org.identityconnectors.framework.common.objects.OperationalAttributes;
import org.identityconnectors.framework.common.objects.Schema;
import org.identityconnectors.framework.common.objects.SchemaBuilder;

/**
 * @author Radovan Semancik
 *
 */
public class TableStructureRegistry {

	private static final Map<Integer,Class> TYPE_MAP_SQL = new HashMap<Integer, Class>();
	private static final Map<Class,Integer> TYPE_MAP_ICF = new HashMap<Class,Integer>();
	
	private DbTableConfiguration configuration;
	private Map<String,TableStructure> tableStructureMap = new HashMap<String,TableStructure>(); 
	
	public Schema parse(DbTableConnection connection) {
		String[] tableNames = configuration.getTable();
		String[] columnDefStrings = configuration.getColumn();
		DatabaseMetaData metadata = connection.getMetaData();
		SchemaBuilder schemaBuilder = new SchemaBuilder(DbTableConnector.class);
		for (String tableName: tableNames) {
			TableStructure tstruct = new TableStructure();
			for (String columnDefString: columnDefStrings) {
				String[] columnDef = columnDefString.split(":");
				if (columnDef.length != 3) {
					throw new ConfigurationException("Wrong format of the 'column' configuration property, line "+columnDefString);
				}
				if (tableName.equals(columnDef[0])) {
					if (DbTableConfiguration.COLUMN_TAG_UID.equals(columnDef[2])) {
						if (tstruct.getUidColumn() != null) {
							throw new ConfigurationException("More than one column defined as UID for table "+tableName);
						}
						tstruct.setUidColumn(columnDef[1]);
					} else if (DbTableConfiguration.COLUMN_TAG_NAME.equals(columnDef[2])) {
						if (tstruct.getNameColumn() != null) {
							throw new ConfigurationException("More than one column defined as NAME for table "+tableName);
						}
						tstruct.setNameColumn(columnDef[1]);
					} else if (DbTableConfiguration.COLUMN_TAG_PASSWORD.equals(columnDef[2])) {
						if (tstruct.getPasswordColumn() != null) {
							throw new ConfigurationException("More than one column defined as password for table "+tableName);
						}
						tstruct.setPasswordColumn(columnDef[1]);
					} else if (DbTableConfiguration.COLUMN_TAG_CHANGETIME.equals(columnDef[2])) {
						if (tstruct.getChangetimeColumn() != null) {
							throw new ConfigurationException("More than one column defined as changetime for table "+tableName);
						}
						tstruct.setChangetimeColumn(columnDef[1]);
					} else if (DbTableConfiguration.COLUMN_TAG_IGNORE.equals(columnDef[2])) {
						tstruct.getIgnoredColumns().add(columnDef[1]);
					} else {
						throw new ConfigurationException("Unknown tag in the 'column' configuration property, line "+columnDefString);
					}
				}
			}
			ObjectClassInfoBuilder ocib = new ObjectClassInfoBuilder();
			ocib.setType(tableName);
			try {
				ResultSet rset = metadata.getColumns(null,null,tableName,null);
				while (rset.next()) {
					String columnName = rset.getString(4);
					// Name the first column to be name and uid if this is not explicitly defined
					if (tstruct.getUidColumn() == null) {
						tstruct.setUidColumn(columnName);
					}
					if (tstruct.getNameColumn() == null) {
						tstruct.setNameColumn(columnName);
					}
					if (tstruct.isUidColumn(columnName) && !tstruct.isNameColumn(columnName)) {
						// UID is implicit, it is not part of the schema
						continue;
					}
					AttributeInfoBuilder aib = new AttributeInfoBuilder(toIcfAttributeName(tstruct, columnName));
					int sqlTypeCode = rset.getInt(5);
					Class<?> icfType = toIcfType(sqlTypeCode);
					aib.setType(icfType);
					tstruct.setIcfType(icfType);
					String remarks = rset.getString(12);
					String nullableCode = rset.getString(18); // "YES", "NO", null
					aib.setMultiValued(false);
					if ("NO".equals(nullableCode)) {
						aib.setRequired(true);
					} else {
						aib.setRequired(false);
					}
					String autoincrementCode = rset.getString(23);
					String generatedCode = rset.getString(24);
					ocib.addAttributeInfo(aib.build());
				}
				rset.close();
			} catch (SQLException e) {
				throw new ConnectorException("Error retrieving schema of table "+tableName+": "+e.getMessage(),e);
			}
			schemaBuilder.defineObjectClass(ocib.build());
		}
		return schemaBuilder.build();
	}

	private Class<?> toIcfType(int sqlTypeCode) {
		Class type = TYPE_MAP_SQL.get(sqlTypeCode);
		if (type == null) {
			throw new IllegalStateException("No type mapping for SQL type code "+sqlTypeCode);
		}
		return type;
	}

	private String toIcfAttributeName(TableStructure tstruct, String columnName) {
		if (tstruct.isNameColumn(columnName)) {
			return Name.NAME;
		} else if (tstruct.isPasswordColumn(columnName)) {
			return OperationalAttributes.PASSWORD_NAME;
		} else {
			return columnName;
		}
	}
	
	static {
		addTypeMapping(Types.BIT, Boolean.class);
		addTypeMapping(Types.TINYINT, Integer.class);
		// TODO
	}

	private static void addTypeMapping(int sqlTypeCode, Class<?> icfType) {
		TYPE_MAP_SQL.put(sqlTypeCode, icfType);
		TYPE_MAP_ICF.put(icfType, sqlTypeCode);
	}
	
}
