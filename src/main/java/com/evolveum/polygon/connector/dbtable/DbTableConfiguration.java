/*
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

import org.identityconnectors.framework.spi.AbstractConfiguration;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.spi.ConfigurationProperty;

public class DbTableConfiguration extends AbstractConfiguration {

    private static final Log LOG = Log.getLog(DbTableConfiguration.class);

    private String host;
    
    private Integer port;
    
    private String user;
    
    private GuardedString password;
    
    private String database;
    
    private String jdbcDriver;
    
    private String jdbcUrl;
    
    private String[] table;
    
    /**
     * Format: tableName:column:type, where type is "uid", "name", "password", "ignore", "changetime"
     */
    private String[] column;

	public static final String COLUMN_TAG_UID = "uid";
	public static final String COLUMN_TAG_NAME = "name";
	public static final String COLUMN_TAG_PASSWORD = "password";
	public static final String COLUMN_TAG_IGNORE = "ignore";
	public static final String COLUMN_TAG_CHANGETIME = "changetime";
    
    private String accountTable;
    
    private String accountUidColumn;
    
    private String accountNameColumn;
    
    private String accountPasswordColumn;
    
    private String validConnectionQuery;
    
    public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public GuardedString getPassword() {
		return password;
	}

	public void setPassword(GuardedString password) {
		this.password = password;
	}

	public String getDatabase() {
		return database;
	}

	public void setDatabase(String database) {
		this.database = database;
	}

	public String getJdbcDriver() {
		return jdbcDriver;
	}

	public void setJdbcDriver(String jdbcDriver) {
		this.jdbcDriver = jdbcDriver;
	}

	public String getJdbcUrl() {
		return jdbcUrl;
	}

	public void setJdbcUrl(String jdbcUrl) {
		this.jdbcUrl = jdbcUrl;
	}

	public String[] getTable() {
		return table;
	}

	public void setTable(String[] table) {
		this.table = table;
	}

	public String[] getColumn() {
		return column;
	}

	public void setColumn(String[] column) {
		this.column = column;
	}

	public String getAccountTable() {
		return accountTable;
	}

	public void setAccountTable(String accountTable) {
		this.accountTable = accountTable;
	}

	public String getAccountUidColumn() {
		return accountUidColumn;
	}

	public void setAccountUidColumn(String accountUidColumn) {
		this.accountUidColumn = accountUidColumn;
	}

	public String getAccountNameColumn() {
		return accountNameColumn;
	}

	public void setAccountNameColumn(String accountNameColumn) {
		this.accountNameColumn = accountNameColumn;
	}

	public String getAccountPasswordColumn() {
		return accountPasswordColumn;
	}

	public void setAccountPasswordColumn(String accountPasswordColumn) {
		this.accountPasswordColumn = accountPasswordColumn;
	}

	public String getValidConnectionQuery() {
		return validConnectionQuery;
	}

	public void setValidConnectionQuery(String validConnectionQuery) {
		this.validConnectionQuery = validConnectionQuery;
	}

	@Override
    public void validate() {
        //todo implement
    }

	public String formatJdbcUrl() {
		if (jdbcUrl == null) {
			return null;
		}
		String s = jdbcUrl;
		if (host != null) {
			s = s.replaceAll("%h", host);
		} else {
			s = s.replaceAll("%h", "");
		}
		if (port != null) {
			s = s.replaceAll("%p", port.toString());
		} else {
			s = s.replaceAll("%p", "");
		}
		if (database != null) {
			s = s.replaceAll("%d", database);
		} else {
			s = s.replaceAll("%d", "");
		}
		s = s.replaceAll("%%", "%");
		return s;
	}

}