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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.exceptions.ConfigurationException;
import org.identityconnectors.framework.common.exceptions.ConnectionFailedException;
import org.identityconnectors.framework.common.exceptions.ConnectorException;

import com.evolveum.polygon.common.GuardedStringAccessor;

public class DbTableConnection {

    private static final Log LOG = Log.getLog(DbTableConnection.class);

    private DbTableConfiguration configuration;
    private Connection connection = null;

    public DbTableConnection(DbTableConfiguration configuration) {
        this.configuration = configuration;
    }

    public void connect() {
    	try {
			Class.forName(configuration.getJdbcDriver());
		} catch (ClassNotFoundException e) {
			throw new ConfigurationException("JDBC driver class "+configuration.getJdbcDriver()+" was not found", e);
		}
    	
    	String url = configuration.formatJdbcUrl();
    	
    	String username = configuration.getUser();
    	try {
	    	if (username != null) {
		    	GuardedStringAccessor accessor = new GuardedStringAccessor();
		    	configuration.getPassword().access(accessor);
		    	connection = DriverManager.getConnection(url, username, new String(accessor.getClearChars()));
	    	} else {    		
				connection = DriverManager.getConnection(url);
	    	}
		} catch (SQLException e) {
			throw new ConnectionFailedException("Connection to "+url+" failed: "+e.getMessage(), e);
		}
    }
    
    public Connection getSqlConnection() {
    	return connection;
    }
    
    public DatabaseMetaData getMetaData() {
    	try {
			return connection.getMetaData();
		} catch (SQLException e) {
			throw new ConnectorException("Unable to get metadata: "+e.getMessage(),e);
		}
    }
    
    public Statement createStatememt() {
    	try {
			return connection.createStatement();
		} catch (SQLException e) {
			throw new ConnectorException("Unable to create SQL statememnt: "+e.getMessage(),e);
		}
    }
    
    public void close() {
    	if (connection != null) {
    		try {
				if (!connection.isClosed()) {
					connection.close();
				}
			} catch (SQLException e) {
				LOG.error("Error closing SQL connection: {0}", e.getMessage());
				// but do not rethrow the exception. We want to close it quietly.
			}
    	}
    }

	public void init() {
		if (connection == null) {
			connect();
		}
	}

	public void checkAlive() {
		// TODO: some implicit check (e.g. flipping autocommit)
		String query = configuration.getValidConnectionQuery();
		if (query != null) {
			Statement stmt = null;
			try {
				stmt = connection.createStatement();
				if (!stmt.execute(query)) {
		            throw new ConnectorException("Invalid check query: "+query);                            
		        }
			} catch (Exception e) {
				throw new ConnectorException("Connection is invalid: "+e.getMessage(), e);
			} finally {
				if (stmt != null) {
					try {
						stmt.close();
					} catch (SQLException e) {
						throw new ConnectorException("Connection is invalid: "+e.getMessage(), e);
					}
				}
			}
		}
	}
}