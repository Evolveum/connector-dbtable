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
import java.sql.ResultSet;
import java.sql.Statement;

import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.objects.AttributeInfoBuilder;
import org.identityconnectors.framework.common.objects.ObjectClassInfoBuilder;
import org.identityconnectors.framework.common.objects.Schema;
import org.identityconnectors.framework.common.objects.SchemaBuilder;
import org.identityconnectors.framework.spi.Configuration;
import org.identityconnectors.framework.spi.Connector;
import org.identityconnectors.framework.spi.ConnectorClass;
import org.identityconnectors.framework.spi.PoolableConnector;
import org.identityconnectors.framework.spi.operations.SchemaOp;
import org.identityconnectors.framework.spi.operations.TestOp;

@ConnectorClass(displayNameKey = "dbtable.connector.display", configurationClass = DbTableConfiguration.class)
public class DbTableConnector implements PoolableConnector, TestOp, SchemaOp {

    private static final Log LOG = Log.getLog(DbTableConnector.class);

    private DbTableConfiguration configuration;
    private DbTableConnection connection;
    private TableStructureRegistry tableStructureRegistry = null;

    @Override
    public Configuration getConfiguration() {
        return configuration;
    }

    @Override
    public void init(Configuration configuration) {
        this.configuration = (DbTableConfiguration)configuration;
        this.connection = new DbTableConnection(this.configuration);
    }

    @Override
    public void dispose() {
        configuration = null;
        if (connection != null) {
            connection.close();
            connection = null;
        }
    }

	@Override
	public void test() {
		connection.connect();
		connection.checkAlive();
		connection.close();
	}
    	
	@Override
	public Schema schema() {
		connection.init();
		tableStructureRegistry = new TableStructureRegistry();
		Schema schema = tableStructureRegistry.parse(connection);
		return schema;
	}

	@Override
	public void checkAlive() {
		connection.checkAlive();
	}
}
