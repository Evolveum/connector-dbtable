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

import java.util.ArrayList;
import java.util.Collection;

/**
 * @author Radovan Semancik
 *
 */
public class TableStructure {

	private String uidColumn;
	private String nameColumn;
	private String passwordColumn;
	private Collection<String> ignoredColumns = new ArrayList<String>();
	private String changetimeColumn;
	private Class icfType;

	public String getUidColumn() {
		return uidColumn;
	}

	public void setUidColumn(String uidColumn) {
		this.uidColumn = uidColumn;
	}
	
	public boolean isUidColumn(String colName) {
		return uidColumn.equalsIgnoreCase(colName);
	}

	public String getNameColumn() {
		return nameColumn;
	}

	public void setNameColumn(String nameColumn) {
		this.nameColumn = nameColumn;
	}
	
	public boolean isNameColumn(String colName) {
		return nameColumn.equalsIgnoreCase(colName);
	}

	public String getPasswordColumn() {
		return passwordColumn;
	}

	public void setPasswordColumn(String passwordColumn) {
		this.passwordColumn = passwordColumn;
	}
	
	public boolean isPasswordColumn(String colName) {
		if (passwordColumn == null) {
			return false;
		}
		return passwordColumn.equalsIgnoreCase(colName);
	}

	public String getChangetimeColumn() {
		return changetimeColumn;
	}

	public void setChangetimeColumn(String changetimeColumn) {
		this.changetimeColumn = changetimeColumn;
	}

	public boolean isChangetimeColumn(String colName) {
		if (changetimeColumn == null) {
			return false;
		}
		return changetimeColumn.equalsIgnoreCase(colName);
	}
	
	public Collection<String> getIgnoredColumns() {
		return ignoredColumns;
	}

	public boolean isIgnoredColumn(String colName) {
		for (String ignoredColumn: ignoredColumns) {
			if (ignoredColumn.equalsIgnoreCase(colName)) {
				return true;
			}
		}
		return false;
	}

	public Class getIcfType() {
		return icfType;
	}

	public void setIcfType(Class icfType) {
		this.icfType = icfType;
	}
	
	
}
