/*
 * NorthRidge Software, LLC - Copyright (c) 2019.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.nridge.core.base.ds;

/**
 * The DS (data source) class captures the constants, enumerated types
 * and utility methods for the data source package.
 *
 * @author Al Cole
 * @since 1.0
 */
public class DS
{
    public static final String VALUE_FORMAT_TYPE_CSV = "csv";
    public static final String VALUE_FORMAT_TYPE_XML = "xml";
    public static final String VALUE_FORMAT_TYPE_TXT = "txt";

    public static final String TXT_TABLE_FIELD_NAME = "item";
    public static final String TXT_TABLE_FIELD_TITLE = "Item";

    public static final String CRITERION_VALUE_IS_NULL = "+[NULL]+";

    public static final String CRITERIA_ENTRY_TYPE_NAME = "field_type";
    public static final String CRITERIA_ENTRY_FIELD_NAME = "field_name";
    public static final String CRITERIA_VALUE_FIELD_NAME = "field_value";
    public static final String CRITERIA_BOOLEAN_FIELD_NAME = "boolean_operator";
    public static final String CRITERIA_OPERATOR_FIELD_NAME = "logical_operator";

    public static final String TYPE_GSA = "gsa";
    public static final String TYPE_SOLR = "solr";
    public static final String TYPE_RDBMS = "rdbms";
    public static final String TYPE_ARAS_INNOVATOR = "ai";
    public static final String TYPE_MEMORY_TABLE = "memtbl";

    public static final String PROPERTY_INSTANCE = "Instance";
    public static final String PROPERTY_CONNECTION_POOL = "jdbc_connection_pool";

    public static final String RELATIONSHIP_SCHEMA = "Schema";
    public static final String RELATIONSHIP_VALUES = "Values";
    public static final String RELATIONSHIP_CRITERIA = "Criteria";
    public static final String RELATIONSHIP_CONFIGURATION = "Configuration";
}
