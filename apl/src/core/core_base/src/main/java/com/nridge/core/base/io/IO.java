/*
 * NorthRidge Software, LLC - Copyright (c) 2015.
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

package com.nridge.core.base.io;

import com.nridge.core.base.std.StrUtl;
import org.apache.commons.lang3.StringUtils;

/**
 * The IO class captures the constants, enumerated types and utility methods for the IO package.
 *
 * @since 1.0
 * @author Al Cole
 */
public class IO
{
    public static final String SQLBAG_XML_FORMAT_VERSION = "1.0";
    public static final String DATABAG_XML_FORMAT_VERSION = "1.0";
    public static final String CRITERIA_XML_FORMAT_VERSION = "1.0";
    public static final String DATATABLE_XML_FORMAT_VERSION = "1.0";

    public static final String SQLBAG_JSON_FORMAT_VERSION = "1.0";
    public static final String DATABAG_JSON_FORMAT_VERSION = "1.0";
    public static final String CRITERIA_JSON_FORMAT_VERSION = "1.0";
    public static final String DATATABLE_JSON_FORMAT_VERSION = "1.0";

// XML nodes.

    public static final String XML_ACL_NODE_NAME = "ACL";
    public static final String XML_ACE_NODE_NAME = "ACE";
    public static final String XML_LIST_NODE_NAME = "List";
    public static final String XML_FIELD_NODE_NAME = "Field";
    public static final String XML_REPLY_NODE_NAME = "Reply";
    public static final String XML_TABLE_NODE_NAME = "Table";
    public static final String XML_RELATED_NODE_NAME = "Related";
    public static final String XML_DOCUMENT_NODE_NAME = "Document";
    public static final String XML_CRITERIA_NODE_NAME = "Criteria";
    public static final String XML_OPERATION_NODE_NAME = "Operation";
    public static final String XML_PROPERTIES_NODE_NAME = "Properties";
    public static final String XML_RELATIONSHIP_NODE_NAME = "Relationship";

// JSON names.

    public static final String JSON_BAG_OBJECT_NAME = "bag";
    public static final String JSON_FIELD_OBJECT_NAME = "field";
    public static final String JSON_RANGE_OBJECT_NAME = "range";
    public static final String JSON_TABLE_OBJECT_NAME = "table";
    public static final String JSON_CONTEXT_OBJECT_NAME = "context";
    public static final String JSON_CRITERIA_OBJECT_NAME = "criteria";
    public static final String JSON_DOCUMENT_OBJECT_NAME = "document";

    public static final String JSON_ACL_ARRAY_NAME = "acl";
    public static final String JSON_ROWS_ARRAY_NAME = "rows";
    public static final String JSON_FIELDS_ARRAY_NAME = "fields";
    public static final String JSON_RELATED_ARRAY_NAME = "related";
    public static final String JSON_FEATURES_ARRAY_NAME = "features";
    public static final String JSON_DOCUMENTS_ARRAY_NAME = "documents";

    public static final String JSON_CELL_MEMBER_NAME = "c";
    public static final String JSON_NAME_MEMBER_NAME = "name";
    public static final String JSON_TYPE_MEMBER_NAME = "type";
    public static final String JSON_TITLE_MEMBER_NAME = "title";
    public static final String JSON_VALUE_MEMBER_NAME = "value";
    public static final String JSON_COUNT_MEMBER_NAME = "count";
    public static final String JSON_START_MEMBER_NAME = "start";
    public static final String JSON_LIMIT_MEMBER_NAME = "limit";
    public static final String JSON_TOTAL_MEMBER_NAME = "total";
    public static final String JSON_VERSION_MEMBER_NAME = "version";
    public static final String JSON_OPERATOR_MEMBER_NAME = "operator";
    public static final String JSON_DIMENSIONS_MEMBER_NAME = "dimensions";
    public static final String JSON_DELIMITER_MEMBER_NAME = "delimiterChar";


    private IO()
    {
    }

    public static String extractType(String aClassName)
    {
        if (StringUtils.isNotEmpty(aClassName))
        {
            int offset = aClassName.lastIndexOf(StrUtl.CHAR_DOT);
            if (offset == -1)
                return aClassName;
            else
            {
                if (offset < aClassName.length()-1)
                    return aClassName.substring(offset + 1);
                else
                    return aClassName;
            }
        }
        return StringUtils.EMPTY;
    }

    public static boolean isTypesEqual(String aClassName1, String aClassName2)
    {
        String className1 = extractType(aClassName1);
        String className2 = extractType(aClassName2);

        return className1.equals(className2);
    }
}
