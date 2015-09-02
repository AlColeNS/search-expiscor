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

package com.nridge.core.ds.rdbms;

import com.nridge.core.base.field.Field;
import com.nridge.core.base.field.data.DataField;

/**
 * The SQL class captures the constants, enumerated types and utility methods for the sql package.
 *
 * @since 1.0
 * @author Al Cole
 */
public class SQL
{
    public static String COLUMN_ID_FIELD_NAME = "id";
    public static String COLUMN_VALUE_EMPTY = "NULL";

    public static String FUNCTION_COLUMN_AVG = "AVG";
    public static String FUNCTION_COLUMN_MIN = "MIN";
    public static String FUNCTION_COLUMN_MAX = "MAX";
    public static String FUNCTION_COLUMN_SUM = "SUM";
    public static String FUNCTION_COLUMN_COUNT = "COUNT";
    public static String FUNCTION_COLUMN_UNIQUE = "UNIQUE";
    public static String FUNCTION_COLUMN_DISTINCT = "DISTINCT";

    public static final String SORT_ORDER_ASCEND = " ASC";
    public static final String SORT_ORDER_DESCEND = " DESC";

    public static final int VALUE_IS_INVALID = -1;
    public static final int CRITERIA_NO_OFFSET = -1;
    public static final int CRITERIA_NO_LIMITS = -1;

    public static final String PROPERTY_PREFIX_DEFAULT = "rdbms.default";

    private SQL()
    {
    }

    /**
     * Returns <i>true</i> if the field is sequence managed or <i>false</i> otherwise.
     *
     * @param aField Data field.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public static boolean isSequenceManaged(DataField aField)
    {
        return ((aField.isFeatureEqual(Field.FEATURE_SEQUENCE_MANAGEMENT, Field.SQL_INDEX_MANAGEMENT_IMPLICIT)) ||
                (aField.isFeatureEqual(Field.FEATURE_SEQUENCE_MANAGEMENT, Field.SQL_INDEX_MANAGEMENT_EXPLICIT)));
    }

    /**
     * Returns <i>true</i> if the field is an implicit sequence or <i>false</i> otherwise.
     *
     * @param aField Data field.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public static boolean isSequenceImplicit(DataField aField)
    {
        return aField.isFeatureEqual(Field.FEATURE_SEQUENCE_MANAGEMENT, Field.SQL_INDEX_MANAGEMENT_IMPLICIT);
    }

    /**
     * Returns <i>true</i> if the field is an explicit sequence or <i>false</i> otherwise.
     *
     * @param aField Data field.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public static boolean isSequenceExplicit(DataField aField)
    {
        return aField.isFeatureEqual(Field.FEATURE_SEQUENCE_MANAGEMENT, Field.SQL_INDEX_MANAGEMENT_EXPLICIT);
    }

    /**
     * Returns the sequence seed value (if assigned or 1 otherwise).
     *
     * @param aField Data field.
     *
     * @return Sequence seed value.
     */
    public static int getSequenceSeed(DataField aField)
    {
        if (aField.isFeatureAssigned(Field.FEATURE_SEQUENCE_SEED))
            return aField.getFeatureAsInt(Field.FEATURE_SEQUENCE_SEED);
        else
            return 1;
    }

    /**
     * Returns the sequence seed increment value (if assigned or 1 otherwise).
     *
     * @param aField Data field.
     *
     * @return Sequence increment value.
     */
    public static int getSequenceIncrement(DataField aField)
    {
        if (aField.isFeatureAssigned(Field.FEATURE_SEQUENCE_INCREMENT))
            return aField.getFeatureAsInt(Field.FEATURE_SEQUENCE_INCREMENT);
        else
            return 1;
    }
}
