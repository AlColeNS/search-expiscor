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

package com.nridge.core.base.field;

import com.nridge.core.base.doc.Document;
import com.nridge.core.base.field.data.DataBag;
import com.nridge.core.base.field.data.DataField;
import com.nridge.core.base.std.StrUtl;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;

/**
 * The Field class captures the constants, enumerated types and utility methods for the field package.
 * Specifically, it defines the following:
 * <ul>
 *     <li>Field data types</li>
 *     <li>Search criteria operators</li>
 *     <li>Date/Time and currency formatting constants</li>
 *     <li>Validation message defaults</li>
 *     <li>Data type conversion utility methods</li>
 * </ul>
 *
 * @since 1.0
 * @author Al Cole
 */
@SuppressWarnings({"UnusedDeclaration"})
public class Field
{
    public static final String VALUE_DATETIME_TODAY = "DateTimeToday";
    public static final String FORMAT_DATETIME_DEFAULT = "MMM-dd-yyyy HH:mm:ss";

// Date and time related constants.
// http://docs.oracle.com/javase/7/docs/api/index.html

    public static final String FORMAT_DATE_DEFAULT = "MMM-dd-yyyy";
    public static final String FORMAT_TIME_AMPM = "HH:mm:ss a";
    public static final String FORMAT_TIME_PLAIN = "HH:mm:ss";
    public static final String FORMAT_TIME_DEFAULT = FORMAT_TIME_PLAIN;
    public static final String FORMAT_TIMESTAMP_PACKED = "yyMMddHHmmss";
    public static final String FORMAT_SQLISODATE_DEFAULT = "yyyy-MM-dd";
    public static final String FORMAT_SQLISOTIME_DEFAULT = FORMAT_TIME_DEFAULT;
    public static final String FORMAT_SQLISODATETIME_DEFAULT = "yyyy-MM-dd HH:mm:ss";
    public static final String FORMAT_SQLORACLEDATE_DEFAULT = FORMAT_DATETIME_DEFAULT;
    public static final String FORMAT_ISO8601DATETIME_DEFAULT = "yyyy-MM-dd'T'HH:mm:ss'Z'";
    public static final String FORMAT_ISO8601DATETIME_MILLI2D = "yyyy-MM-dd'T'HH:mm:ss.SS'Z'";
    public static final String FORMAT_ISO8601DATETIME_MILLI3D = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    public static final String FORMAT_RFC1123_DATE_TIME = "EEE, dd MMM yyyy HH:mm:ss 'GMT'";

// Number related constants.
// http://docs.oracle.com/javase/7/docs/api/index.html

    public static final String FORMAT_INTEGER_PLAIN = "#";
    public static final String FORMAT_DOUBLE_COMMA = "###,###";
    public static final String FORMAT_DOUBLE_POINT = "###.####";
    public static final String FORMAT_DOUBLE_COMMA_POINT = "###,###.####";
    public static final String FORMAT_DOUBLE_PERCENT = "%##";
    public static final String FORMAT_DOUBLE_CURRENCY_COMMA_POINT = "$###,###.##";

// Data field feature constants.

    public static final String FEATURE_IS_SECRET = "isSecret";
    public static final String FEATURE_IS_STORED = "isStored";
    public static final String FEATURE_IS_UNIQUE = "isUnique";
    public static final String FEATURE_IS_HIDDEN = "isHidden";
    public static final String FEATURE_IS_INDEXED = "isIndexed";
    public static final String FEATURE_IS_CONTENT = "isContent";
    public static final String FEATURE_IS_REQUIRED = "isRequired";
    public static final String FEATURE_MV_DELIMITER = "delimiterChar";
    public static final String FEATURE_IS_PRIMARY_KEY = "isPrimaryKey";
    public static final String FEATURE_IS_RELATED_SRC_KEY = "isRelatedSrcKey";
    public static final String FEATURE_IS_RELATED_DST_KEY = "isRelatedDstKey";

// SQL field feature constants.

    public static final String FEATURE_TYPE_ID = "typeId";
    public static final String FEATURE_INDEX_TYPE = "indexType";
    public static final String FEATURE_STORED_SIZE = "storedSize";
    public static final String FEATURE_INDEX_POLICY = "indexPolicy";
    public static final String FEATURE_FUNCTION_NAME = "functionName";
    public static final String FEATURE_SEQUENCE_SEED = "sequenceSeed";
    public static final String FEATURE_TABLESPACE_NAME = "tableSpace";
    public static final String FEATURE_OPERATION_NAME = "operationName";
    public static final String FEATURE_SEQUENCE_INCREMENT = "sequenceIncrement";
    public static final String FEATURE_SEQUENCE_MANAGEMENT = "sequenceManagement";

// SQL field feature value constants.

    public static final String SQL_TABLE_TYPE_STORED = "Stored";
    public static final String SQL_TABLE_TYPE_MEMORY = "Memory";

    public static final String SQL_INDEX_UNDEFINED = "Undefined";
    public static final String SQL_INDEX_TYPE_STANDARD = "Standard";
    public static final String SQL_INDEX_TYPE_FULLTEXT = "FullText";

    public static final String SQL_INDEX_POLICY_UNIQUE = "Unique";
    public static final String SQL_INDEX_POLICY_DUPLICATE = "Duplicate";

    public static final String SQL_INDEX_MANAGEMENT_IMPLICIT = "Implicit";
    public static final String SQL_INDEX_MANAGEMENT_EXPLICIT = "Explicit";

// Field difference statuses.

    public static final String DIFF_STATUS_ADDED = "Added";
    public static final String DIFF_STATUS_UPDATED = "Updated";
    public static final String DIFF_STATUS_DELETED = "Deleted";
    public static final String DIFF_STATUS_UNCHANGED = "Unchanged";

// Field validation constants.

    public static final String VALIDATION_BAG_NAME = "bag_invalid";
    public static final String VALIDATION_PROPERTY_NAME = "field_invalid";
    public static final String VALIDATION_FIELD_CHANGED = "field_changed";
    public static final String VALIDATION_MESSAGE_DEFAULT = "One or more fields are invalid.";
    public static final String VALIDATION_MESSAGE_IS_REQUIRED = "A value must be assigned.";
    public static final String VALIDATION_MESSAGE_OUT_OF_RANGE = "The value is out of range.";
    public static final String VALIDATION_MESSAGE_FIELD_CHANGED = "The field value has changed.";
    public static final String VALIDATION_MESSAGE_SIZE_TOO_LARGE = "The value exceeds storage size.";
    public static final String VALIDATION_MESSAGE_PRIMARY_KEY = "There must be exactly one primary key field defined.";

    public static enum Type
    {
        Text, Integer, Long, Float, Double, Boolean, Date, Time, DateTime, Undefined
    }

    public static enum Operator
    {
        UNDEFINED, EQUAL, NOT_EQUAL, GREATER_THAN, GREATER_THAN_EQUAL,
        LESS_THAN, LESS_THAN_EQUAL, CONTAINS, NOT_CONTAINS, STARTS_WITH,
        NOT_STARTS_WITH, ENDS_WITH, NOT_ENDS_WITH, BETWEEN, NOT_BETWEEN,
        BETWEEN_INCLUSIVE, REGEX, EMPTY, NOT_EMPTY, AND, OR, IN, NOT_IN,
        SORT
    }

    public static enum Order
    {
        UNDEFINED, ASCENDING, DESCENDING
    }

    private Field()
    {
    }

    /**
     * Returns a string representation of a field type.
     *
     * @param aType Field type.
     *
     * @return String representation of a field type.
     */
    public static String typeToString(Type aType)
    {
        return aType.name();
    }

    /**
     * Returns the field type matching the string representation.
     *
     * @param aString String representation of a field type.
     *
     * @return Field type.
     */
    public static Type stringToType(String aString)
    {
        return Type.valueOf(aString);
    }

    /**
     * Returns a string representation of a field operator.
     *
     * @param anOperator Field operator.
     *
     * @return String representation of a field operator.
     */
    public static String operatorToString(Operator anOperator)
    {
        switch (anOperator)
        {
            case EQUAL:
                return "Equal";
            case NOT_EQUAL:
                return "NotEqual";
            case GREATER_THAN:
                return "GreaterThan";
            case GREATER_THAN_EQUAL:
                return "GreaterThanEqual";
            case LESS_THAN:
                return "LessThan";
            case LESS_THAN_EQUAL:
                return "LessThanEqual";
            case CONTAINS:
                return "Contains";
            case NOT_CONTAINS:
                return "NotContains";
            case STARTS_WITH:
                return "StartsWith";
            case NOT_STARTS_WITH:
                return "NotStartsWith";
            case ENDS_WITH:
                return "EndsWith";
            case NOT_ENDS_WITH:
                return "NotEndsWith";
            case BETWEEN:
                return "Between";
            case NOT_BETWEEN:
                return "NotBetween";
            case BETWEEN_INCLUSIVE:
                return "BetweenInclusive";
            case REGEX:
                return "RegEx";
            case EMPTY:
                return "Empty";
            case NOT_EMPTY:
                return "NotEmpty";
            case AND:
                return "And";
            case OR:
                return "Or";
            case IN:
                return "In";
            case NOT_IN:
                return "NotIn";
            case SORT:
                return "Sort";
            default:
                return "Undefined";
        }
    }

    /**
     * Returns the field operator matching the string representation.
     *
     * @param anOperator String representation of a field operator.
     *
     * @return Field operator.
     */
    public static Operator stringToOperator(String anOperator)
    {
        if (StringUtils.equalsIgnoreCase(anOperator, "Equal"))
            return Operator.EQUAL;
        else if (StringUtils.equalsIgnoreCase(anOperator, "NotEqual"))
            return Operator.NOT_EQUAL;
        else if (StringUtils.equalsIgnoreCase(anOperator, "GreaterThan"))
            return Operator.GREATER_THAN;
        else if (StringUtils.equalsIgnoreCase(anOperator, "GreaterThanEqual"))
            return Operator.GREATER_THAN_EQUAL;
        else if (StringUtils.equalsIgnoreCase(anOperator, "LessThan"))
            return Operator.LESS_THAN;
        else if (StringUtils.equalsIgnoreCase(anOperator, "LessThanEqual"))
            return Operator.LESS_THAN_EQUAL;
        else if (StringUtils.equalsIgnoreCase(anOperator, "Contains"))
            return Operator.CONTAINS;
        else if (StringUtils.equalsIgnoreCase(anOperator, "NotContains"))
            return Operator.NOT_CONTAINS;
        else if (StringUtils.equalsIgnoreCase(anOperator, "StartsWith"))
            return Operator.STARTS_WITH;
        else if (StringUtils.equalsIgnoreCase(anOperator, "NotStartsWith"))
            return Operator.NOT_STARTS_WITH;
        else if (StringUtils.equalsIgnoreCase(anOperator, "EndsWith"))
            return Operator.ENDS_WITH;
        else if (StringUtils.equalsIgnoreCase(anOperator, "NotEndsWith"))
            return Operator.NOT_ENDS_WITH;
        else if (StringUtils.equalsIgnoreCase(anOperator, "Between"))
            return Operator.BETWEEN;
        else if (StringUtils.equalsIgnoreCase(anOperator, "NotBetween"))
            return Operator.NOT_BETWEEN;
        else if (StringUtils.equalsIgnoreCase(anOperator, "BetweenInclusive"))
            return Operator.BETWEEN_INCLUSIVE;
        else if (StringUtils.equalsIgnoreCase(anOperator, "RegEx"))
            return Operator.REGEX;
        else if (StringUtils.equalsIgnoreCase(anOperator, "Empty"))
            return Operator.EMPTY;
        else if (StringUtils.equalsIgnoreCase(anOperator, "NotEmpty"))
            return Operator.NOT_EMPTY;
        else if (StringUtils.equalsIgnoreCase(anOperator, "And"))
            return Operator.AND;
        else if (StringUtils.equalsIgnoreCase(anOperator, "Or"))
            return Operator.OR;
        else if (StringUtils.equalsIgnoreCase(anOperator, "In"))
            return Operator.IN;
        else if (StringUtils.equalsIgnoreCase(anOperator, "NotIn"))
            return Operator.NOT_IN;
        else if (StringUtils.equalsIgnoreCase(anOperator, "Sort"))
            return Operator.SORT;
        else
            return Operator.UNDEFINED;
    }

    /**
     * Returns <i>true</i> if the field type represents a numeric
     * type.
     *
     * @param aType Field type.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public static boolean isNumber(Field.Type aType)
    {
        switch (aType)
        {
            case Integer:
            case Long:
            case Float:
            case Double:
                return true;
            default:
                return false;
        }
    }

    /**
     * Returns <i>true</i> if the field type represents a boolean
     * type.
     *
     * @param aType Field type.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public static boolean isBoolean(Field.Type aType)
    {
        switch (aType)
        {
            case Boolean:
                return true;
            default:
                return false;
        }
    }

    /**
     * Returns <i>true</i> if the field type represents a text
     * type.
     *
     * @param aType Field type.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public static boolean isText(Field.Type aType)
    {
        switch (aType)
        {
            case Text:
                return true;
            default:
                return false;
        }
    }

    /**
     * Returns <i>true</i> if the field type represents a date
     * or time type.
     *
     * @param aType Field type.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public static boolean isDateOrTime(Field.Type aType)
    {
        switch (aType)
        {
            case Date:
            case Time:
            case DateTime:
                return true;
            default:
                return false;
        }
    }

    /**
     * Returns a title that has been derived from the name of the
     * field.  This method will handle the conversion as follows:
     *
     * <ul>
     *     <li>id becomes Id</li>
     *     <li>employee_name becomes Employee Name</li>
     *     <li>federatedName becomes Federated Name</li>
     * </ul>
     *
     * The logic will ignore any other conventions and simply pass
     * the original character forward.
     *
     * @param aFieldName Name of the field to convert.
     *
     * @return Title for the field name.
     */
    public static String nameToTitle(String aFieldName)
    {
        if (StringUtils.isNotEmpty(aFieldName))
        {
            char curChar;
            boolean isLastSpace = true;
            boolean isLastLower = false;

            StringBuilder stringBuilder = new StringBuilder();
            int strLength = aFieldName.length();
            for (int i = 0; i < strLength; i++)
            {
                curChar = aFieldName.charAt(i);

                if (curChar == StrUtl.CHAR_UNDERLINE)
                {
                    curChar = StrUtl.CHAR_SPACE;
                    stringBuilder.append(curChar);
                }
                else if (isLastSpace)
                    stringBuilder.append(Character.toUpperCase(curChar));
                else if ((Character.isUpperCase(curChar)) && (isLastLower))
                {
                    stringBuilder.append(StrUtl.CHAR_SPACE);
                    stringBuilder.append(curChar);
                }
                else
                    stringBuilder.append(curChar);

                isLastSpace = (curChar == StrUtl.CHAR_SPACE);
                isLastLower = Character.isLowerCase(curChar);
            }

            return stringBuilder.toString();
        }
        else
            return aFieldName;
    }

    /**
     * Returns a field name that has been derived from the title.  This
     * method will handle the conversion as follows:
     *
     * <ul>
     *     <li>Id becomes id</li>
     *     <li>Employee Name becomes employee_name</li>
     *     <li>Federated Name becomes federated_name</li>
     * </ul>
     *
     * The logic will ignore any other conventions and simply pass
     * the original character forward.
     *
     * @param aTitle Title string to convert.
     *
     * @return Field name.
     */
    public static String titleToName(String aTitle)
    {
        if (StringUtils.isNotEmpty(aTitle))
            return StringUtils.replaceChars(aTitle.toLowerCase(), StrUtl.CHAR_SPACE, StrUtl.CHAR_UNDERLINE);
        else
            return aTitle;
    }

    /**
     * Returns a Java data type class representing the field type.
     *
     * @param aType Field type.
     *
     * @return Java data type class.
     */
    public static Class getTypeClass(Field.Type aType)
    {
        switch (aType)
        {
            case Integer:
                return Integer.class;
            case Long:
                return Long.class;
            case Float:
                return Float.class;
            case Double:
                return Double.class;
            case Boolean:
                return Boolean.class;
            case Date:
                return Date.class;
            case Time:
            case DateTime:
                return Calendar.class;
            default:
                return String.class;
        }
    }

    /**
     * Return a field type representing the object type.
     *
     * @param anObject Object instance.
     *
     * @return Field type.
     */
    public static Field.Type getTypeField(Object anObject)
    {
        if (anObject != null)
        {
            if (anObject instanceof Integer)
                return Type.Integer;
            else if (anObject instanceof Long)
                return Type.Long;
            else if (anObject instanceof Float)
                return Type.Float;
            else if (anObject instanceof Double)
                return Type.Double;
            else if (anObject instanceof Boolean)
                return Type.Boolean;
            else if (anObject instanceof Date)
                return Type.DateTime;
            else if (anObject instanceof Calendar)
                return Type.DateTime;
        }

        return Type.Text;
    }

    /**
     * Returns a hidden string value (e.g. a simple Caesar-cypher
     * encryption) based on the value parameter.  This method is
     * only intended to obscure a field value.  The developer
     * should use other encryption methods to achieve the goal
     * of strong encryption.
     *
     * @param aValue String to hide.
     *
     * @return Hidden string value.
     */
    public static String hideValue(String aValue)
    {
        if (StrUtl.isHidden(aValue))
            return aValue;
        else
            return StrUtl.hidePassword(aValue);
    }

    /**
     * Returns a previously hidden (e.g. Caesar-cypher encrypted)
     * string to its original form.
     *
     * @param aValue Hidden string value.
     *
     * @return Decrypted string value.
     */
    public static String recoverValue(String aValue)
    {
        if (StrUtl.isHidden(aValue))
            return StrUtl.recoverPassword(aValue);
        else
            return aValue;
    }

    /**
     * Returns an <i>int</i> representation of the field
     * value string.
     *
     * @param aValue Numeric string value.
     *
     * @return Converted value.
     */
    public static int createInt(String aValue)
    {
        if (NumberUtils.isNumber(aValue))
            return Integer.parseInt(aValue);
        else
            return Integer.MIN_VALUE;
    }

    /**
     * Returns an <i>Integer</i> representation of the field
     * value string.
     *
     * @param aValue Numeric string value.
     *
     * @return Converted value.
     */
    public static Integer createIntegerObject(String aValue)
    {
        if (NumberUtils.isNumber(aValue))
            return new Integer(aValue);
        else
            return Integer.MIN_VALUE;
    }

    /**
     * Returns a <i>long</i> representation of the field
     * value string.
     *
     * @param aValue Numeric string value.
     *
     * @return Converted value.
     */
    public static long createLong(String aValue)
    {
        if (NumberUtils.isNumber(aValue))
            return Long.parseLong(aValue);
        else
            return Long.MIN_VALUE;
    }

    /**
     * Returns a <i>Long</i> representation of the field
     * value string.
     *
     * @param aValue Numeric string value.
     *
     * @return Converted value.
     */
    public static Long createLongObject(String aValue)
    {
        if (NumberUtils.isNumber(aValue))
            return new Long(aValue);
        else
            return Long.MIN_VALUE;
    }

    /**
     * Returns a <i>float</i> representation of the field
     * value string.
     *
     * @param aValue Numeric string value.
     *
     * @return Converted value.
     */
    public static float createFloat(String aValue)
    {
        if (NumberUtils.isNumber(aValue))
            return Float.parseFloat(aValue);
        else
            return Float.MIN_VALUE;
    }

    /**
     * Returns a <i>Float</i> representation of the field
     * value string.
     *
     * @param aValue Numeric string value.
     *
     * @return Converted value.
     */
    public static Float createFloatObject(String aValue)
    {
        if (NumberUtils.isNumber(aValue))
            return new Float(aValue);
        else
            return Float.MIN_VALUE;
    }

    /**
     * Returns a <i>double</i> representation of the field
     * value string.
     *
     * @param aValue Numeric string value.
     *
     * @return Converted value.
     */
    public static double createDouble(String aValue)
    {
        if (NumberUtils.isNumber(aValue))
            return Double.parseDouble(aValue);
        else
            return Double.MIN_VALUE;
    }

    /**
     * Returns a <i>Double</i> representation of the field
     * value string.
     *
     * @param aValue Numeric string value.
     *
     * @return Converted value.
     */
    public static Double createDoubleObject(String aValue)
    {
        if (NumberUtils.isNumber(aValue))
            return new Double(aValue);
        else
            return Double.MIN_VALUE;
    }

    /**
     * Returns <i>true</i> if the field value represents a boolean
     * true string (e.g. yes, true) or <i>false</i> otherwise.
     *
     * @param aValue Boolean string value.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public static boolean isValueTrue(String aValue)
    {
        return StrUtl.stringToBoolean(aValue);
    }

    /**
     * Returns a <i>Date</i> representation of the field value
     * string based on the format mask property.
     *
     * @param aValue Date/Time string value.
     * @param aFormatMask SimpleDateFormat mask.
     *
     * @return Converted value.
     */
    public static Date createDate(String aValue, String aFormatMask)
    {
        if (StringUtils.isNotEmpty(aValue))
        {
            ParsePosition parsePosition = new ParsePosition(0);
            SimpleDateFormat simpleDateFormat;
            if (StringUtils.isNotEmpty(aFormatMask))
                simpleDateFormat = new SimpleDateFormat(aFormatMask);
            else
                simpleDateFormat = new SimpleDateFormat(Field.FORMAT_DATETIME_DEFAULT);
            return simpleDateFormat.parse(aValue, parsePosition);
        }
        else
            return new Date();
    }

    /**
     * Returns a <i>Date</i> representation of the field value
     * string based on the FORMAT_DATETIME_DEFAULT format mask
     * property.
     *
     * @param aValue Date/Time string value.
     *
     * @return Converted value.
     */
    public static Date createDate(String aValue)
    {
        if (StringUtils.isNotEmpty(aValue))
        {
            if (aValue.equals(Field.VALUE_DATETIME_TODAY))
                return new Date();
            else
            {
                ParsePosition parsePosition = new ParsePosition(0);
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat(Field.FORMAT_DATETIME_DEFAULT);
                return simpleDateFormat.parse(aValue, parsePosition);
            }
        }
        else
            return new Date();
    }

    /**
     * Returns a formatted <i>String</i> representation of the date
     * parameter based on the format mask parameter.  If the format
     * mask is <i>null</i>, then <code>Field.FORMAT_DATETIME_DEFAULT</code>
     * will be used.
     *
     * @param aDate Date/Time to convert.
     * @param aFormatMask Format mask string.
     *
     * @return String representation of the date/time parameter.
     */
    public static String dateValueFormatted(Date aDate, String aFormatMask)
    {
        if (StringUtils.isEmpty(aFormatMask))
            aFormatMask = Field.FORMAT_DATETIME_DEFAULT;

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(aFormatMask);

        return simpleDateFormat.format(aDate.getTime());
    }

    /**
     * Returns the time representation of field value
     * string based on the FORMAT_DATETIME_DEFAULT format mask
     * property.
     *
     * @param aValue Date/Time string value.
     *
     * @return The number of milliseconds since January 1, 1970, 00:00:00 GMT
     * represented by this value.
     */
    public static long createDateLong(String aValue)
    {
        Date dateValue = createDate(aValue);
        return dateValue.getTime();
    }

    /**
     * Convenience method that extracts the first field row from
     * an array of field rows.
     *
     * @param aFieldRows An array of field rows.
     *
     * @return A single field row (the first) or <i>null</i> if
     * the array is empty.
     */
    public static FieldRow firstFieldRow(ArrayList<FieldRow> aFieldRows)
    {
        if ((aFieldRows != null) && (aFieldRows.size() > 0))
            return aFieldRows.get(0);
        else
            return null;
    }

    /**
     * Convenience method that extracts the first validation error
     * message from a bag of fields.
     *
     * @param aBag Data bag instance.
     *
     * @return Validation message or a default message.
     */
    public static String firstValidationMessage(DataBag aBag)
    {
        if (aBag != null)
        {
            String validationMessage;

            for (DataField dataField : aBag.getFields())
            {
                validationMessage = (String) dataField.getProperty(Field.VALIDATION_PROPERTY_NAME);
                if (StringUtils.isNotEmpty(validationMessage))
                    return String.format("%s: %s", dataField.getName(), validationMessage);
            }
        }

        return VALIDATION_MESSAGE_DEFAULT;
    }

    /**
     * Convenience method that extracts the first validation error
     * message from a document of fields.
     *
     * @param aDocument Document instance.
     *
     * @return Validation message or a default message.
     */
    public static String firstValidationMessage(Document aDocument)
    {
        if (aDocument != null)
            return firstValidationMessage(aDocument.getBag());
        else
            return VALIDATION_MESSAGE_DEFAULT;
    }

    /**
     * Convenience method that concatenates the vendor name with a schema or
     * account name to form a property prefix.
     *
     * @param aVendor RDBMS vendor name.
     * @param aSchemaOrAccountName Schema or RDBMS account name.
     *
     * @return Property prefix.
     */
    public static String sqlPropertyPrefix(String aVendor, String aSchemaOrAccountName)
    {
        return aVendor + "." + aSchemaOrAccountName;
    }
}
