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

package com.nridge.core.ds.rdbms;

import com.nridge.core.app.mgr.AppMgr;
import com.nridge.core.base.ds.DSCriteria;
import com.nridge.core.base.ds.DSCriterion;
import com.nridge.core.base.ds.DSCriterionEntry;
import com.nridge.core.base.field.Field;
import com.nridge.core.base.field.data.DataBag;
import com.nridge.core.base.field.data.DataDateTimeField;
import com.nridge.core.base.field.data.DataField;
import com.nridge.core.base.field.data.DataTable;
import com.nridge.core.base.std.NSException;
import com.nridge.core.base.std.StrUtl;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.sql.ResultSet;
import java.text.SimpleDateFormat;

/**
 * The SQLTable is an abstract type that represents an RDBMS
 * SQL table object.  Vendor specific implementations
 * will override these methods to ensure they are customized
 * to their unique SQL statement syntax.
 * <p>
 * <b>Note:</b> If the auto-commit flag is <i>false</i>, then
 * the developer is responsible for managing the transaction
 * model for any RDBMS insert/update/delete operations.
 * </p>
 *
 * @author Al Cole
 * @since 1.0
 */
public abstract class SQLTable
{
    protected final String NS_TABLE_PREFIX = "tbl";

    protected SQLConnection mSQLConnection;
    protected String mType = Field.SQL_TABLE_TYPE_STORED;

    /**
     * Default constructor.
     */
    public SQLTable()
    {
        mSQLConnection = null;
    }

    /**
     * Constructor that accepts a SQL connection.
     *
     * @param aConnection SQL connection.
     */
    public SQLTable(SQLConnection aConnection)
    {
        mSQLConnection = aConnection;
    }

    /**
     * Returns a string summary representation of a SQL table.
     *
     * @return String summary representation of this SQL table.
     */
    @Override
    public String toString()
    {
        String idName;

        if (mSQLConnection == null)
            idName = "Table - SQL";
        else
            idName = "Table - " + mSQLConnection.getVendorName();

        return idName;
    }

    /**
     * Returns a reference to the underlying SQL connection
     * instance.
     *
     * @return SQL connection.
     */
    public SQLConnection getConnection()
    {
        return mSQLConnection;
    }

    /**
     * Assigns a vendor specific table type implementation
     * (e.g. memory).
     *
     * @param aType SQL table type.
     */
    public void setType(String aType)
    {
        mType = aType;
    }

    /**
     * Returns a vendor specific table type implementation.
     *
     * @return SQL table type.
     */
    public String getType()
    {
        return mType;
    }

    /**
     * If the auto-naming feature is enabled against the SQL connection,
     * then this method will return an updated table name based on the
     * DB name assigned to the field bag.  Otherwise, it simply returns
     * the DB name.
     *
     * @param aBag Bag of fields.
     *
     * @return Table name.
     *
     * @throws NSException Catch-all exception for any SQL related issue.
     */
    public String schemaName(DataBag aBag)
        throws NSException
    {
        String dbName = aBag.getName();
        if (StringUtils.isEmpty(dbName))
            throw new NSException("The name for the data bag is undefined.");

        String tableName;
        if ((mSQLConnection.isAutoNamingEnabled()) && (! StringUtils.startsWith(dbName, NS_TABLE_PREFIX)))
            tableName = String.format("%s_%s", NS_TABLE_PREFIX, dbName);
        else
            tableName = dbName;

        return tableName;
    }

    /**
     * Returns a properly escaped column name suitable for use in
     * SQL statements.
     *
     * @param aName Name of column.
     *
     * @return An escaped column name (if appropriate).
     *
     * @throws NSException Catch-all exception for any SQL related issue.
     */
    public String columnName(String aName)
        throws NSException
    {
        if (StringUtils.isEmpty(aName))
            throw new NSException("The name for the column is undefined.");

        String columnName;
        if (mSQLConnection.isVendorSQLServer())
            columnName = String.format("[%s]", aName);
        else
            columnName = aName;

        return columnName;
    }

    /**
     * Returns a SQL fragment representing a function being applied
     * to a column.  The supported list of function names are defined
     * in the <i>Field</i> class.
     *
     * @param aFunctionName SQL function name.
     * @param aColumnName Column name.
     *
     * @return SQL function column name.
     */
    public String functionColumnName(String aFunctionName, String aColumnName)
    {
        if (aFunctionName.equals(SQL.FUNCTION_COLUMN_COUNT))
            return String.format("%s(*)", aFunctionName);       // Potential performance improvement
        else
            return String.format("%s(%s)", aFunctionName, aColumnName);
    }

    /**
     * Returns a properly escaped SQL text string.  The escaping
     * logic is vendor specific.
     *
     * @param aText SQL text that requires escaping.
     *
     * @return A properly escaped SQL text string.
     */
    public String escapeText(String aText)
    {
        if (aText.indexOf(StrUtl.CHAR_SGLQUOTE) != -1)
        {
            if ((mSQLConnection.isVendorOracle()) ||
                (mSQLConnection.isVendorHyperSQL()) ||
                (mSQLConnection.isVendorPostgreSQL()))
            {
                char ch;
                int strLength = aText.length();
                StringBuilder stringBuilder = new StringBuilder();

                for (int i = 0; i < strLength; i++)
                {
                    if ((ch = aText.charAt(i)) == StrUtl.CHAR_SGLQUOTE)
                        stringBuilder.append("''");
                    else
                        stringBuilder.append(ch);
                }
                return stringBuilder.toString();
            }
            else
            {
                char ch;
                int strLength = aText.length();
                StringBuilder stringBuilder = new StringBuilder();

                for (int i = 0; i < strLength; i++)
                {
                    ch = aText.charAt(i);
                    if (ch == StrUtl.CHAR_SGLQUOTE)
                        stringBuilder.append(StrUtl.CHAR_BACKSLASH);
                    stringBuilder.append(ch);
                }
                return stringBuilder.toString();
            }
        }
        else
            return aText;
    }

    /**
     * Returns a properly escaped SQL timestamp string.  The escaping
     * logic is vendor specific.
     *
     * @param aTimestamp Timestamp value.
     *
     * @return A properly escaped SQL timestamp string.
     *
     * @throws NSException Catch-all exception for any SQL related issue.
     */
    public String escapeTimestamp(long aTimestamp)
        throws NSException
    {
        SimpleDateFormat dateTimeFormat = new SimpleDateFormat(Field.FORMAT_SQLISODATETIME_DEFAULT);
        return String.format("{ts '%s'}", dateTimeFormat.format(aTimestamp));
    }

    /**
     * Returns a SQL column condition representing the criterion entry
     * parameter.  Typically, these column conditions are concatenated
     * together to form the SQL where clause in a select statement.
     *
     * @param aCriterionEntry Criterion entry.
     *
     * @return SQL column condition.
     *
     * @throws NSException Thrown if the logical operator is unsupported.
     */
    public String columnCondition(DSCriterionEntry aCriterionEntry)
        throws NSException
    {
        DataField dataField = aCriterionEntry.getField();
        DSCriterion dsCriterion = aCriterionEntry.getCriterion();
        String columnName = columnName(aCriterionEntry.getName());

        if (dataField.isTypeText())
        {
            if (aCriterionEntry.isCaseInsensitive())
                columnName = columnName.toLowerCase();
            String columnValue = escapeText(aCriterionEntry.getValue());

            switch (aCriterionEntry.getLogicalOperator())
            {
                case EQUAL:
                    if (aCriterionEntry.isCaseInsensitive())
                        return String.format(" (LOWER(%s) = LOWER('%s'))", columnName, columnValue);
                    else
                        return String.format(" (%s = '%s')", columnName, columnValue);
                case NOT_EQUAL:
                    if (aCriterionEntry.isCaseInsensitive())
                        return String.format(" (LOWER(%s) != LOWER('%s'))", columnName, columnValue);
                    else
                        return String.format("(%s != '%s')", columnName, columnValue);
                case CONTAINS:
                    if (aCriterionEntry.isCaseInsensitive())
                        return String.format(" (LOWER(%s) LIKE LOWER('%%%s%%'))", columnName, columnValue);
                    else
                        return String.format(" (%s LIKE '%%%s%%')", columnName, columnValue);
                case NOT_CONTAINS:
                    if (aCriterionEntry.isCaseInsensitive())
                        return String.format(" (LOWER(%s) NOT LIKE LOWER('%%%s%%'))", columnName, columnValue);
                    else
                        return String.format(" (%s NOT LIKE '%%%s%%')", columnName, columnValue);
                case STARTS_WITH:
                    if (aCriterionEntry.isCaseInsensitive())
                        return String.format(" (LOWER(%s) LIKE LOWER('%s%%'))", columnName, columnValue);
                    else
                        return String.format(" (%s LIKE '%s%%')", columnName, columnValue);
                case ENDS_WITH:
                    if (aCriterionEntry.isCaseInsensitive())
                        return String.format(" (LOWER(%s) LIKE LOWER('%%%s'))", columnName, columnValue);
                    else
                        return String.format(" (%s LIKE '%%%s')", columnName, columnValue);
                case EMPTY:
                    return String.format(" (%s IS NULL)", columnName);
                case NOT_EMPTY:
                    return String.format(" (%s IS NOT NULL)", columnName);
                case IN:
                    if (dsCriterion.isMultiValue())
                    {
                        boolean isFirst = true;
                        StringBuilder sqlBuilder = new StringBuilder(" " + columnName + " IN (");
                        for (String mValue : dsCriterion.getValues())
                        {
                            if (isFirst)
                            {
                                isFirst = false;
                                sqlBuilder.append(mValue);
                            }
                            else
                            {
                                sqlBuilder.append(StrUtl.CHAR_COMMA);
                                sqlBuilder.append(mValue);
                            }
                        }
                        sqlBuilder.append(StrUtl.CHAR_PAREN_CLOSE);

                        return sqlBuilder.toString();
                    }
                    else
                        throw new NSException(String.format("[%s] Must be multi-value: %s",
                                              aCriterionEntry.getName(),
                                              aCriterionEntry.getLogicalOperator()));
                case NOT_IN:
                    if (dsCriterion.isMultiValue())
                    {
                        boolean isFirst = true;
                        StringBuilder sqlBuilder = new StringBuilder(" " + columnName + " NOT IN (");
                        for (String mValue : dsCriterion.getValues())
                        {
                            if (isFirst)
                            {
                                isFirst = false;
                                sqlBuilder.append(mValue);
                            }
                            else
                            {
                                sqlBuilder.append(StrUtl.CHAR_COMMA);
                                sqlBuilder.append(mValue);
                            }
                        }
                        sqlBuilder.append(StrUtl.CHAR_PAREN_CLOSE);

                        return sqlBuilder.toString();
                    }
                    else
                        throw new NSException(String.format("[%s] Must be multi-value: %s",
                                                            aCriterionEntry.getName(),
                                                            aCriterionEntry.getLogicalOperator()));
                default:
                    throw new NSException(String.format("[%s] Unsupported condition operator: %s",
                                                        aCriterionEntry.getName(),
                                                        aCriterionEntry.getLogicalOperator()));
            }
        }
        else if (dataField.isTypeNumber())
        {
            switch (aCriterionEntry.getLogicalOperator())
            {
                case EQUAL:
                    return String.format(" (%s = %s)", columnName, aCriterionEntry.getValue());
                case NOT_EQUAL:
                    return String.format(" (%s != %s)", columnName, aCriterionEntry.getValue());
                case GREATER_THAN:
                    return String.format(" (%s > %s)", columnName, aCriterionEntry.getValue());
                case GREATER_THAN_EQUAL:
                    return String.format(" (%s >= %s)", columnName, aCriterionEntry.getValue());
                case LESS_THAN:
                    return String.format(" (%s < %s)", columnName, aCriterionEntry.getValue());
                case LESS_THAN_EQUAL:
                    return String.format(" (%s <= %s)", columnName, aCriterionEntry.getValue());
                case BETWEEN:
                    return String.format(" ((%s > %s) AND (%s < %s))", columnName, aCriterionEntry.getValue(0),
                        columnName, aCriterionEntry.getValue(1));
                case NOT_BETWEEN:
                    return String.format(" (NOT ((%s > %s) AND (%s < %s)))", columnName, aCriterionEntry.getValue(0),
                                         columnName, aCriterionEntry.getValue(1));
                case BETWEEN_INCLUSIVE:
                    return String.format(" ((%s >= %s) AND (%s <= %s))", columnName, aCriterionEntry.getValue(0),
                                         columnName, aCriterionEntry.getValue(1));
                case IN:
                    if (dsCriterion.isMultiValue())
                    {
                        boolean isFirst = true;
                        StringBuilder sqlBuilder = new StringBuilder(" " + columnName + " IN (");
                        for (String mValue : dsCriterion.getValues())
                        {
                            if (isFirst)
                            {
                                isFirst = false;
                                sqlBuilder.append(mValue);
                            }
                            else
                            {
                                sqlBuilder.append(StrUtl.CHAR_COMMA);
                                sqlBuilder.append(mValue);
                            }
                        }
                        sqlBuilder.append(StrUtl.CHAR_PAREN_CLOSE);

                        return sqlBuilder.toString();
                    }
                    else
                        throw new NSException(String.format("[%s] Must be multi-value: %s",
                                                            aCriterionEntry.getName(),
                                                            aCriterionEntry.getLogicalOperator()));
                case NOT_IN:
                    if (dsCriterion.isMultiValue())
                    {
                        boolean isFirst = true;
                        StringBuilder sqlBuilder = new StringBuilder(" " + columnName + " NOT IN (");
                        for (String mValue : dsCriterion.getValues())
                        {
                            if (isFirst)
                            {
                                isFirst = false;
                                sqlBuilder.append(mValue);
                            }
                            else
                            {
                                sqlBuilder.append(StrUtl.CHAR_COMMA);
                                sqlBuilder.append(mValue);
                            }
                        }
                        sqlBuilder.append(StrUtl.CHAR_PAREN_CLOSE);

                        return sqlBuilder.toString();
                    }
                    else
                        throw new NSException(String.format("[%s] Must be multi-value: %s",
                                                            aCriterionEntry.getName(),
                                                            aCriterionEntry.getLogicalOperator()));
                default:
                    throw new NSException(String.format("[%s] Unsupported condition operator: %s",
                                                        aCriterionEntry.getName(),
                                                        aCriterionEntry.getLogicalOperator()));
            }
        }
        else if (dataField.isTypeDateOrTime())
        {
            DataField dataField2;
            String escapeDateTime2;
            String escapeDateTime1 = escapeTimestamp(dataField.getValueAsDate().getTime());

            switch (aCriterionEntry.getLogicalOperator())
            {
                case EQUAL:
                    return String.format(" (%s = %s)", columnName, escapeDateTime1);
                case NOT_EQUAL:
                    return String.format(" (%s != %s)", columnName, escapeDateTime1);
                case GREATER_THAN:
                    return String.format(" (%s > %s)", columnName, escapeDateTime1);
                case GREATER_THAN_EQUAL:
                    return String.format(" (%s >= %s)", columnName, escapeDateTime1);
                case LESS_THAN:
                    return String.format(" (%s < %s)", columnName, escapeDateTime1);
                case LESS_THAN_EQUAL:
                    return String.format(" (%s <= %s)", columnName, escapeDateTime1);
                case BETWEEN:
                    dataField2 = new DataDateTimeField(dataField.getName(), dataField.getTitle(),
                                                       aCriterionEntry.getValue(1));
                    escapeDateTime2 = escapeTimestamp(dataField2.getValueAsDate().getTime());
                    return String.format(" ((%s > %s) AND (%s < %s))", columnName, escapeDateTime1,
                                         columnName, escapeDateTime2);
                case NOT_BETWEEN:
                    dataField2 = new DataDateTimeField(dataField.getName(), dataField.getTitle(),
                                                       aCriterionEntry.getValue(1));
                    escapeDateTime2 = escapeTimestamp(dataField2.getValueAsDate().getTime());
                    return String.format(" (NOT ((%s > %s) AND (%s < %s)))", columnName, escapeDateTime1,
                                         columnName, escapeDateTime2);
                case BETWEEN_INCLUSIVE:
                    dataField2 = new DataDateTimeField(dataField.getName(), dataField.getTitle(),
                                                       aCriterionEntry.getValue(1));
                    escapeDateTime2 = escapeTimestamp(dataField2.getValueAsDate().getTime());
                    return String.format(" ((%s >= %s) AND (%s <= %s))", columnName, escapeDateTime1,
                                         columnName, escapeDateTime2);
                case IN:
                    if (dsCriterion.isMultiValue())
                    {
                        String escapeDateTime;
                        boolean isFirst = true;
                        StringBuilder sqlBuilder = new StringBuilder(" " + columnName + " IN (");
                        for (String mValue : dsCriterion.getValues())
                        {
                            dataField = new DataDateTimeField(dataField.getName(), dataField.getTitle(), mValue);
                            escapeDateTime = escapeTimestamp(dataField.getValueAsDate().getTime());
                            if (isFirst)
                            {
                                isFirst = false;
                                sqlBuilder.append(escapeDateTime);
                            }
                            else
                            {
                                sqlBuilder.append(StrUtl.CHAR_COMMA);
                                sqlBuilder.append(escapeDateTime);
                            }
                        }
                        sqlBuilder.append(StrUtl.CHAR_PAREN_CLOSE);

                        return sqlBuilder.toString();
                    }
                    else
                        throw new NSException(String.format("[%s] Must be multi-value: %s",
                                                            aCriterionEntry.getName(),
                                                            aCriterionEntry.getLogicalOperator()));
                case NOT_IN:
                    if (dsCriterion.isMultiValue())
                    {
                        String escapeDateTime;
                        boolean isFirst = true;
                        StringBuilder sqlBuilder = new StringBuilder(" " + columnName + " NOT IN (");
                        for (String mValue : dsCriterion.getValues())
                        {
                            dataField = new DataDateTimeField(dataField.getName(), dataField.getTitle(), mValue);
                            escapeDateTime = escapeTimestamp(dataField.getValueAsDate().getTime());
                            if (isFirst)
                            {
                                isFirst = false;
                                sqlBuilder.append(escapeDateTime);
                            }
                            else
                            {
                                sqlBuilder.append(StrUtl.CHAR_COMMA);
                                sqlBuilder.append(escapeDateTime);
                            }
                        }
                        sqlBuilder.append(StrUtl.CHAR_PAREN_CLOSE);

                        return sqlBuilder.toString();
                    }
                    else
                        throw new NSException(String.format("[%s] Must be multi-value: %s",
                                              aCriterionEntry.getName(), aCriterionEntry.getLogicalOperator()));
                default:
                    throw new NSException(String.format("[%s] Unsupported condition operator: %s",
                        aCriterionEntry.getName(), aCriterionEntry.getLogicalOperator()));
            }
        }
        else if (dataField.isTypeBoolean())
        {
            switch (aCriterionEntry.getLogicalOperator())
            {
                case EQUAL:
                    return String.format(" (%s = %s)", columnName, aCriterionEntry.getValue());
                case NOT_EQUAL:
                    return String.format(" (%s != %s)", columnName, aCriterionEntry.getValue());
                default:
                    throw new NSException(String.format("[%s] Unsupported condition operator: %s",
                                          aCriterionEntry.getName(), aCriterionEntry.getLogicalOperator()));
            }
        }
        else
            throw new NSException(String.format("[%s] Unsupported type: %s",
                dataField.getName(), dataField.getType()));
    }

    /**
     * Appends order by clauses for any fields in the data field
     * bag that require sorting.
     *
     * @param aStringBuilder String builder used to hold order by clauses.
     * @param aBag Bag of fields to examine.
     *
     * @throws NSException Catch-all exception for any SQL related issue.
     */
    @SuppressWarnings({"StringConcatenationInsideStringBufferAppend"})
    public void orderByClause(StringBuilder aStringBuilder, DataBag aBag)
        throws NSException
    {
        boolean isFirstOrder = true;
        for (DataField pField : aBag.getFields())
        {
            if (pField.isSorted())
            {
                if (isFirstOrder)
                {
                    isFirstOrder = false;
                    aStringBuilder.append(" ORDER BY");
                }
                if (pField.getSortOrder() == Field.Order.ASCENDING)
                    aStringBuilder.append(" " + columnName(pField.getName()) + SQL.SORT_ORDER_ASCEND);
                else
                    aStringBuilder.append(" " + columnName(pField.getName()) + SQL.SORT_ORDER_DESCEND);
            }
        }
    }

    /**
     * Returns the order by clauses for any fields in the data field
     * bag that require sorting.
     *
     * @param aBag Bag of fields to examine.
     *
     * @return String representation of the order by clause.
     *
     * @throws NSException Catch-all exception for any SQL related issue.
     */
    public String orderByClause(DataBag aBag)
        throws NSException
    {
        StringBuilder stringBuilder = new StringBuilder();
        orderByClause(stringBuilder, aBag);
        if (stringBuilder.length() > 0)
            return stringBuilder.toString();
        else
            return StringUtils.EMPTY;
    }

    /**
     * Returns the order by clauses for any fields in the data field
     * bag that require sorting.
     *
     * @param aStatement Initial statement that requires appending.
     * @param aBag       Bag of fields to examine.
     *
     * @return String representation of the order by clause.
     *
     * @throws NSException Catch-all exception for any SQL related issue.
     */
    public String orderByClause(String aStatement, DataBag aBag)
        throws NSException
    {
        StringBuilder stringBuilder = new StringBuilder(aStatement);
        orderByClause(stringBuilder, aBag);

        return stringBuilder.toString();
    }

    /**
     * Returns the count of fields in the bag that have been assigned a
     * value.
     *
     * @param aBag Bag of fields.
     *
     * @return Assigned value field count.
     */
    public int assignedFieldsCount(DataBag aBag)
    {
        int count = 0;
        for (DataField pField : aBag.getFields())
        {
            if ((! pField.getName().equals(SQL.COLUMN_ID_FIELD_NAME)) && (pField.isAssigned()))
                count++;
        }

        return count;
    }

    /**
     * Clears all changes made since the previous commit/rollback
     * operation and releases any database locks currently held by
     * {<i>SQLConnection</i>. This method should be used only when
     * the "jdbc_autocommit" property is assigned <i>false</i>.
     *
     * @throws NSException Catch-all exception for any SQL related issue.
     */
    public void rollback()
        throws NSException
    {
        mSQLConnection.rollback();
    }

    /**
     * Applies all changes made since the previous commit/rollback
     * permanent and releases any database locks currently held by
     * <i>SQLConnection</i>. This method should be used only when
     * the "jdbc_autocommit" property is assigned <i>false</i>.
     *
     * @throws NSException Catch-all exception for any SQL related issue.
     */
    public void commit()
        throws NSException
    {
        mSQLConnection.commit();
    }

    /**
     * Creates a table object in the RDBMS based on the DB name
     * assigned to the bag.
     *
     * @param aBag Field bag with DB name assigned.
     *
     * @throws NSException Catch-all exception for any SQL related issue.
     */
    public abstract void create(DataBag aBag) throws NSException;

    /**
     * Drops a table object in the RDBMS based on the DB name
     * assigned to the bag.
     *
     * @param aBag Field bag with DB name assigned.
     *
     * @throws NSException Catch-all exception for any SQL related issue.
     */
    public void drop(DataBag aBag)
        throws NSException
    {
        AppMgr appMgr = mSQLConnection.getAppMgr();
        Logger appLogger = appMgr.getLogger(this, "drop");

        appLogger.trace(appMgr.LOGMSG_TRACE_ENTER);

        String tableName = schemaName(aBag);
        mSQLConnection.execute(String.format("DROP TABLE %s", tableName));

        appLogger.trace(appMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Inserts the fields within the data bag into the RDBMS
     * table (based on the DB name assigned to the bag).  If the
     * primary key field is designated as an auto-incremented
     * sequence, then that scenario will be handled as part of
     * this operation.
     *
     * @param aBag Field bag with DB name assigned.
     *
     * @throws NSException Catch-all exception for any SQL related issue.
     */
    public abstract void insert(DataBag aBag) throws NSException;

    /**
     * Returns the count of rows in the RDBMS table identified by the
     * DB name assigned to the data bag of fields.
     *
     * @param aBag Data field bag with DB name assigned and a
     *             primary key designated.
     *
     * @return Count of rows in the table.
     *
     * @throws NSException Catch-all exception for any SQL related issue.
     */
    public abstract int count(DataBag aBag) throws NSException;

    /**
     * Returns a count of rows in the RDBMS table identified by the
     * DB name that match the <i>DSCriteria</i>.
     *
     * @param aBag Data field bag.
     * @param aCriteria Data source criteria.
     *
     * @return Count of rows in the RDBMS table matching the criteria.
     *
     * @throws NSException Catch-all exception for any SQL related issue.
     */
    public abstract int count(DataBag aBag, DSCriteria aCriteria) throws NSException;

    /**
     * Returns a <i>DataTable</i> representation of all rows
     * fetched from the RDBMS table (using a wildcard criteria).
     * <p>
     * <b>Note:</b> Depending on the number of rows in the RDBMS
     * table, this method could consume large amounts of heap
     * memory.  Therefore, it should only be used when the number
     * of column and rows is known to be small in size.
     * </p>
     *
     * @param aBag Data field bag.
     *
     * @return Table representing all rows in the RDBMS table.
     *
     * @throws NSException Catch-all exception for any SQL related issue.
     */
    public abstract DataTable select(DataBag aBag) throws NSException;

    /**
     * Returns a <i>DataTable</i> representation of all rows
     * fetched from the RDBMS table that match the criteria
     * provided.
     * <p>
     * <b>Note:</b> Depending on the number of rows in the RDBMS
     * table, this method could consume large amounts of heap
     * memory.  Therefore, the developer is encouraged to use
     * the alternative method for select where an offset and
     * limit parameter can be specified.
     * </p>
     *
     * @param aBag Data field bag.
     * @param aCriteria Data source criteria.
     * @return Table representing all rows in the RDBMS table
     * matching the criteria.
     *
     * @throws NSException Catch-all exception for any SQL related issue.
     */
    public abstract DataTable select(DataBag aBag, DSCriteria aCriteria) throws NSException;

    /**
     * Returns a <i>DataTable</i> representation of all rows
     * fetched from the RDBMS table that match the criteria
     * provided. In addition, this method offers a paging mechanism
     * where the starting offset and a fetch limit can be applied
     * to each operation.
     *
     * @param aBag Data field bag.
     * @param aCriteria Data source criteria.
     * @param anOffset Starting offset into the matching table rows.
     * @param aLimit Limit on the total number of rows to fetch from
     *               the RDBMS table during this select operation.
     *
     * @return Table representing all rows that match the criteria
     * in the RDBMS table (based on the offset and limit values).
     *
     * @throws NSException Catch-all exception for any SQL related issue.
     */
    public abstract DataTable select(DataBag aBag, DSCriteria aCriteria,
                                     int anOffset, int aLimit) throws NSException;

    /**
     * Returns a <i>DataTable</i> representation of all rows
     * fetched from the RDBMS table that match the SQL where clause
     * provided.
     * <p>
     * <b>Note:</b> The developer is responsible for ensuring that the
     * where clause is properly formatted for the RDBMS vendor it will
     * be executed against.
     * </p>
     *
     * @param aBag Data field bag.
     * @param aWhereClause SQL where clause.
     *
     * @return Table representing all rows that match the where
     * clause in the RDBMS table.
     *
     * @throws NSException Catch-all exception for any SQL related issue.
     */
    public abstract DataTable select(DataBag aBag, String aWhereClause) throws NSException;

    /**
     * Returns a low-level JDBC <i>ResultSet</i> representation of all rows
     * fetched from the RDBMS table that match the SQL select statement
     * provided.
     * <p>
     * <b>Note:</b> The developer is responsible for ensuring that the
     * SQL statement is properly formatted for the RDBMS vendor it will
     * be executed against.
     * </p>
     *
     * @param aSelectFromWhereStatement SQL select statement.
     *
     * @return JDBC result set instance.
     *
     * @throws NSException Catch-all exception for any SQL related issue.
     */
    public abstract ResultSet select(String aSelectFromWhereStatement) throws NSException;

    /**
     * Updates one or more rows in the RDBMS table matching the
     * <i>DSCriteria</i> with the assigned values in the
     * <i>DataBag</i>.
     *
     * @param aBag Data field bag with DB name.
     * @param aCriteria Data source criteria.
     *
     * @throws NSException Catch-all exception for any SQL related issue.
     */
    public abstract void update(DataBag aBag, DSCriteria aCriteria) throws NSException;

    /**
     * Updates a single row in the RDBMS table that matches the
     * primary key field in the <i>DataBag</i> with any fields
     * that have assigned values.
     *
     * @param aBag Data field bag with DB name assigned and a
     *             primary key designated.
     *
     * @throws NSException Catch-all exception for any SQL related issue.
     */
    public abstract void update(DataBag aBag) throws NSException;

    /**
     * Deletes a single row in the RDBMS table that matches the
     * primary key field in the <i>DataBag</i>.
     *
     * @param aBag Data field bag with DB name assigned and a
     *             primary key designated.
     *
     * @throws NSException Catch-all exception for any SQL related issue.
     */
    public abstract void delete(DataBag aBag) throws NSException;

    /**
     * Deletes one or more rows in the RDBMS table matching the
     * <i>DSCriteria</i>.  The <i>DataBag</i> is used to
     * determine the name of the DB table.
     *
     * @param aBag Data field bag with DB name.
     * @param aCriteria Data source criteria.
     *
     * @throws NSException Catch-all exception for any SQL related issue.
     */
    public abstract void delete(DataBag aBag, DSCriteria aCriteria) throws NSException;
}
