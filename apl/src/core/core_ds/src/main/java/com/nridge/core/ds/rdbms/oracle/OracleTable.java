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

package com.nridge.core.ds.rdbms.oracle;

import com.nridge.core.app.mgr.AppMgr;
import com.nridge.core.base.ds.DSCriteria;
import com.nridge.core.base.ds.DSCriterionEntry;
import com.nridge.core.base.field.Field;
import com.nridge.core.base.field.FieldRow;
import com.nridge.core.base.field.data.DataBag;
import com.nridge.core.base.field.data.DataField;
import com.nridge.core.base.field.data.DataTable;
import com.nridge.core.base.std.NSException;
import com.nridge.core.base.std.StrUtl;
import com.nridge.core.ds.rdbms.SQL;
import com.nridge.core.ds.rdbms.SQLConnection;
import com.nridge.core.ds.rdbms.SQLSequence;
import com.nridge.core.ds.rdbms.SQLTable;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Implements the Oracle RDBMS interfaces for table
 * operations.  Since these methods are specific to a
 * particular RDBMS vendor, an application developer
 * is encouraged to use the abstracted <i>SQLTable</i>
 * class instead.
 *
 * @author Al Cole
 * @since 1.0
 */
public class OracleTable extends SQLTable
{
    private static final int VENDOR_CHAR_THRESHOLD = 5;
    private static final int VENDOR_VARCHAR_THRESHOLD = 4000;

    private AppMgr mAppMgr;

    /**
     * Constructor that accepts a SQL connection.
     *
     * @param aConnection SQL connection.
     */
    public OracleTable(SQLConnection aConnection)
    {
        super(aConnection);
        mAppMgr = aConnection.getAppMgr();
    }

// http://docs.oracle.com/cd/E11882_01/server.112/e17118/sql_elements001.htm#i54330

    private String columnElement(DataField aField)
    {
        String sqlElement;

        String fieldName = aField.getName();
        int storageSize = aField.getFeatureAsInt(Field.FEATURE_STORED_SIZE);
        switch(aField.getType())
        {
            case Boolean:
                sqlElement = String.format("%s NUMBER(1)", fieldName);
                break;
            case Long:
            case Integer:
                sqlElement = String.format("%s NUMBER", fieldName);
                break;
            case Float:
                sqlElement = String.format("%s BINARY_FLOAT", fieldName);
                break;
            case Double:
                sqlElement = String.format("%s BINARY_DOUBLE", fieldName);
                break;
            case Text:
                if (storageSize < VENDOR_CHAR_THRESHOLD)
                    sqlElement = String.format("%s CHAR(%d)", fieldName, storageSize);
                else if (storageSize < VENDOR_VARCHAR_THRESHOLD)
                    sqlElement = String.format("%s VARCHAR(%d)", fieldName, storageSize);
                else
                    sqlElement = String.format("%s CLOB", fieldName);
                break;
            case Date:
            case Time:
                sqlElement = String.format("%s DATE", fieldName);
                break;
            case DateTime:
                sqlElement = String.format("%s TIMESTAMP", fieldName);
                break;
            default:
                return fieldName;
        }

        if (aField.isFeatureTrue(Field.FEATURE_IS_PRIMARY_KEY))
            sqlElement += String.format(" PRIMARY KEY");
        if (aField.isFeatureTrue(Field.FEATURE_IS_UNIQUE))
            sqlElement += " UNIQUE";
        if ((aField.isFeatureTrue(Field.FEATURE_IS_REQUIRED)) && (aField.isFeatureFalse(Field.FEATURE_IS_PRIMARY_KEY)))
            sqlElement += " NOT NULL";

        return sqlElement;
    }

    private void createSequences(DataBag aBag)
        throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "createSequences");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        SQLSequence sqlSequence = mSQLConnection.newSequence();
        for (DataField pField : aBag.getFields())
        {
            if (SQL.isSequenceManaged(pField))
                sqlSequence.create(aBag, pField);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Creates a table object in the RDBMS based on the DB name
     * assigned to the bag.
     *
     * @param aBag Field bag with DB name assigned.
     *
     * @throws com.nridge.core.base.std.NSException Catch-all exception for any SQL related issue.
     */
    @Override
    public void create(DataBag aBag)
        throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "create");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if ((aBag == null) || (aBag.count() == 0))
            throw new NSException("Empty persistent bag.");

        String sqlStatement = "CREATE TABLE " + schemaName(aBag) + StrUtl.CHAR_SPACE;

        boolean isCommaNeeded = false;
        StringBuilder sqlBuilder = new StringBuilder(sqlStatement);
        sqlBuilder.append(StrUtl.CHAR_LEFTPAREN);
        for (DataField pField : aBag.getFields())
        {
            if (isCommaNeeded)
                sqlBuilder.append(StrUtl.CHAR_COMMA);
            else
                isCommaNeeded = true;
            sqlBuilder.append(columnElement(pField));
        }
        sqlBuilder.append(StrUtl.CHAR_RIGHTPAREN);

        String tableSpace = aBag.getFeature(Field.FEATURE_TABLESPACE_NAME);
        if (StringUtils.isNotEmpty(tableSpace))
            sqlBuilder.append(String.format(" TABLESPACE %s", tableSpace));

        mSQLConnection.execute(sqlBuilder.toString());

        createSequences(aBag);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    private void dropSequences(DataBag aBag)
        throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "dropSequences");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        SQLSequence sqlSequence = mSQLConnection.newSequence();
        for (DataField pField : aBag.getFields())
        {
            if (SQL.isSequenceManaged(pField))
                sqlSequence.drop(aBag, pField);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Drops a table object in the RDBMS based on the DB name
     * assigned to the bag.
     *
     * @param aBag Field bag with DB name assigned.
     *
     * @throws NSException Catch-all exception for any SQL related issue.
     */
    @Override
    public void drop(DataBag aBag)
        throws NSException
    {
        super.drop(aBag);
        dropSequences(aBag);
    }

    private String buildSelectFrom(DataBag aBag)
        throws NSException
    {
        String sqlStatement = "SELECT ";
        boolean isCommaNeeded = false;
        StringBuilder sqlBuilder = new StringBuilder(sqlStatement);
        for (DataField pField : aBag.getFields())
        {
            if (isCommaNeeded)
                sqlBuilder.append(StrUtl.CHAR_COMMA);
            else
                isCommaNeeded = true;
            if (StringUtils.isNotEmpty(pField.getFeature(Field.FEATURE_FUNCTION_NAME)))
                sqlBuilder.append(functionColumnName(pField.getFeature(Field.FEATURE_FUNCTION_NAME), pField.getName()));
            else
                sqlBuilder.append(columnName(pField.getName()));
        }
        sqlBuilder.append(" FROM ");
        sqlBuilder.append(schemaName(aBag));

        return sqlBuilder.toString();
    }

// http://www.oracle.com/technetwork/issue-archive/2006/06-sep/o56asktom-086197.html

    @SuppressWarnings({"StringConcatenationInsideStringBufferAppend"})
    private String buildWhereClause(DataBag aBag, DSCriteria aCriteria)
        throws NSException
    {
        StringBuilder sqlBuilder = new StringBuilder();

        if ((aCriteria != null) && (aCriteria.count() > 0))
        {
            boolean isFirst = true;
            sqlBuilder.append(" WHERE");
            for (DSCriterionEntry ce : aCriteria.getCriterionEntries())
            {
                if (isFirst)
                {
                    isFirst = false;
                    sqlBuilder.append(columnCondition(ce));
                }
                else
                {
                    if (ce.getBooleanOperator() == Field.Operator.AND)
                        sqlBuilder.append(" AND ");
                    else
                        sqlBuilder.append(" OR ");
                    sqlBuilder.append(columnCondition(ce));
                }
            }
        }

        orderByClause(sqlBuilder, aBag);

// Note: Oracle requires that offset and limit handling must be outside this method.

        return sqlBuilder.toString();
    }

    private void addTableRowFromResultSet(DataTable aTable, ResultSet aResultSet)
    {
        String columnName;
        DataField dataField;
        Logger appLogger = mAppMgr.getLogger(this, "addTableRowFromResultSet");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        FieldRow fieldRow = aTable.newRow();

        for (DataField pField : aTable.getColumnBag().getFields())
        {
            dataField = new DataField(pField);

            try
            {
                columnName = columnName(pField.getName());

                switch (pField.getType())
                {
                    case Integer:
                        dataField.setValue(aResultSet.getInt(columnName));
                        break;
                    case Long:
                        dataField.setValue(aResultSet.getLong(columnName));
                        break;
                    case Float:
                        dataField.setValue(aResultSet.getFloat(columnName));
                        break;
                    case Double:
                        dataField.setValue(aResultSet.getDouble(columnName));
                        break;
                    case Boolean:
                        dataField.setValue(aResultSet.getBoolean(columnName));
                        break;
                    case Date:
                        dataField.setValue(aResultSet.getDate(columnName));
                        break;
                    case Time:
                        dataField.setValue(aResultSet.getTime(columnName));
                        break;
                    case DateTime:
                        dataField.setValue(aResultSet.getTimestamp(columnName));
                        break;
                    default:
                        dataField.setValue(aResultSet.getString(columnName));
                        break;
                }

                if (! aResultSet.wasNull())
                    aTable.setValueByName(fieldRow, pField.getName(), dataField.getValue());
            }
            catch (SQLException e)
            {
                appLogger.error(String.format("SQL Exception (%s): %s", pField.getName(), e.getMessage()));
            }
            catch (NSException e)
            {
                appLogger.error(String.format("NS Exception (%s): %s", pField.getName(), e.getMessage()));
            }
        }

        aTable.addRow(fieldRow);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    private void query(String aSQLStatement, DataTable aTable, int aLimit)
        throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "query");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        Statement stmtQuery = null;
        appLogger.debug(aSQLStatement);
        Connection jdbcConnection = mSQLConnection.getJDBCConnection();
        try
        {
            stmtQuery = jdbcConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                                                       ResultSet.CONCUR_READ_ONLY);
            if (aLimit > 0)
                stmtQuery.setFetchSize(aLimit);
            stmtQuery.setEscapeProcessing(mSQLConnection.isStatementEscapingEnabled());
            mSQLConnection.setLastStatement(aSQLStatement);
            ResultSet resultSet = stmtQuery.executeQuery(aSQLStatement);
            while (resultSet.next())
                addTableRowFromResultSet(aTable, resultSet);
        }
        catch (SQLException e)
        {
            throw new NSException("RDBMS Query Error: " + aSQLStatement + " : " + e.getMessage(), e);
        }
        finally
        {
            if (stmtQuery != null)
            {
                try { stmtQuery.close(); } catch (SQLException ignored) { }
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    private void addTableRowFromFunctionResultSet(DataTable aTable, ResultSet aResultSet)
    {
        DataField dataField;
        Logger appLogger = mAppMgr.getLogger(this, "addTableRowFromFunctionResultSet");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        FieldRow fieldRow = aTable.newRow();

        int columnNumber = 0;
        for (DataField pField : aTable.getColumnBag().getFields())
        {
            columnNumber++;
            dataField = new DataField(pField);

            try
            {
                switch (pField.getType())
                {
                    case Integer:
                        dataField.setValue(aResultSet.getInt(columnNumber));
                        break;
                    case Long:
                        dataField.setValue(aResultSet.getLong(columnNumber));
                        break;
                    case Float:
                        dataField.setValue(aResultSet.getFloat(columnNumber));
                        break;
                    case Double:
                        dataField.setValue(aResultSet.getDouble(columnNumber));
                        break;
                    case Boolean:
                        dataField.setValue(aResultSet.getBoolean(columnNumber));
                        break;
                    case Date:
                        dataField.setValue(aResultSet.getDate(columnNumber));
                        break;
                    case Time:
                        dataField.setValue(aResultSet.getTime(columnNumber));
                        break;
                    case DateTime:
                        dataField.setValue(aResultSet.getTimestamp(columnNumber));
                        break;
                    default:
                        dataField.setValue(aResultSet.getString(columnNumber));
                        break;
                }

                if (! aResultSet.wasNull())
                    aTable.setValueByName(fieldRow, pField.getName(), dataField.getValue());
            }
            catch (SQLException e)
            {
                appLogger.error(String.format("SQL Exception (%s): %s", pField.getName(), e.getMessage()));
            }
        }

        aTable.addRow(fieldRow);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    private void queryFunction(String aSQLStatement, DataTable aTable, int aLimit)
        throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "queryFunction");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        Statement stmtQuery = null;
        appLogger.debug(aSQLStatement);
        Connection jdbcConnection = mSQLConnection.getJDBCConnection();
        try
        {
            stmtQuery = jdbcConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                                                       ResultSet.CONCUR_READ_ONLY);
            if (aLimit > 0)
                stmtQuery.setFetchSize(aLimit);
            stmtQuery.setEscapeProcessing(mSQLConnection.isStatementEscapingEnabled());
            mSQLConnection.setLastStatement(aSQLStatement);
            ResultSet resultSet = stmtQuery.executeQuery(aSQLStatement);
            while (resultSet.next())
                addTableRowFromFunctionResultSet(aTable, resultSet);
        }
        catch (SQLException e)
        {
            throw new NSException("RDBMS Query Error: " + aSQLStatement + " : " + e.getMessage(), e);
        }
        finally
        {
            if (stmtQuery != null)
            {
                try { stmtQuery.close(); } catch (SQLException ignored) { }
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
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
     * @throws com.nridge.core.base.std.NSException Catch-all exception for any SQL related issue.
     */
    @Override
    public void insert(DataBag aBag)
        throws NSException
    {
        String fieldValue;
        Logger appLogger = mAppMgr.getLogger(this, "insert");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if ((aBag == null) || (aBag.count() == 0))
            throw new NSException("Empty persistent bag.");

        boolean isCommaNeeded = false;
        StringBuilder sqlBuilder = new StringBuilder(String.format("INSERT INTO %s ", schemaName(aBag)));

        sqlBuilder.append(StrUtl.CHAR_LEFTPAREN);
        for (DataField pField : aBag.getFields())
        {
            if (isCommaNeeded)
                sqlBuilder.append(StrUtl.CHAR_COMMA);
            else
                isCommaNeeded = true;
            sqlBuilder.append(columnName(pField.getName()));
        }
        sqlBuilder.append(StrUtl.CHAR_RIGHTPAREN);

        isCommaNeeded = false;
        sqlBuilder.append(" VALUES ");

        sqlBuilder.append(StrUtl.CHAR_LEFTPAREN);
        for (DataField pField : aBag.getFields())
        {
            if (isCommaNeeded)
                sqlBuilder.append(StrUtl.CHAR_COMMA);
            else
                isCommaNeeded = true;

            fieldValue = pField.getValue();
            if (StringUtils.isEmpty(fieldValue))
            {
                if (SQL.isSequenceManaged(pField))
                {
                    SQLSequence sqlSequence = mSQLConnection.newSequence();
                    sqlBuilder.append(sqlSequence.insertValue(aBag, pField));
                }
                else
                    sqlBuilder.append(SQL.COLUMN_VALUE_EMPTY);
            }
            else
            {
                if (pField.isTypeText())
                {
                    sqlBuilder.append(StrUtl.CHAR_SGLQUOTE);
                    sqlBuilder.append(escapeText(fieldValue));
                    sqlBuilder.append(StrUtl.CHAR_SGLQUOTE);
                }
                else if (pField.isTypeBoolean())
                {
                    if (pField.isValueTrue())
                        sqlBuilder.append("1");
                    else
                        sqlBuilder.append("0");
                }
                else if (pField.isTypeDateOrTime())
                    sqlBuilder.append(escapeTimestamp(pField.getValueAsDate().getTime()));
                else
                    sqlBuilder.append(fieldValue);
            }
        }
        sqlBuilder.append(StrUtl.CHAR_RIGHTPAREN);

        mSQLConnection.execute(sqlBuilder.toString());

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Returns the count of rows in the RDBMS table identified by the
     * DB name assigned to the data bag of fields.
     *
     * @param aBag Data field bag with DB name assigned and a
     *             primary key designated.
     *
     * @return Count of rows in the table.
     *
     * @throws com.nridge.core.base.std.NSException Catch-all exception for any SQL related issue.
     */
    @Override
    public int count(DataBag aBag) throws NSException
    {
        return count(aBag, null);
    }

    /**
     * Returns a count of rows in the RDBMS table identified by the
     * DB name that match the {@link com.nridge.core.base.ds.DSCriteria}.
     *
     * @param aBag      Data field bag.
     * @param aCriteria Data source criteria.
     *
     * @return Count of rows in the RDBMS table matching the criteria.
     *
     * @throws com.nridge.core.base.std.NSException Catch-all exception for any SQL related issue.
     */
    @Override
    public int count(DataBag aBag, DSCriteria aCriteria)
        throws NSException
    {
        int countValue = SQL.VALUE_IS_INVALID;
        Logger appLogger = mAppMgr.getLogger(this, "count");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        DataBag countBag = new DataBag(aBag.getName(), aBag.getTitle());
        DataField dataField = new DataField(SQL.COLUMN_ID_FIELD_NAME, "Id", 0);
        dataField.addFeature(Field.FEATURE_FUNCTION_NAME, SQL.FUNCTION_COLUMN_COUNT);
        countBag.add(dataField);

        String sqlStatement = buildSelectFrom(countBag);

        if (aCriteria != null)
            sqlStatement += buildWhereClause(countBag, aCriteria);

        DataTable dataTable = new DataTable(countBag);
        queryFunction(sqlStatement, dataTable, SQL.CRITERIA_NO_LIMITS);
        dataField = dataTable.getFieldByRowCol(0, 0);
        if (dataField != null)
            countValue = dataField.getValueAsInt();

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return countValue;
    }

    /**
     * Returns a {@link com.nridge.core.base.field.data.DataTable} representation of all rows
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
     * @throws com.nridge.core.base.std.NSException Catch-all exception for any SQL related issue.
     */
    @Override
    public DataTable select(DataBag aBag)
        throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "select");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        String sqlStatement = buildSelectFrom(aBag);

        sqlStatement += orderByClause(aBag);

        DataTable persistTable = new DataTable(aBag);

        if (aBag.featureNameCount(Field.FEATURE_FUNCTION_NAME) == 0)
            query(sqlStatement, persistTable, SQL.CRITERIA_NO_LIMITS);
        else
            queryFunction(sqlStatement, persistTable, SQL.CRITERIA_NO_LIMITS);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return persistTable;
    }

    /**
     * Returns a {@link com.nridge.core.base.field.data.DataTable} representation of all rows
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
     * @param aBag      Data field bag.
     * @param aCriteria Data source criteria.
     *
     * @return Table representing all rows in the RDBMS table
     * matching the criteria.
     *
     * @throws com.nridge.core.base.std.NSException Catch-all exception for any SQL related issue.
     */
    @Override
    public DataTable select(DataBag aBag, DSCriteria aCriteria) throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "select");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        String sqlStatement = buildSelectFrom(aBag);

        if (aCriteria != null)
            sqlStatement += buildWhereClause(aBag, aCriteria);

        DataTable dataTable = new DataTable(aBag);

        if (aBag.featureNameCount(Field.FEATURE_FUNCTION_NAME) == 0)
            query(sqlStatement, dataTable, SQL.CRITERIA_NO_LIMITS);
        else
            queryFunction(sqlStatement, dataTable, SQL.CRITERIA_NO_LIMITS);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return dataTable;
    }

    /**
     * Returns a {@link com.nridge.core.base.field.data.DataTable} representation of all rows
     * fetched from the RDBMS table that match the criteria
     * provided. In addition, this method offers a paging mechanism
     * where the starting offset and a fetch limit can be applied
     * to each operation.
     *
     * @param aBag      Data field bag.
     * @param aCriteria Data source criteria.
     * @param anOffset  Starting offset into the matching table rows.
     * @param aLimit    Limit on the total number of rows to fetch from
     *                  the RDBMS table during this select operation.
     *
     * @return Table representing all rows that match the criteria
     * in the RDBMS table (based on the offset and limit values).
     *
     * @throws com.nridge.core.base.std.NSException Catch-all exception for any SQL related issue.
     */
    @Override
    public DataTable select(DataBag aBag, DSCriteria aCriteria, int anOffset, int aLimit)
        throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "select");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        String sqlStatement = buildSelectFrom(aBag);

        if (aCriteria != null)
            sqlStatement += buildWhereClause(aBag, aCriteria);

        DataTable dataTable = new DataTable(aBag);

        if (aBag.featureNameCount(Field.FEATURE_FUNCTION_NAME) == 0)
        {
            if ((anOffset >= 0) && (aLimit >= 0))
            {
                StringBuilder sqlBuilder = new StringBuilder("SELECT * FROM (SELECT a.*, rownum rnum FROM (");
                sqlBuilder.append(sqlStatement);
                sqlBuilder.append(String.format(") a WHERE rownum <= %d) WHERE rnum >= %d", aLimit, anOffset));
                sqlStatement = sqlBuilder.toString();
            }
            else
                sqlStatement += String.format("WHERE rownum <= %d", aLimit);

            query(sqlStatement, dataTable, aLimit);
        }
        else
            queryFunction(sqlStatement, dataTable, aLimit);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return dataTable;
    }

    /**
     * Returns a {@link com.nridge.core.base.field.data.DataTable} representation of all rows
     * fetched from the RDBMS table that match the SQL where clause
     * provided.
     * <p>
     * <b>Note:</b> The developer is responsible for ensuring that the
     * where clause is properly formatted for the RDBMS vendor it will
     * be executed against.
     * </p>
     *
     * @param aBag         Data field bag.
     * @param aWhereClause SQL where clause.
     *
     * @return Table representing all rows that match the where
     * clause in the RDBMS table.
     *
     * @throws com.nridge.core.base.std.NSException Catch-all exception for any SQL related issue.
     */
    @Override
    public DataTable select(DataBag aBag, String aWhereClause) throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "select");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        String sqlStatement = buildSelectFrom(aBag) + " " + aWhereClause;

        DataTable dataTable = new DataTable(aBag);

        if (aBag.featureNameCount(Field.FEATURE_FUNCTION_NAME) == 0)
            query(sqlStatement, dataTable, SQL.CRITERIA_NO_LIMITS);
        else
            queryFunction(sqlStatement, dataTable, SQL.CRITERIA_NO_LIMITS);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return dataTable;
    }

    /**
     * Returns a low-level JDBC {@link java.sql.ResultSet} representation of all rows
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
     * @throws com.nridge.core.base.std.NSException Catch-all exception for any SQL related issue.
     */
    @Override
    public ResultSet select(String aSelectFromWhereStatement) throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "select");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        ResultSet resultSet;
        appLogger.debug(aSelectFromWhereStatement);
        Connection jdbcConnection = mSQLConnection.getJDBCConnection();
        try
        {
            Statement stmtQuery = jdbcConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                                                                 ResultSet.CONCUR_READ_ONLY);
            stmtQuery.setEscapeProcessing(mSQLConnection.isStatementEscapingEnabled());
            mSQLConnection.setLastStatement(aSelectFromWhereStatement);
            resultSet = stmtQuery.executeQuery(aSelectFromWhereStatement);
        }
        catch (SQLException e)
        {
            throw new NSException("RDBMS Query Error: " + aSelectFromWhereStatement + " : " + e.getMessage(), e);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return resultSet;
    }

    /**
     * Updates one or more rows in the RDBMS table matching the
     * {@link com.nridge.core.base.ds.DSCriteria} with the assigned values in the
     * {@link com.nridge.core.base.field.data.DataBag}.
     *
     * @param aBag      Data field bag with DB name.
     * @param aCriteria Data source criteria.
     *
     * @throws com.nridge.core.base.std.NSException Catch-all exception for any SQL related issue.
     */
    @Override
    public void update(DataBag aBag, DSCriteria aCriteria)
        throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "update");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if ((aBag == null) || (aBag.count() == 0))
            throw new NSException("Empty persistent bag.");
        else if ((aCriteria == null) &&
                 (aBag.featureNameValueCount(Field.FEATURE_IS_PRIMARY_KEY, StrUtl.STRING_TRUE) != 1))
            throw new NSException(Field.VALIDATION_MESSAGE_PRIMARY_KEY);
        else if (assignedFieldsCount(aBag) == 0)
            throw new NSException("The bag does not have assigned fields to update.");

        boolean isCommaNeeded = false;
        StringBuilder sqlBuilder = new StringBuilder(String.format("UPDATE %s", schemaName(aBag)));
        for (DataField pField : aBag.getFields())
        {
            if ((pField.isFeatureTrue(Field.FEATURE_IS_PRIMARY_KEY)) || (! pField.isAssigned()))
                continue;

            if (isCommaNeeded)
                sqlBuilder.append(StrUtl.CHAR_COMMA);
            else
            {
                isCommaNeeded = true;
                sqlBuilder.append(" SET");
            }
            sqlBuilder.append(String.format(" %s=", columnName(pField.getName())));
            if (pField.isTypeText())
            {
                sqlBuilder.append(StrUtl.CHAR_SGLQUOTE);
                sqlBuilder.append(escapeText(pField.getValue()));
                sqlBuilder.append(StrUtl.CHAR_SGLQUOTE);
            }
            else if (pField.isTypeDateOrTime())
                sqlBuilder.append(escapeTimestamp(pField.getValueAsDate().getTime()));
            else
                sqlBuilder.append(pField.getValue());
        }

        if (aCriteria == null)
        {
            DataField persistField = aBag.getPrimaryKeyField();
            sqlBuilder.append(String.format(" WHERE %s=%s", columnName(persistField.getName()),
                                            persistField.getValue()));
        }
        else
            sqlBuilder.append(buildWhereClause(aBag, aCriteria));

        mSQLConnection.execute(sqlBuilder.toString());

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Updates a single row in the RDBMS table that matches the
     * primary key field in the {@link com.nridge.core.base.field.data.DataBag} with any fields
     * that have assigned values.
     *
     * @param aBag Data field bag with DB name assigned and a
     *             primary key designated.
     *
     * @throws com.nridge.core.base.std.NSException Catch-all exception for any SQL related issue.
     */
    @Override
    public void update(DataBag aBag)
        throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "update");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        update(aBag, null);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Deletes a single row in the RDBMS table that matches the
     * primary key field in the {@link com.nridge.core.base.field.data.DataBag}.
     *
     * @param aBag Data field bag with DB name assigned and a
     *             primary key designated.
     *
     * @throws com.nridge.core.base.std.NSException Catch-all exception for any SQL related issue.
     */
    @Override
    public void delete(DataBag aBag)
        throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "delete");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        DataField dataField = aBag.getPrimaryKeyField();
        DSCriteria dsCriteria = new DSCriteria("Delete Criteria");
        dsCriteria.add(dataField.getName(), Field.Operator.EQUAL, dataField.getValueAsInt());

        delete(aBag, dsCriteria);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Deletes one or more rows in the RDBMS table matching the
     * {@link com.nridge.core.base.ds.DSCriteria}.  The {@link com.nridge.core.base.field.data.DataBag} is used to
     * determine the name of the DB table.
     *
     * @param aBag      Data field bag with DB name.
     * @param aCriteria Data source criteria.
     *
     * @throws com.nridge.core.base.std.NSException Catch-all exception for any SQL related issue.
     */
    @Override
    public void delete(DataBag aBag, DSCriteria aCriteria)
        throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "delete");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (aBag.featureNameValueCount(Field.FEATURE_IS_PRIMARY_KEY, StrUtl.STRING_TRUE) != 1)
            throw new NSException(Field.VALIDATION_MESSAGE_PRIMARY_KEY);
        else if ((aCriteria == null) && (aBag.featureNameValueCount(Field.FEATURE_IS_PRIMARY_KEY, StrUtl.STRING_TRUE) != 1))
            throw new NSException(Field.VALIDATION_MESSAGE_PRIMARY_KEY);

        StringBuilder sqlBuilder = new StringBuilder(String.format("DELETE FROM %s", schemaName(aBag)));
        if (aCriteria == null)
        {
            DataField dataField = aBag.getPrimaryKeyField();
            sqlBuilder.append(String.format(" WHERE %s=%s", columnName(dataField.getName()),
                                            dataField.getValueAsInt()));
        }
        else
            sqlBuilder.append(buildWhereClause(aBag, aCriteria));

        mSQLConnection.execute(sqlBuilder.toString());

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }
}
