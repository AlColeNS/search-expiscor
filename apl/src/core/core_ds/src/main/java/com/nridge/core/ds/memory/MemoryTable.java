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

package com.nridge.core.ds.memory;

import com.nridge.core.app.mgr.AppMgr;
import com.nridge.core.base.ds.DSCriteria;
import com.nridge.core.base.ds.DSCriterionEntry;
import com.nridge.core.base.ds.DSException;
import com.nridge.core.base.field.Field;
import com.nridge.core.base.field.FieldRow;
import com.nridge.core.base.field.data.DataBag;
import com.nridge.core.base.field.data.DataField;
import com.nridge.core.base.field.data.DataTable;
import com.nridge.core.ds.DSTable;
import com.nridge.core.io.csv.DataTableCSV;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayList;

/**
 * The MemoryTable data source manages a row x column matrix of
 * <i>DataFields</i> in memory.  It implements the four CRUD
 * methods and supports advanced queries using a <i>DSCriteria</i>.
 * In addition, this data source offers save and load methods for
 * data values.  This can be a useful data source if your table
 * size is small in nature and there is sufficient heap space
 * available.
 *
 * @since 1.0
 * @author Al Cole
 */
public class MemoryTable extends DSTable
{
    private final String DS_TYPE_NAME = "MemoryTable";

    protected DataTable mValueTable;

    /**
     * Constructor accepts an application manager parameter and initializes
     * the data source accordingly.
     *
     * @param anAppMgr Application manager.
     */
    public MemoryTable(AppMgr anAppMgr)
    {
        super(anAppMgr);
        setName("Value");
        mValueTable = new DataTable(getCacheTable().getColumnBag());
        mValueTable.setName("Value");
    }

    /**
     * Constructor accepts an application manager and name parameter and
     * initializes the daa source accordingly.
     *
     * @param anAppMgr Application manager.
     * @param aName    Name of the data source.
     */
    public MemoryTable(AppMgr anAppMgr, String aName)
    {
        super(anAppMgr, aName);
        mValueTable = new DataTable(getCacheTable().getColumnBag());
    }

    /**
     * Constructor accepts an application manager, name and title parameters
     * and initializes the data source accordingly.
     *
     * @param anAppMgr Application manager.
     * @param aName    Name of the data source.
     * @param aTitle   Title of the data source.
     */
    public MemoryTable(AppMgr anAppMgr, String aName, String aTitle)
    {
        super(anAppMgr, aName, aTitle);
        mValueTable = new DataTable(getCacheTable().getColumnBag());
    }

    /**
     * Constructor accepts a <i>MemoryTable</i> as an initialization parameter.
     * The data source will be used to assign the schema definition to the new
     * data source.
     *
     * @param anAppMgr Application manager.
     * @param aSrcDS   Data source to clone.
     */
    public MemoryTable(AppMgr anAppMgr, MemoryTable aSrcDS)
    {
        super(anAppMgr, aSrcDS);
        mValueTable = new DataTable(aSrcDS.mValueTable);
    }

    /**
     * Stores the schema definition of the underlying data source
     * (formatted in XML) to the file system. The name of the file
     * is derived from the data of the data source and the location
     * of the file is specified as a parameter.
     *
     * @param aPathName Path name where the file should be written.
     * @throws java.io.IOException                 I/O related exception.
     * @throws com.nridge.core.base.ds.DSException Data source related exception.
     */
    public void saveValues(String aPathName)
        throws IOException, DSException
    {
        String pathFileName = createValuePathFileName(aPathName, "csv");
        DataTableCSV tableCSV = new DataTableCSV(mValueTable);
        tableCSV.save(pathFileName, true);
    }

    /**
     * Loads the (XML formatted) schema file of a (formally saved)
     * data source definition into memory. The name of the file is
     * derived from the data of the data source and the location of
     * the file is specified as a parameter.
     *
     * @param aPathName Path name where the XML file should be
     *                  loaded from.
     * @throws java.io.IOException                 I/O related exception.
     * @throws com.nridge.core.base.ds.DSException Data source related exception.
     */
    public void loadValues(String aPathName)
        throws IOException, DSException
    {
        String pathFileName = createValuePathFileName(aPathName, "csv");
        if (mValueTable.columnCount() == 0)
            mValueTable = new DataTable(getCacheBag());
        else
            mValueTable.emptyRows();
        DataTableCSV tableCSV = new DataTableCSV(mValueTable);
        tableCSV.load(pathFileName, true);
    }

    /**
     * Assigns a bag of fields as the columns of the value table.
     *
     * @param aBag Bag of fields.
     */
    public void setValueBag(DataBag aBag)
    {
        if (aBag != null)
        {
            mValueTable = new DataTable(aBag);
            setDefinedFlag(true);
        }
    }

    /**
     * Assigns a value table to the data source.
     *
     * @param aTable Data table.
     */
    public void setValueTable(DataTable aTable)
    {
        if (aTable != null)
        {
            setCacheBag(aTable.getColumnBag());
            mValueTable = new DataTable(aTable);
            setDefinedFlag(true);
        }
    }

    /**
     * Returns a string summary representation of a memory table.
     *
     * @return String summary representation of this memory table.
     */
    @Override
    public String toString()
    {
        String idString;
        if (StringUtils.isNotEmpty(getTitle()))
            idString = DS_TYPE_NAME + " - " + getTitle();
        else
            idString = DS_TYPE_NAME + " - " + getName();
        if (mValueTable != null)
            idString += " + " + mValueTable.toString();

        return idString;
    }

    /**
     * Returns the application defined type name of the memory table.
     *
     * @return Type name.
     */
    @Override
    public String getTypeName()
    {
        return DS_TYPE_NAME;
    }

    /**
     * Returns the <i>DataTable</i> representation of the values
     * managed by this memory table.
     *
     * @return Table of values.
     */
    public DataTable getValueTable()
    {
        return mValueTable;
    }

    private DataTable query(DSCriteria aDSCriteria)
        throws DSException
    {
        DataTable dataTable;
        Logger appLogger = mAppMgr.getLogger(this, "query");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if ((aDSCriteria != null) && (aDSCriteria.isSimple()))
        {
            DataField dataField;
            ArrayList<FieldRow> fieldRows;
            dataTable = new DataTable(getValueTable());

            for (DSCriterionEntry ce : aDSCriteria.getCriterionEntries())
            {
                dataField = ce.getField();
                switch (dataField.getType())
                {
                    case Integer:
                        fieldRows = dataTable.findValue(dataField.getName(), ce.getLogicalOperator(),
                                                        dataField.getValueAsInt(), 0);
                        break;
                    case Long:
                        fieldRows = dataTable.findValue(dataField.getName(), ce.getLogicalOperator(),
                                                        dataField.getValueAsLong(), 0);
                        break;
                    case Float:
                        fieldRows = dataTable.findValue(dataField.getName(), ce.getLogicalOperator(),
                                                        dataField.getValueAsFloat(), 0.0);
                        break;
                    case Double:
                        fieldRows = dataTable.findValue(dataField.getName(), ce.getLogicalOperator(),
                                                        dataField.getValueAsDouble(), 0.0);
                        break;
                    case Date:
                    case DateTime:
                        fieldRows = dataTable.findValue(dataField.getName(), ce.getLogicalOperator(),
                                                        dataField.getValueAsDate(), null);
                        break;
                    default:
                        if (ce.isCaseInsensitive())
                            fieldRows = dataTable.findValueInsensitive(dataField.getName(), ce.getLogicalOperator(),
                                                                       dataField.getValue());
                        else
                            fieldRows = dataTable.findValue(dataField.getName(), ce.getLogicalOperator(),
                                                            dataField.getValue());
                        break;
                }

                if (fieldRows != null)
                {
                    dataTable.setRows(fieldRows);
                    if (fieldRows.size() == 0)
                        break;
                }
                else
                    break;
            }
        }
        else
            dataTable = new DataTable(getCacheBag());

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return dataTable;
    }

    /**
     * Calculates a count (using a wildcard criteria) of all the
     * rows stored in the content source and returns that value.
     *
     * @return Count of all rows in the content source.
     *
     * @throws com.nridge.core.base.ds.DSException Data source related exception.
     */
    @Override
    public int count()
        throws DSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "count");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return mValueTable.rowCount();
    }

    /**
     * Returns a count of rows that match the <i>DSCriteria</i> specified
     * in the parameter.
     *
     * @param aDSCriteria Data source criteria.
     *
     * @return Count of rows matching the data source criteria.
     *
     * @throws com.nridge.core.base.ds.DSException Data source related exception.
     */
    @Override
    public int count(DSCriteria aDSCriteria)
        throws DSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "count");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        mCacheTable = query(aDSCriteria);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return mCacheTable.rowCount();
    }

    /**
     * Returns a <i>DataTable</i> representation of all rows
     * fetched from the underlying content source (using a wildcard
     * criteria).
     * <p>
     * <b>Note:</b> Depending on the size of the content source
     * behind this data source, this method could consume large
     * amounts of heap memory.  Therefore, it should only be
     * used when the number of column and rows is known to be
     * small in size.
     * </p>
     *
     * @return Table representing all rows in the content source.
     *
     * @throws com.nridge.core.base.ds.DSException Data source related exception.
     */
    @Override
    public DataTable fetch()
        throws DSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "fetch");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        mCacheTable = new DataTable(mValueTable);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return mCacheTable;
    }

    /**
     * Returns a <i>DataTable</i> representation of the rows that
     * match the <i>DSCriteria</i> specified in the parameter.
     * <p>
     * <b>Note:</b> Depending on the size of the content source
     * behind this data source and the criteria specified, this
     * method could consume large amounts of heap memory.
     * Therefore, the developer is encouraged to use the alternative
     * method for fetch where an offset and limit parameter can be
     * specified.
     * </p>
     *
     * @param aDSCriteria Data source criteria.
     *
     * @return Table representing all rows that match the criteria
     * in the content source.
     * @throws com.nridge.core.base.ds.DSException Data source related exception.
     */
    @Override
    public DataTable fetch(DSCriteria aDSCriteria)
        throws DSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "fetch");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        mCacheTable = query(aDSCriteria);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return mCacheTable;
    }

    /**
     * Returns a <i>DataTable</i> representation of the rows that
     * match the <i>DSCriteria</i> specified in the parameter.  In
     * addition, this method offers a paging mechanism where the
     * starting offset and a fetch limit can be applied to each
     * content fetch query.
     *
     * @param aDSCriteria Data source criteria.
     * @param anOffset    Starting offset into the matching content rows.
     * @param aLimit      Limit on the total number of rows to extract from
     *                    the content source during this fetch operation.
     *
     * @return Table representing all rows that match the criteria
     * in the content source (based on the offset and limit values).
     *
     * @throws com.nridge.core.base.ds.DSException Data source related exception.
     */
    @Override
    public DataTable fetch(DSCriteria aDSCriteria, int anOffset, int aLimit)
        throws DSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "fetch");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        fetch(aDSCriteria);

        anOffset = Math.max(anOffset, 0);
        aLimit = Math.min(anOffset+aLimit, mCacheTable.rowCount());

        ArrayList<FieldRow> allRows = mCacheTable.getRows();
        ArrayList<FieldRow> newRows = new ArrayList<FieldRow>();

        for (int row = anOffset; row < aLimit; row++)
            newRows.add(allRows.get(row));

        mCacheTable.setRows(newRows);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return mCacheTable;
    }

    /**
     * Adds the field values captured in the <i>DataBag</i> to
     * the content source.  The fields must be derived from the
     * same collection defined in the cache schema definition.
     *
     * @param aBag Bag of field values to store.
     *
     * @throws com.nridge.core.base.ds.DSException Data source related exception.
     */
    @Override
    public void add(DataBag aBag)
        throws DSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "add");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

// Calculate our max primary key value.

        DataBag persistBag = mValueTable.getColumnBag();
        DataField primaryKeyField = persistBag.getPrimaryKeyField();
        if (primaryKeyField != null)
        {
            int maxPrimaryValue = 0;
            DataField persistField;
            int rowCount = mValueTable.rowCount();
            for (int row = 0; row < rowCount; row++)
            {
                persistField = mValueTable.getFieldByRowName(row, primaryKeyField.getName());
                maxPrimaryValue = Math.max(maxPrimaryValue, persistField.getValueAsInt());
            }
            primaryKeyField = aBag.getPrimaryKeyField();
            if (primaryKeyField != null)
                primaryKeyField.setValue(maxPrimaryValue+1);
        }

        mValueTable.addRow(aBag);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Updates the field values captured in the <i>DataBag</i>
     * within the content source.  The fields must be derived from the
     * same collection defined in the cache schema definition.
     * <p>
     * <b>Note:</b> The bag must designate a field as a primary
     * key and that value must be assigned prior to using this
     * method.
     * </p>
     *
     * @param aBag Bag of field values to update.
     *
     * @throws com.nridge.core.base.ds.DSException Data source related exception.
     */
    @Override
    public void update(DataBag aBag)
        throws DSException
    {
        boolean isUpdated = false;
        Logger appLogger = mAppMgr.getLogger(this, "update");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        DataField primaryKeyField = aBag.getPrimaryKeyField();
        if (primaryKeyField != null)
        {
            ArrayList<FieldRow> fieldRows = mValueTable.findValue(primaryKeyField.getName(), Field.Operator.EQUAL, aBag.getValueAsString(primaryKeyField.getName()));
            if ((fieldRows != null) && (fieldRows.size() == 1))
            {
                FieldRow fieldRow = fieldRows.get(0);
                for (DataField dataField : aBag.getFields())
                    mValueTable.setValueByName(fieldRow, dataField.getName(), dataField.getValue());
                isUpdated = true;
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        if (! isUpdated)
            throw new DSException("Update operation for " + DS_TYPE_NAME + " data source failed.");
    }

    /**
     * Deletes the record identified by the <i>DataBag</i> from
     * the content source.  The fields must be derived from the
     * same collection defined in the cache schema definition.
     * <p>
     * <b>Note:</b> The bag must designate a field as a primary
     * key and that value must be assigned prior to using this
     * method.
     * </p>
     *
     * @param aBag Bag where the primary key field value is
     *             assigned.
     *
     * @throws com.nridge.core.base.ds.DSException Data source related exception.
     */
    @Override
    public void delete(DataBag aBag)
        throws DSException
    {
        boolean isDeleted = false;
        Logger appLogger = mAppMgr.getLogger(this, "delete");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        DataField primaryKeyField = aBag.getPrimaryKeyField();
        if (primaryKeyField != null)
        {
            ArrayList<FieldRow> fieldRows = mValueTable.findValue(primaryKeyField.getName(), Field.Operator.EQUAL, aBag.getValueAsString(primaryKeyField.getName()));
            if ((fieldRows != null) && (fieldRows.size() == 1))
            {
                FieldRow fieldRow = fieldRows.get(0);
                mValueTable.removeRow(fieldRow);
                isDeleted = true;
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        if (! isDeleted)
            throw new DSException("Delete operation for " + DS_TYPE_NAME + " data source failed.");
    }
}
