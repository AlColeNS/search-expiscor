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

package com.nridge.core.base.field.data;

import com.nridge.core.base.field.CellValue;
import com.nridge.core.base.field.Field;
import com.nridge.core.base.field.FieldRow;

import com.nridge.core.base.std.DigitalHash;
import com.nridge.core.base.std.StrUtl;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A DataTable manages a row x column matrix of <i>DataFields</i>.  Use this
 * class when you need to model meta data fields. In addition, you can specify
 * also specify a sort order for data sources to utilize during fetch operations.
 * <p>
 * This framework provides a number of helper classes that accept a DataTable
 * for IO operations.
 * </p>
 *
 * @author Al Cole
 * @since 1.0
 */
public class DataTable
{
    private int mColumnCount;
    private FieldRow mNewRow;
    private DataBag mColumns;
    private String[] mOffsetNameMap;
    private ArrayList<FieldRow> mRows;
    private String mName = StringUtils.EMPTY;
    private HashMap<String, String> mFeatures;
    private String mSortFieldName = StringUtils.EMPTY;
    private transient HashMap<String, Object> mProperties;

    /**
     * Default constructor.
     */
    public DataTable()
    {
        mColumns = new DataBag();
        mRows = new ArrayList<FieldRow>();
        mFeatures = new HashMap<String, String>();
    }

    /**
     * Constructor accepts a table name parameter and initializes the DataTable.
     *
     * @param aName Name of table.
     */
    public DataTable(String aName)
    {
        setName(aName);
        mColumns = new DataBag();
        mRows = new ArrayList<FieldRow>();
        mFeatures = new HashMap<String, String>();
    }

    /**
     * Constructor accepts a {@link DataBag} as an initialization parameter.
     * The bag will be used to assign the column fields for the table.
     *
     * @param aBag Persist bag.
     */
    public DataTable(final DataBag aBag)
    {
        mRows = new ArrayList<FieldRow>();
        mFeatures = new HashMap<String, String>();
        setColumns(new DataBag(aBag));
        String bagName = aBag.getName();
        if (StringUtils.isNotEmpty(bagName))
            setName(bagName);
    }

    /**
     * Constructor clones an existing DataTable (name, title, rows, columns).
     *
     * @param aTable Source table instance to clone.
     * @param aStartOffset Starting offset into table.
     * @param aLimit Maximum number of rows (0 implies all)
     */
    public DataTable(final DataTable aTable, int aStartOffset, int aLimit)
    {
        if (aTable != null)
        {
            this.mRows = new ArrayList<FieldRow>();
            this.setName(aTable.getName());
            this.setSortFieldName(aTable.getSortFieldName());
            this.mFeatures = new HashMap<String, String>(aTable.getFeatures());
            this.setColumns(new DataBag(aTable.getColumnBag()));
            int rowCount = aTable.rowCount();
            if (rowCount > 0)
            {
                FieldRow fieldRow;

                if (aStartOffset < 0)
                    aStartOffset = 0;
                if (aLimit >= rowCount)
                    aLimit = rowCount;

                int recordCount = 0;
                for (int row = aStartOffset; (row < rowCount) && (recordCount < aLimit); row++)
                {
                    recordCount++;
                    fieldRow = aTable.getRow(row);
                    this.addRow(new FieldRow(fieldRow));
                }
            }
        }
    }

    /**
     * Constructor clones an existing DataTable (name, title, rows, columns).
     *
     * @param aTable Source table instance to clone.
     */
    public DataTable(final DataTable aTable)
    {
        if (aTable != null)
        {
            this.mRows = new ArrayList<FieldRow>();
            this.setName(aTable.getName());
            this.setSortFieldName(aTable.getSortFieldName());
            this.mFeatures = new HashMap<String, String>(aTable.getFeatures());
            this.setColumns(new DataBag(aTable.getColumnBag()));
            for (FieldRow fieldRow : aTable.getRows())
                this.addRow(new FieldRow(fieldRow));
        }
    }

    /**
     * Creates a new instance of the data table based on the
     * one currently populated.  The new instance will contain
     * only those columns that have been populated from previous
     * assignments.  This can be helpful when you start with a
     * schema table containing many possible columns that never
     * end up getting populated in a load operation.
     *
     * @return Data table instance.
     */
    public DataTable collapseUnusedColumns()
    {
        FieldRow newRow;
        DataTable newTable;
        String newFieldName;
        DataField curDataField, newDataField;

        if (StringUtils.isEmpty(mName))
            newTable = new DataTable();
        else
            newTable = new DataTable(mName);
        newTable.setFeatures(getFeatures());
        newTable.setSortFieldName(getSortFieldName());
        DataBag newBag = mColumns.collapseUnusedFields();
        newTable.setColumns(newBag);

        int curRowCount = rowCount();
        int newColCount = newBag.count();
        for (int row = 0; row < curRowCount; row++)
        {
            newRow = null;
            for (int col = 0; col < newColCount; col++)
            {
                newDataField = newBag.getByOffset(col);
                newFieldName = newDataField.getName();
                curDataField = getFieldByRowName(row, newFieldName);
                if (curDataField != null)
                {
                    if (newRow == null)
                        newRow = newTable.newRow();
                    if (curDataField.isMultiValue())
                        newTable.setValuesByName(newFieldName, curDataField.getValues());
                    else
                        newTable.setValueByName(newFieldName, curDataField.getValue());
                }
            }
            if (newRow != null)
                newTable.addRow();
        }

        return newTable;
    }

    /**
     * Returns the name of the table.
     *
     * @return Table name.
     */
    public String getName()
    {
        return mName;
    }

    /**
     * Assigns the name of the table.
     *
     * @param aName Table name.
     */
    public void setName(String aName)
    {
        mName = aName;
        if ((mColumns != null) && (StringUtils.isEmpty(mColumns.getName())))
            mColumns.setName(aName);
    }

    /**
     * Returns a string summary representation of a DataTable.
     *
     * @return String summary representation of this DataTable.
     */
    @Override
    public String toString()
    {
        String idName;
        int rowCount, colCount;

        if (StringUtils.isEmpty(mName))
            idName = "Data Table";
        else
            idName = mName;

        if (mRows == null)
            rowCount = 0;
        else
            rowCount = mRows.size();
        if (mColumns == null)
            colCount = 0;
        else
            colCount = mColumns.count();

        return String.format("%s [%d cols x %d rows]", idName, colCount, rowCount);
    }

    /**
     * You can designate one field (column) for sorting in a table.  This
     * method returns the name of that field or an empty string.
     *
     * @return Name of the sort field or an empty string if unassigned.
     */
    public String getSortFieldName()
    {
        return mSortFieldName;
    }

    /**
     * You can designate one field (column) for sorting in a table.  This
     * method assigns that field name.
     *
     * @param aName Field name.
     */
    public void setSortFieldName(String aName)
    {
        mSortFieldName = aName;
    }

    /**
     * Assigns the fields contained within the bag to the columns of
     * the table.
     * <p>
     * <b>Note:</b> This method will only update the columns if the table
     * is empty (e.g. no rows have been added to the table).
     * </p>
     *
     * @param aBag Data bag of fields.
     */
    public void setColumns(DataBag aBag)
    {
        if ((aBag != null) && ((mRows == null) || (mRows.size() == 0)))
        {
            mColumns = aBag;
            mColumnCount = mColumns.count();
        }
    }

    /**
     * Returns the count of column fields in this table.
     *
     * @return Count of column fields.
     */
    public int columnCount()
    {
        mColumnCount = mColumns.count();
        return mColumnCount;
    }

    /**
     * Returns the collection of field rows to derived classes.
     *
     * @return A list of internal field rows.
     */
    public ArrayList<FieldRow> getRows()
    {
        return mRows;
    }

    /**
     * Returns a list of bags based on the rows within the table.
     * Please note that each row is copied into a DataBag - any
     * changes to the the fields in the bag will not find their
     * way back to the original table row.
     *
     * @return A list of data bag instances.
     */
    public ArrayList<DataBag> getAsBagList()
    {
        ArrayList<DataBag> dataBagList = new ArrayList<DataBag>();
        for (FieldRow fieldRow : mRows)
            dataBagList.add(getRowAsBag(fieldRow));

        return dataBagList;
    }

    private void populateOffsetNameMap()
    {
        if (mOffsetNameMap == null)
        {
            int offset = 0;
            mColumnCount = mColumns.count();
            mOffsetNameMap = new String[mColumnCount];
            ArrayList<DataField> bagFields = mColumns.getFields();

            for (DataField dataField : bagFields)
                mOffsetNameMap[offset++] = dataField.getName();
        }
    }

    /**
     * Assigns the list of {@link FieldRow}s to this table.  This
     * method is typically used by IO helper classes to reconstruct
     * the contents of a table.
     *
     * @param aRows List of field rows.
     */
    public void setRows(ArrayList<FieldRow> aRows)
    {
        if ((aRows != null) && (aRows.size() > 0))
        {
            int rowCount = aRows.size();
            FieldRow fieldRow = aRows.get(0);
            if (fieldRow.count() == mColumns.count())
            {
                populateOffsetNameMap();
                mRows = new ArrayList<FieldRow>(rowCount);
                for (int row = 0; row < rowCount; row++)
                    addRow(new FieldRow(aRows.get(row)));
            }
        }
        else
            mRows = new ArrayList<FieldRow>();
    }

    /**
     * Adds the {@link DataField} to collection of columns in the
     * table.
     * <p>
     * <b>Note:</b> This method will only add the column if the table is
     * empty (e.g. no rows have been added to the table).
     * </p>
     *
     * @param aField A field to add as a column.
     */
    public void add(DataField aField)
    {
        if (mRows.size() == 0)
            mColumns.add(aField);
    }

    /**
     * Returns the <i>FieldRow</i> identified by the row offset parameter
     * value.
     *
     * @param aRowOffset Row offset into table.
     *
     * @return Field row representation of the column data.
     */
    public FieldRow getRow(int aRowOffset)
    {
        if (aRowOffset < mRows.size())
            return mRows.get(aRowOffset);
        else
            return null;
    }

    /**
     * Returns the <i>DataField</i> identified by the column offset
     * parameter.
     *
     * @param aColOffset Column offset.
     *
     * @return Simple field representation of the column definition.
     */
    public DataField getColumn(int aColOffset)
    {
        return mColumns.getByOffset(aColOffset);
    }

    /**
     * Returns the <i>DataBag</i> representation of the field columns
     * defined for the table.
     *
     * @return Data bag representation of the table columns.
     */
    public DataBag getColumnBag()
    {
        return mColumns;
    }

    /**
     * Convenience method that transforms the cells of a field row into
     * a <i>DataBag</i> representation.
     *
     * @param aFieldRow Field row.
     *
     * @return Data bag instance.
     */
    public DataBag getRowAsBag(FieldRow aFieldRow)
    {
        DataField dataField;
        DataBag dataBag = new DataBag(mColumns);
        ArrayList<DataField> bagFields = dataBag.getFields();

        for (int col = 0; col < mColumnCount; col++)
        {
            dataField = bagFields.get(col);
            if (dataField.isMultiValue())
                dataField.setValues(aFieldRow.getValues(col));
            else
                dataField.setValue(aFieldRow.getValue(col));
        }

        return dataBag;
    }

    /**
     * Convenience method that transforms the field row identified
     * by the row offset parameter into a <i>DataBag</i>.
     *
     * @param aRowOffset Row offset into table.
     *
     * @return Data bag.
     */
    public DataBag getRowAsBag(int aRowOffset)
    {
        return getRowAsBag(getRow(aRowOffset));
    }

    /**
     * Returns a new <i>FieldRow</i> suitable for updating and later
     * adding to the table.
     * <p>
     * <b>Note:</b> As a convenience, the class will maintain an
     * internal reference to the last field row created via this
     * method.  Several setValue() methods will recognize this
     * internal field row reference when assigning values to
     * cells in a row.
     * </p>
     *
     * @return Field row initialized with column cells.
     *
     * @see <code>DataTable.setValueByName()</code>
     */
    public FieldRow newRow()
    {
        populateOffsetNameMap();
        mNewRow = new FieldRow(mColumnCount);

        return mNewRow;
    }

    /**
     * Convenience method that adds the <i>DataBag</i> as a new row
     * in the table.
     *
     * @param aBag Bag of fields.
     */
    public void addRow(DataBag aBag)
    {
        if (aBag != null)
        {
            FieldRow fieldRow = newRow();
            ArrayList<DataField> bagFields = aBag.getFields();
            for (DataField dataField : bagFields)
                setValueByName(fieldRow, dataField.getName(), dataField.getValue());
            addRow(fieldRow);
        }
    }

    /**
     * Adds the <i>FieldRow</i> parameter to the table.
     *
     * @param aRow Field row.
     *
     * @see <code>DataTable.newRow()</code>
     */
    public void addRow(FieldRow aRow)
    {
        CellValue cellValue;

        for (int col = 0; col < mColumnCount; col++)
        {
            cellValue = aRow.getCellValue(col);
            if (cellValue.isAssigned())
                markColumnAssigned(col);
        }
        mRows.add(aRow);
    }

    /**
     * Adds the internally managed field row reference to the table.  The
     * parent application should not use this method unless it called
     * <code>DataTable.newRow()</code>  previously and populated the cell
     * values.
     */
    public void addRow()
    {
        if (mNewRow != null)
        {
            mRows.add(mNewRow);
            mNewRow = null;
        }
    }

    /**
     * Removes the row identified by the offset parameter from the table.
     *
     * @param aRowOffset Row offset.
     */
    public void removeRow(int aRowOffset)
    {
        if (aRowOffset < rowCount())
            mRows.remove(aRowOffset);
    }

    /**
     * Removes the field row identified by the <i>FieldRow</i> parameter
     * from the table.
     *
     * @param aRow Field row.
     */
    public void removeRow(FieldRow aRow)
    {
        if (aRow != null)
        {
            int rowOffset = 0;
            for (FieldRow fieldRow : mRows)
            {
                if (fieldRow.isEqual(aRow))
                {
                    mRows.remove(rowOffset);
                    break;
                }
                else
                    rowOffset++;
            }
        }
    }

    /**
     * Empties the table of any field rows.  The columns and other
     * properties remain unchanged.
     */
    public void emptyRows()
    {
        mRows = new ArrayList<FieldRow>();
    }

    /**
     * Empties the table of any field rows and columns.
     */
    public void empty()
    {
        mFeatures.clear();
        mColumns = new DataBag();
        mRows = new ArrayList<FieldRow>();
    }

    /**
     * Returns the number of field rows in the table.
     *
     * @return Total rows.
     */
    public int rowCount()
    {
        return mRows.size();
    }

    /**
     * Convenience method that identifies the offset of the column that
     * has a field name matching the parameter name.
     *
     * @param aName Field name of column.
     *
     * @return Offset of field column or -1 if not found.
     */
    private int offsetByName(String aName)
    {
        if (StringUtils.isNotEmpty(aName))
        {
            populateOffsetNameMap();
            for (int offset = 0; offset < mColumnCount; offset++)
            {
                if (mOffsetNameMap[offset].equals(aName))
                    return offset;
            }
        }
        return -1;
    }

    private void markColumnAssigned(int aCol)
    {
        if ((aCol >= 0) && (aCol < mColumnCount))
        {
            DataField colField = mColumns.getByOffset(aCol);
            if ((colField != null) && (! colField.isAssigned()))
                colField.setAssignedFlag(true);
        }
    }

    /**
     * Assigns a value to the cell located at row and column.
     *
     * @param aRow Offset into the rows of the table.
     * @param aCol Offset into the columns of the table.
     * @param aValue Value to assign.
     *
     */
    public void setValueByRowColumn(int aRow, int aCol, String aValue)
    {
        FieldRow fieldRow = getRow(aRow);
        if (fieldRow != null)
        {
            markColumnAssigned(aCol);
            fieldRow.setValue(aCol, aValue);
        }
    }

    /**
     * Assigns multiple values to the cell located at row and column.
     *
     * @param aRow Offset into the rows of the table.
     * @param aCol Offset into the columns of the table.
     * @param aValues Values to assign.
     *
     */
    public void setValuesByRowColumn(int aRow, int aCol, ArrayList<String> aValues)
    {
        FieldRow fieldRow = getRow(aRow);
        if (fieldRow != null)
        {
            markColumnAssigned(aCol);
            fieldRow.setValues(aCol, aValues);
        }
    }

    /**
     * Assigns the value parameter to the cell of the internally
     * managed row identified by the column.
     *
     * <p>
     * <b>Note:</b> The parent application must call {@link DataTable}.addRow()
     * prior to using this method.
     * </p>
     *
     * @param aCol Offset into the columns of the table.
     * @param aValue Cell value to assign.
     */
    public void setValueByColumn(int aCol, String aValue)
    {
        if (mNewRow != null)
        {
            markColumnAssigned(aCol);
            mNewRow.setValue(aCol, aValue);
        }
    }

    /**
     * Assigns the values parameter to the cell of the internally
     * managed row identified by the column.
     *
     * <p>
     * <b>Note:</b> The parent application must call {@link DataTable}.addRow()
     * prior to using this method.
     * </p>
     *
     * @param aCol Offset into the columns of the table.
     * @param aValues Cell values to assign.
     */
    public void setValuesByColumn(int aCol, ArrayList<String> aValues)
    {
        if (mNewRow != null)
        {
            markColumnAssigned(aCol);
            mNewRow.setValues(aCol, aValues);
        }
    }

    /**
     * Returns a newly created data field from the cell at location
     * row, column.
     *
     * <p>
     * <b>Note:</b> The field is a copy of the original table cell.
     * Any modifications to the field will NOT be reflected in the
     * table.
     * </p>
     *
     * @param aRow Offset into the rows of the table.
     * @param aCol Offset into the columns of the table.
     *
     * @return Data field if successfully located or <i>null</i>.
     */
    public DataField getFieldByRowCol(int aRow, int aCol)
    {
        DataField dataField = null;

        FieldRow fieldRow = getRow(aRow);
        if (fieldRow != null)
        {
            dataField = new DataField(mColumns.getByOffset(aCol));
            dataField.setValues(fieldRow.getValues(aCol));
        }

        return dataField;
    }

    /**
     * Returns the field located at the row and matches the field
     * name parameter.
     *
     * <p>
     * <b>Note:</b> The field is a copy of the original table cell.
     * Any modifications to the field will NOT be reflected in the
     * table.
     * </p>
     *
     * @param aRow Offset into the rows of the table.
     * @param aName Name of the field to match in the row.
     *
     * @return Data field if successfully located or <i>null</i>.
     */
    public DataField getFieldByRowName(int aRow, String aName)
    {
        DataField dataField = null;

        FieldRow fieldRow = getRow(aRow);
        if (fieldRow != null)
        {
            int colOffset = offsetByName(aName);
            dataField = new DataField(mColumns.getByOffset(colOffset));
            dataField.setValues(fieldRow.getValues(colOffset));
        }

        return dataField;
    }

    /**
     * Returns a {@link DataField} representing the cell identified
     * by the column offset parameter.
     *
     * <p>
     * <b>Note:</b> The parent application must call {@link DataTable}.addRow()
     * prior to using this method.  Also, the field is a copy of the
     * original table cell. Any modifications to the field will NOT
     * be reflected in the table.
     * </p>
     *
     * @param aCol Column offset.
     * @return Data field or <i>null</i> if not found.
     */
    public DataField getFieldByColumn(int aCol)
    {
        DataField dataField;

        dataField = new DataField(mColumns.getByOffset(aCol));
        if ((dataField != null) && (mNewRow != null))
            dataField.setValues(mNewRow.getValues(aCol));

        return dataField;
    }

    /**
     * Returns the cell value for the column name contained
     * within the field row parameter.
     *
     * @param aRow Field row.
     * @param aName Name of the column.
     *
     * @return Cell value or an empty string if not found.
     *
     * @see <code>DataTable.getRow()</code>
     */
    public String getValueByName(FieldRow aRow, String aName)
    {
        int colOffset = offsetByName(aName);
        markColumnAssigned(colOffset);
        return aRow.getValue(colOffset);
    }

    /**
     * Returns the cell values for the column name contained
     * within the field row parameter.
     *
     * @param aRow Field row.
     * @param aName Name of the column.
     *
     * @return Cell values.
     *
     * @see <code>DataTable.getRow()</code>
     */
    public ArrayList<String> getValuesByName(FieldRow aRow, String aName)
    {
        return aRow.getValues(offsetByName(aName));
    }

    /**
     * Returns the value of the column name for the row contained within
     * the table.
     *
     * @param aRow Row offset into the table.
     * @param aName Name of the column.
     *
     * @return Cell value or an empty string if not found.
     *
     * @see <code>DataTable.rowCount()</code>
     */
    public String getValueByName(int aRow, String aName)
    {
        FieldRow fieldRow = getRow(aRow);
        if (fieldRow != null)
            return getValueByName(fieldRow, aName);
        else
            return StringUtils.EMPTY;
    }

    /**
     * Returns the values of the column name for the row contained within
     * the table.
     *
     * @param aRow Row offset into the table.
     * @param aName Name of the column.
     *
     * @return Cell values.
     *
     * @see <code>DataTable.rowCount()</code>
     */
    public ArrayList<String> getValuesByName(int aRow, String aName)
    {
        FieldRow fieldRow = getRow(aRow);
        if (fieldRow != null)
            return getValuesByName(fieldRow, aName);
        else
            return new ArrayList<String>();
    }

    /**
     * Assigns the value to the cell identified by the column name
     * contained within the field row.
     *
     * @param aRow Field row.
     * @param aName Name of the column.
     * @param aValue Value to assign to the cell.
     *
     * @see <code>DataTable.getRow()</code>
     */
    public void setValueByName(FieldRow aRow, String aName, String aValue)
    {
        int colOffset = offsetByName(aName);
        markColumnAssigned(colOffset);
        aRow.setValue(colOffset, aValue);
    }

    /**
     * Assigns the value to the cell identified by the row offset and
     * column name within the table.
     *
     * @param aRow Row offset into the table.
     * @param aName Name of the column.
     * @param aValue Value to assign to the cell.
     *
     * @see <code>DataTable.rowCount()</code>
     */
    public void setValueByName(int aRow, String aName, String aValue)
    {
        FieldRow fieldRow = getRow(aRow);
        if (fieldRow != null)
            setValueByName(fieldRow, aName, aValue);
    }

    /**
     * Assigns the value list to the cell identified by the row offset and
     * column name within the table.
     *
     * @param aRow Row offset into the table.
     * @param aName Name of the column.
     * @param aValues Value list to assign to the cell.
     *
     * @see <code>DataTable.rowCount()</code>
     */
    public void setValuesByName(int aRow, String aName, ArrayList<String> aValues)
    {
        FieldRow fieldRow = getRow(aRow);
        if (fieldRow != null)
            setValuesByName(fieldRow, aName, aValues);
    }

    /**
     * Assigns the value to the cell identified by the column name of
     * the internally managed field row.
     * <p>
     * <b>Note:</b> The parent application must call {@link DataTable}.addRow()
     * prior to using this method.
     * </p>
     *
     * @param aName Name of the column.
     * @param aValue Cell value to assign.
     *
     * @see <code>DataTable.addRow()</code>
     */
    public void setValueByName(String aName, String aValue)
    {
        if (mNewRow != null)
            setValueByName(mNewRow, aName, aValue);
    }

    /**
     * Assigns the list of values to the cell identified by the column
     * name of the internally managed field row.
     * <p>
     * <b>Note:</b> The parent application must call<code>DataTable.addRow()</code>
     * prior to using this method.
     * </p>
     *
     * @param aName Name of the column.
     * @param aValues Cell values to assign.
     *
     * @see <code>DataTable.addRow()</code>
     */
    public void setValuesByName(String aName, ArrayList<String> aValues)
    {
        if (mNewRow != null)
            setValuesByName(mNewRow, aName, aValues);
    }

    /**
     * Assigns the value to the cell identified by the column name
     * contained within the field row.
     *
     * @param aRow Field row.
     * @param aName Name of the column.
     * @param aValue Value to assign to the cell.
     *
     * @see <code>DataTable.getRow()</code>
     */
    public void setValueByName(FieldRow aRow, String aName, int aValue)
    {
        int colOffset = offsetByName(aName);
        markColumnAssigned(colOffset);
        aRow.setValue(colOffset, ((Integer) aValue).toString());
    }

    /**
     * Assigns the list of values to the cell identified by the column
     * name contained within the field row..
     * <p>
     * <b>Note:</b> The parent application must call {@link DataTable}.addRow()
     * prior to using this method.
     * </p>
     *
     * @param aRow Field row.
     * @param aName Name of the column.
     * @param aValues Cell values to assign.
     *
     * @see <code>DataTable.addRow()</code>
     */
    public void setValuesByName(FieldRow aRow, String aName, ArrayList<String> aValues)
    {
        int colOffset = offsetByName(aName);
        markColumnAssigned(colOffset);
        aRow.setValues(colOffset, aValues);
    }

    /**
     * Assigns the value to the cell identified by the row offset and
     * column name within the table.
     *
     * @param aRow Row offset into the table.
     * @param aName Name of the column.
     * @param aValue Value to assign to the cell.
     *
     * @see <code>DataTable.rowCount()</code>
     */
    public void setValueByName(int aRow, String aName, int aValue)
    {
        FieldRow fieldRow = getRow(aRow);
        if (fieldRow != null)
            setValueByName(fieldRow, aName, aValue);
    }

    /**
     * Assigns the value to the cell identified by the column name of
     * the internally managed field row.
     * <p>
     * <b>Note:</b> The parent application must call {@link DataTable}.addRow()
     * prior to using this method.
     * </p>
     *
     * @param aName Name of the column.
     * @param aValue Cell value to assign.
     *
     * @see <code>DataTable.addRow()</code>
     */
    public void setValueByName(String aName, int aValue)
    {
        if (mNewRow != null)
            setValueByName(mNewRow, aName, aValue);
    }

    /**
     * Assigns the value to the cell identified by the column name
     * contained within the field row.
     *
     * @param aRow Field row.
     * @param aName Name of the column.
     * @param aValue Value to assign to the cell.
     *
     * @see <code>DataTable.getRow()</code>
     */
    public void setValueByName(FieldRow aRow, String aName, long aValue)
    {
        int colOffset = offsetByName(aName);
        markColumnAssigned(colOffset);
        aRow.setValue(colOffset, ((Long)aValue).toString());
    }

    /**
     * Assigns the value to the cell identified by the row offset and
     * column name within the table.
     *
     * @param aRow Row offset into the table.
     * @param aName Name of the column.
     * @param aValue Value to assign to the cell.
     *
     * @see <code>DataTable.rowCount()</code>
     */
    public void setValueByName(int aRow, String aName, long aValue)
    {
        FieldRow fieldRow = getRow(aRow);
        if (fieldRow != null)
            setValueByName(fieldRow, aName, aValue);
    }

    /**
     * Assigns the value to the cell identified by the column name of
     * the internally managed field row.
     * <p>
     * <b>Note:</b> The parent application must call {@link DataTable}.addRow()
     * prior to using this method.
     * </p>
     *
     * @param aName Name of the column.
     * @param aValue Cell value to assign.
     *
     * @see <code>DataTable.addRow()</code>
     */
    public void setValueByName(String aName, long aValue)
    {
        if (mNewRow != null)
            setValueByName(mNewRow, aName, aValue);
    }

    /**
     * Assigns the value to the cell identified by the column name
     * contained within the field row.
     *
     * @param aRow Field row.
     * @param aName Name of the column.
     * @param aValue Value to assign to the cell.
     *
     * @see <code>DataTable.getRow()</code>
     */
    public void setValueByName(FieldRow aRow, String aName, float aValue)
    {
        int colOffset = offsetByName(aName);
        markColumnAssigned(colOffset);
        aRow.setValue(colOffset, ((Float)aValue).toString());
    }

    /**
     * Assigns the value to the cell identified by the row offset and
     * column name within the table.
     *
     * @param aRow Row offset into the table.
     * @param aName Name of the column.
     * @param aValue Value to assign to the cell.
     *
     * @see <code>DataTable.rowCount()</code>
     */
    public void setValueByName(int aRow, String aName, float aValue)
    {
        FieldRow fieldRow = getRow(aRow);
        if (fieldRow != null)
            setValueByName(fieldRow, aName, aValue);
    }

    /**
     * Assigns the value to the cell identified by the column name of
     * the internally managed field row.
     * <p>
     * <b>Note:</b> The parent application must call
     * <code>DataTable.addRow()</code> prior to using this method.
     * </p>
     *
     * @param aName Name of the column.
     * @param aValue Cell value to assign.
     *
     * @see <code>DataTable.addRow()</code>
     */
    public void setValueByName(String aName, float aValue)
    {
        if (mNewRow != null)
            setValueByName(mNewRow, aName, aValue);
    }

    /**
     * Assigns the value to the cell identified by the column name
     * contained within the field row.
     *
     * @param aRow Field row.
     * @param aName Name of the column.
     * @param aValue Value to assign to the cell.
     *
     * @see <code>DataTable.getRow()</code>
     */
    public void setValueByName(FieldRow aRow, String aName, double aValue)
    {
        int colOffset = offsetByName(aName);
        markColumnAssigned(colOffset);
        aRow.setValue(colOffset, ((Double)aValue).toString());
    }

    /**
     * Assigns the value to the cell identified by the row offset and
     * column name within the table.
     *
     * @param aRow Row offset into the table.
     * @param aName Name of the column.
     * @param aValue Value to assign to the cell.
     *
     * @see <code>DataTable.rowCount()</code>
     */
    public void setValueByName(int aRow, String aName, double aValue)
    {
        FieldRow fieldRow = getRow(aRow);
        if (fieldRow != null)
            setValueByName(fieldRow, aName, aValue);
    }

    /**
     * Assigns the value to the cell identified by the column name of
     * the internally managed field row.
     * <p>
     * <b>Note:</b> The parent application must call {@link DataTable}.addRow()
     * prior to using this method.
     * </p>
     *
     * @param aName Name of the column.
     * @param aValue Cell value to assign.
     *
     * @see <code>DataTable.addRow()</code>
     */
    public void setValueByName(String aName, double aValue)
    {
        if (mNewRow != null)
            setValueByName(mNewRow, aName, aValue);
    }

    /**
     * Assigns the value to the cell identified by the column name
     * contained within the field row.
     *
     * @param aRow Field row.
     * @param aName Name of the column.
     * @param aValue Value to assign to the cell.
     *
     * @see <code>DataTable.getRow()</code>
     */
    public void setValueByName(FieldRow aRow, String aName, boolean aValue)
    {
        int colOffset = offsetByName(aName);
        markColumnAssigned(colOffset);
        aRow.setValue(colOffset, StrUtl.booleanToString(aValue));
    }

    /**
     * Assigns the value to the cell identified by the row offset and
     * column name within the table.
     *
     * @param aRow Row offset into the table.
     * @param aName Name of the column.
     * @param aValue Value to assign to the cell.
     *
     * @see <code>DataTable.rowCount()</code>
     */
    public void setValueByName(int aRow, String aName, boolean aValue)
    {
        FieldRow fieldRow = getRow(aRow);
        if (fieldRow != null)
            setValueByName(fieldRow, aName, aValue);
    }

    /**
     * Assigns the value to the cell identified by the column name of
     * the internally managed field row.
     * <p>
     * <b>Note:</b> The parent application must call
     * <code>DataTable.addRow()</code> prior to using this method.
     * </p>
     *
     * @param aName Name of the column.
     * @param aValue Cell value to assign.
     *
     * @see <code>DataTable.addRow()</code>
     */
    public void setValueByName(String aName, boolean aValue)
    {
        if (mNewRow != null)
            setValueByName(mNewRow, aName, aValue);
    }

    /**
     * Assigns the value to the cell identified by the column name
     * contained within the field row.  Since the value parameter
     * represents a <i>Date</i>, this method accepts a format mask
     * parameter.
     *
     * @param aRow Field row.
     * @param aName Name of the column.
     * @param aValue Value to assign to the cell.
     *
     * @param aFormatMask Format mask string. Refer to <i>Field</i> for examples.
     *
     * @see <code>DataTable.getRow()</code>
     */
    public void setValueByName(FieldRow aRow, String aName, Date aValue, String aFormatMask)
    {
        if (aValue != null)
        {
            int colOffset = offsetByName(aName);
            markColumnAssigned(colOffset);
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(aFormatMask);
            aRow.setValue(colOffset, simpleDateFormat.format(aValue.getTime()));
        }
    }

    /**
     * Assigns the value to the cell identified by the column name
     * contained within the field row.
     *
     * @param aRow Field row.
     * @param aName Name of the column.
     * @param aValue Value to assign to the cell.
     *
     * @see <code>DataTable.getRow()</code>
     */
    public void setValueByName(FieldRow aRow, String aName, Date aValue)
    {
        setValueByName(aRow, aName, aValue, Field.FORMAT_DATETIME_DEFAULT);
    }

    /**
     * Assigns the value to the cell identified by the row offset and
     * column name within the table.
     *
     * @param aRow Row offset into the table.
     * @param aName Name of the column.
     * @param aValue Value to assign to the cell.
     *
     * @see <code>DataTable.rowCount()</code>
     */
    public void setValueByName(int aRow, String aName, Date aValue)
    {
        FieldRow fieldRow = getRow(aRow);
        if (fieldRow != null)
            setValueByName(fieldRow, aName, aValue);
    }

    /**
     * Assigns the value to the cell identified by the column name of
     * the internally managed field row. Since the value parameter
     * represents a <i>Date</i>, this method accepts a format mask
     * parameter.
     * <p>
     * <b>Note:</b> The parent application must call
     * <code>DataTable.addRow()</code> prior to using this method.
     * </p>
     *
     * @param aName Name of the column.
     * @param aValue Cell value to assign.
     * @param aFormatMask Format mask string. Refer to {@link Field} for examples.
     *
     * @see <code>DataTable.addRow()</code>
     */
    public void setValueByName(String aName, Date aValue, String aFormatMask)
    {
        if (mNewRow != null)
            setValueByName(mNewRow, aName, aValue, aFormatMask);
    }

    /**
     * Assigns the value to the cell identified by the row offset and
     * column name within the table. Since the value parameter
     * represents a <i>Date</i>, this method accepts a format mask
     * parameter.
     *
     * @param aRow Row offset into the table.
     * @param aName Name of the column.
     * @param aValue Value to assign to the cell.
     *
     * @param aFormatMask Format mask string. Refer to <i>Field</i> for examples.
     *
     * @see <code>DataTable.rowCount()</code>
     */
    public void setValueByName(int aRow, String aName, Date aValue, String aFormatMask)
    {
        FieldRow fieldRow = getRow(aRow);
        if (fieldRow != null)
            setValueByName(fieldRow, aName, aValue, aFormatMask);
    }

    /**
     * Assigns the value to the cell identified by the column name of
     * the internally managed field row.
     * <p>
     * <b>Note:</b> The parent application must call
     * <code>DataTable.addRow()</code> prior to using this method.
     * </p>
     *
     * @param aName Name of the column.
     * @param aValue Cell value to assign.
     *
     * @see <code>DataTable.addRow()</code>
     */
    public void setValueByName(String aName, Date aValue)
    {
        if (mNewRow != null)
            setValueByName(mNewRow, aName, aValue);
    }

    /**
     * Returns the cell value contained within the array of field rows at the
     * specified row offset and matching the column name.
     *
     * @param aRows Array list of field rows.
     * @param aRowOffset Row offset into the array list.
     * @param aName Name of the column.
     *
     * @return Cell value or an empty string if not found.
     */
    public String getValueByName(ArrayList<FieldRow> aRows, int aRowOffset, String aName)
    {
        FieldRow fieldRow = aRows.get(aRowOffset);
        if (fieldRow != null)
            return getValueByName(fieldRow, aName);
        else
            return StringUtils.EMPTY;
    }

    /**
     * Returns a <i>DataField</i> representing the cell contained within
     * the array of field rows at the specified row offset and matching the
     * column name.
     *
     * @param aRows Array list of field rows.
     * @param aRowOffset Row offset into the array list.
     * @param aName Name of the column.
     *
     * @return Data field or <i>null</i> if not found.
     */
    public DataField getFieldByName(ArrayList<FieldRow> aRows, int aRowOffset, String aName)
    {
        DataField dataField = null;

        FieldRow fieldRow = aRows.get(aRowOffset);
        if (fieldRow != null)
        {
            int colOffset = mColumns.getOffsetByName(aName);
            dataField = new DataField(mColumns.getByOffset(colOffset));
            dataField.setValues(fieldRow.getValues(colOffset));
        }

        return dataField;
    }

    /**
     * Process the table information (name, columns, features, rows)
     * through the digital hash algorithm.
     *
     * @param aHash Digital hash instance.
     * @param anIsFeatureIncluded Should features be included.
     *
     * @throws IOException Triggered by hash algorithm.
     */
    public void processHash(DigitalHash aHash, boolean anIsFeatureIncluded)
        throws IOException
    {
        DataBag rowBag;

        aHash.processBuffer(getName());
        if (anIsFeatureIncluded)
        {
            for (Map.Entry<String, String> featureEntry : getFeatures().entrySet())
            {
                aHash.processBuffer(featureEntry.getKey());
                aHash.processBuffer(featureEntry.getValue());
            }
        }
        int rowCount = rowCount();
        for (int row = 0; row < rowCount; row++)
        {
            rowBag = getRowAsBag(row);
            rowBag.processHash(aHash, anIsFeatureIncluded);
        }
    }

    /**
     * Generates a unique hash string using the MD5 algorithm using
     * the table information.
     *
     * @param anIsFeatureIncluded Should feature name/values be included?
     *
     * @return Unique hash string.
     */
    public String generateUniqueHash(boolean anIsFeatureIncluded)
    {
        String hashId;
        DataBag rowBag;

        DigitalHash digitalHash = new DigitalHash();
        try
        {
            processHash(digitalHash, anIsFeatureIncluded);
            hashId = digitalHash.getHashSequence();
        }
        catch (IOException e)
        {
            UUID uniqueId = UUID.randomUUID();
            hashId = uniqueId.toString();
        }

        return hashId;
    }

    /**
     * Returns one or more field rows that match the search criteria of the
     * parameters provided.  Each row of the table will be examined to determine
     * if a cell identified by name evaluates true when the operator and value
     * are applied to it.
     * <p>
     * <b>Note:</b> This method supports text based logical operators only.
     * You should use other <code>findValue()</code> methods for different
     * data types.
     * </p>
     *
     * @param aName Column name.
     * @param anOperator Logical operator.
     * @param aValue Comparison value.
     *
     * @return Array list of matching field rows or <i>null</i> if none evaluate true.
     */
    public ArrayList<FieldRow> findValue(String aName, Field.Operator anOperator, String aValue)
    {
        String valueString;
        FieldRow fieldRow;
        Matcher regexMatcher = null;
        Pattern regexPattern = null;
        ArrayList<FieldRow> matchingRows = new ArrayList<FieldRow>();

        int rowCount = rowCount();
        int colOffset = offsetByName(aName);
        if ((aValue != null) && (colOffset != -1) && (rowCount > 0))
        {
            for (int row = 0; row < rowCount; row++)
            {
                fieldRow = getRow(row);
                valueString = fieldRow.getValue(colOffset);

                switch (anOperator)
                {
                    case NOT_EMPTY:
                        if (StringUtils.isNotEmpty(valueString))
                            matchingRows.add(fieldRow);
                        break;
                    case EQUAL:
                        if (StringUtils.equals(valueString, aValue))
                            matchingRows.add(fieldRow);
                        break;
                    case NOT_EQUAL:
                        if (! StringUtils.equals(valueString, aValue))
                            matchingRows.add(fieldRow);
                        break;
                    case CONTAINS:
                        if (StringUtils.contains(valueString, aValue))
                            matchingRows.add(fieldRow);
                        break;
                    case STARTS_WITH:
                        if (StringUtils.startsWith(valueString, aValue))
                            matchingRows.add(fieldRow);
                        break;
                    case ENDS_WITH:
                        if (StringUtils.endsWith(valueString, aValue))
                            matchingRows.add(fieldRow);
                        break;
                    case EMPTY:
                        if (StringUtils.isEmpty(valueString))
                            matchingRows.add(fieldRow);
                        break;
                    case REGEX: // http://www.regular-expressions.info/java.html
                        if (regexPattern == null)
                            regexPattern = Pattern.compile(aValue);
                        if (regexMatcher == null)
                            regexMatcher = regexPattern.matcher(valueString);
                        else
                            regexMatcher.reset(valueString);
                        if (regexMatcher.find())
                            matchingRows.add(fieldRow);
                        break;
                }
            }
        }

        return matchingRows;
    }

    /**
     * Returns one or more field rows that match the search criteria of the
     * parameters provided in a case insensitive manner.  Each row of the
     * table will be examined to determine if a cell identified by name
     * evaluates true when the operator and value are applied to it.
     * <p>
     * <b>Note:</b> This method supports text based logical operators only.
     * You should use other <code>findValue()</code> methods for different
     * data types.
     * </p>
     *
     * @param aName Column name.
     * @param anOperator Logical operator.
     * @param aValue Comparison value.
     *
     * @return Array list of matching field rows or <i>null</i> if none evaluate true.
     */
    public ArrayList<FieldRow> findValueInsensitive(String aName, Field.Operator anOperator, String aValue)
    {
        FieldRow fieldRow;
        String valueString;
        Matcher regexMatcher = null;
        Pattern regexPattern = null;
        ArrayList<FieldRow> matchingRows = new ArrayList<FieldRow>();

        int rowCount = rowCount();
        int colOffset = offsetByName(aName);
        if ((aValue != null) && (colOffset != -1) && (rowCount > 0))
        {
            for (int row = 0; row < rowCount; row++)
            {
                fieldRow = getRow(row);
                valueString = fieldRow.getValue(colOffset);

                switch (anOperator)
                {
                    case EQUAL:
                        if (StringUtils.equalsIgnoreCase(valueString, aValue))
                            matchingRows.add(fieldRow);
                        break;
                    case NOT_EQUAL:
                        if (! StringUtils.equalsIgnoreCase(valueString, aValue))
                            matchingRows.add(fieldRow);
                        break;
                    case CONTAINS:
                        if (StringUtils.containsIgnoreCase(valueString, aValue))
                            matchingRows.add(fieldRow);
                        break;
                    case STARTS_WITH:
                        if (StringUtils.startsWithIgnoreCase(valueString, aValue))
                            matchingRows.add(fieldRow);
                        break;
                    case ENDS_WITH:
                        if (StringUtils.endsWithIgnoreCase(valueString, aValue))
                            matchingRows.add(fieldRow);
                        break;
                    case EMPTY:
                        if (StringUtils.isEmpty(valueString))
                            matchingRows.add(fieldRow);
                        break;
                    case REGEX: // http://www.regular-expressions.info/java.html
                        if (regexPattern == null)
                            regexPattern = Pattern.compile(aValue, Pattern.CASE_INSENSITIVE);
                        if (regexMatcher == null)
                            regexMatcher = regexPattern.matcher(valueString);
                        else
                            regexMatcher.reset(valueString);
                        if (regexMatcher.find())
                            matchingRows.add(fieldRow);
                        break;
                }
            }
        }

        return matchingRows;
    }

    /**
     * Returns one or more field rows that match the search criteria of the
     * parameters provided in a case insensitive manner.  Each row of the
     * table will be examined to determine if a cell identified by name
     * evaluates true when the operator and value(s) are applied to it.
     * <p>
     * <b>Note:</b> This method supports numeric based logical operators only.
     * You should use other <code>findValue()</code> methods for different
     * data types.
     * </p>
     *
     * @param aName Column name.
     * @param anOperator Logical operator.
     * @param aValue1 Primary comparison value.
     * @param aValue2 Secondary comparison value (ignored unless the logical
     *                operator is <i>Field.Operator.BETWEEN</i>).
     *
     * @return Array list of matching field rows or <i>null</i> if none evaluate true.
     */
    public ArrayList<FieldRow> findValue(String aName, Field.Operator anOperator,
                                         int aValue1, int aValue2)
    {
        int nativeValue;
        FieldRow fieldRow;
        ArrayList<FieldRow> matchingRows = new ArrayList<FieldRow>();

        int rowCount = rowCount();
        int colOffset = offsetByName(aName);
        if ((colOffset != -1) && (rowCount > 0))
        {
            DataField dataField = mColumns.getByOffset(colOffset);

            for (int row = 0; row < rowCount; row++)
            {
                fieldRow = getRow(row);

                if (dataField.isTypeNumber())
                {
                    nativeValue = Field.createInt(fieldRow.getValue(colOffset));
                    switch (anOperator)
                    {
                        case EQUAL:
                            if (nativeValue == aValue1)
                                matchingRows.add(fieldRow);
                            break;
                        case NOT_EQUAL:
                            if (nativeValue != aValue1)
                                matchingRows.add(fieldRow);
                            break;
                        case GREATER_THAN:
                            if (nativeValue > aValue1)
                                matchingRows.add(fieldRow);
                            break;
                        case GREATER_THAN_EQUAL:
                            if (nativeValue >= aValue1)
                                matchingRows.add(fieldRow);
                            break;
                        case LESS_THAN:
                            if (nativeValue < aValue1)
                                matchingRows.add(fieldRow);
                            break;
                        case LESS_THAN_EQUAL:
                            if (nativeValue <= aValue1)
                                matchingRows.add(fieldRow);
                            break;
                        case BETWEEN:
                            if ((nativeValue > aValue1) && (nativeValue < aValue2))
                                matchingRows.add(fieldRow);
                            break;
                        case BETWEEN_INCLUSIVE:
                            if ((nativeValue >= aValue1) && (nativeValue <= aValue2))
                                matchingRows.add(fieldRow);
                            break;
                        case EMPTY:
                            if (StringUtils.isEmpty(fieldRow.getValue(colOffset)))
                                matchingRows.add(fieldRow);
                            break;
                    }
                }
            }
        }

        return matchingRows;
    }

    /**
     * Returns one or more field rows that match the search criteria of the
     * parameters provided in a case insensitive manner.  Each row of the
     * table will be examined to determine if a cell identified by name
     * evaluates true when the operator and value(s) are applied to it.
     * <p>
     * <b>Note:</b> This method supports numeric based logical operators only.
     * You should use other <code>findValue()</code> methods for different
     * data types.
     * </p>
     *
     * @param aName Column name.
     * @param anOperator Logical operator.
     * @param aValue1 Primary comparison value.
     * @param aValue2 Secondary comparison value (ignored unless the logical
     *                operator is <i>Field.Operator.BETWEEN</i>).
     *
     * @return Array list of matching field rows or <i>null</i> if none evaluate true.
     */
    public ArrayList<FieldRow> findValue(String aName, Field.Operator anOperator,
                                         long aValue1, long aValue2)
    {
        long nativeValue;
        FieldRow fieldRow;
        ArrayList<FieldRow> matchingRows = new ArrayList<FieldRow>();

        int rowCount = rowCount();
        int colOffset = offsetByName(aName);
        if ((colOffset != -1) && (rowCount > 0))
        {
            DataField dataField = mColumns.getByOffset(colOffset);

            for (int row = 0; row < rowCount; row++)
            {
                fieldRow = getRow(row);

                if ((dataField.isTypeNumber()) || (dataField.isTypeDateOrTime()))
                {
                    nativeValue = Field.createLong(fieldRow.getValue(colOffset));
                    switch (anOperator)
                    {
                        case EQUAL:
                            if (nativeValue == aValue1)
                                matchingRows.add(fieldRow);
                            break;
                        case NOT_EQUAL:
                            if (nativeValue != aValue1)
                                matchingRows.add(fieldRow);
                            break;
                        case GREATER_THAN:
                            if (nativeValue > aValue1)
                                matchingRows.add(fieldRow);
                            break;
                        case GREATER_THAN_EQUAL:
                            if (nativeValue >= aValue1)
                                matchingRows.add(fieldRow);
                            break;
                        case LESS_THAN:
                            if (nativeValue < aValue1)
                                matchingRows.add(fieldRow);
                            break;
                        case LESS_THAN_EQUAL:
                            if (nativeValue <= aValue1)
                                matchingRows.add(fieldRow);
                            break;
                        case BETWEEN:
                            if ((nativeValue > aValue1) && (nativeValue < aValue2))
                                matchingRows.add(fieldRow);
                            break;
                        case BETWEEN_INCLUSIVE:
                            if ((nativeValue >= aValue1) && (nativeValue <= aValue2))
                                matchingRows.add(fieldRow);
                            break;
                        case EMPTY:
                            if (StringUtils.isEmpty(fieldRow.getValue(colOffset)))
                                matchingRows.add(fieldRow);
                            break;
                    }
                }
            }
        }

        return matchingRows;
    }

    /**
     * Returns one or more field rows that match the search criteria of the
     * parameters provided in a case insensitive manner.  Each row of the
     * table will be examined to determine if a cell identified by name
     * evaluates true when the operator and value(s) are applied to it.
     * <p>
     * <b>Note:</b> This method supports date/time based logical operators only.
     * You should use other <code>findValue()</code> methods for different
     * data types.
     * </p>
     *
     * @param aName Column name.
     * @param anOperator Logical operator.
     * @param aValue1 Primary comparison value.
     * @param aValue2 Secondary comparison value (ignored unless the logical
     *                operator is <i>Field.Operator.BETWEEN</i>).
     *
     * @return Array list of matching field rows or <i>null</i> if none evaluate true.
     */
    public ArrayList<FieldRow> findValue(String aName, Field.Operator anOperator,
                                         Date aValue1, Date aValue2)
    {
        long nativeValue;
        FieldRow fieldRow;
        long longValue1 = aValue1.getTime();
        long longValue2 = aValue2.getTime();
        ArrayList<FieldRow> matchingRows = new ArrayList<FieldRow>();

        int rowCount = rowCount();
        int colOffset = offsetByName(aName);
        if ((colOffset != -1) && (rowCount > 0))
        {
            DataField dataField = mColumns.getByOffset(colOffset);

            for (int row = 0; row < rowCount; row++)
            {
                fieldRow = getRow(row);

                if (dataField.isTypeDateOrTime())
                {
                    nativeValue = Field.createDate(fieldRow.getValue(colOffset)).getTime();
                    switch (anOperator)
                    {
                        case EQUAL:
                            if (nativeValue == longValue1)
                                matchingRows.add(fieldRow);
                            break;
                        case NOT_EQUAL:
                            if (nativeValue != longValue1)
                                matchingRows.add(fieldRow);
                            break;
                        case GREATER_THAN:
                            if (nativeValue > longValue1)
                                matchingRows.add(fieldRow);
                            break;
                        case GREATER_THAN_EQUAL:
                            if (nativeValue >= longValue1)
                                matchingRows.add(fieldRow);
                            break;
                        case LESS_THAN:
                            if (nativeValue < longValue1)
                                matchingRows.add(fieldRow);
                            break;
                        case LESS_THAN_EQUAL:
                            if (nativeValue <= longValue1)
                                matchingRows.add(fieldRow);
                            break;
                        case BETWEEN:
                            if ((nativeValue > longValue1) && (nativeValue < longValue2))
                                matchingRows.add(fieldRow);
                            break;
                        case BETWEEN_INCLUSIVE:
                            if ((nativeValue >= longValue1) && (nativeValue <= longValue2))
                                matchingRows.add(fieldRow);
                            break;
                        case EMPTY:
                            if (StringUtils.isEmpty(fieldRow.getValue(colOffset)))
                                matchingRows.add(fieldRow);
                            break;
                    }
                }
            }
        }

        return matchingRows;
    }

    /**
     * Returns one or more field rows that match the search criteria of the
     * parameters provided in a case insensitive manner.  Each row of the
     * table will be examined to determine if a cell identified by name
     * evaluates true when the operator and value(s) are applied to it.
     * <p>
     * <b>Note:</b> This method supports numeric based logical operators only.
     * You should use other <code>findValue()</code> methods for different
     * data types.
     * </p>
     *
     * @param aName Column name.
     * @param anOperator Logical operator.
     * @param aValue1 Primary comparison value.
     * @param aValue2 Secondary comparison value (ignored unless the logical
     *                operator is <i>Field.Operator.BETWEEN</i>).
     *
     * @return Array list of matching field rows or <i>null</i> if none evaluate true.
     */
    public ArrayList<FieldRow> findValue(String aName, Field.Operator anOperator,
                                         double aValue1, double aValue2)
    {
        double nativeValue;
        FieldRow fieldRow;
        ArrayList<FieldRow> matchingRows = new ArrayList<FieldRow>();

        int rowCount = rowCount();
        int colOffset = offsetByName(aName);
        if ((colOffset != -1) && (rowCount > 0))
        {
            DataField dataField = mColumns.getByOffset(colOffset);

            for (int row = 0; row < rowCount; row++)
            {
                fieldRow = getRow(row);

                if (dataField.isTypeNumber())
                {
                    nativeValue = Field.createDouble(fieldRow.getValue(colOffset));
                    switch (anOperator)
                    {
                        case EQUAL:
                            if (nativeValue == aValue1)
                                matchingRows.add(fieldRow);
                            break;
                        case NOT_EQUAL:
                            if (nativeValue != aValue1)
                                matchingRows.add(fieldRow);
                            break;
                        case GREATER_THAN:
                            if (nativeValue > aValue1)
                                matchingRows.add(fieldRow);
                            break;
                        case GREATER_THAN_EQUAL:
                            if (nativeValue >= aValue1)
                                matchingRows.add(fieldRow);
                            break;
                        case LESS_THAN:
                            if (nativeValue < aValue1)
                                matchingRows.add(fieldRow);
                            break;
                        case LESS_THAN_EQUAL:
                            if (nativeValue <= aValue1)
                                matchingRows.add(fieldRow);
                            break;
                        case BETWEEN:
                            if ((nativeValue > aValue1) && (nativeValue < aValue2))
                                matchingRows.add(fieldRow);
                            break;
                        case BETWEEN_INCLUSIVE:
                            if ((nativeValue >= aValue1) && (nativeValue <= aValue2))
                                matchingRows.add(fieldRow);
                            break;
                        case EMPTY:
                            if (StringUtils.isEmpty(fieldRow.getValue(colOffset)))
                                matchingRows.add(fieldRow);
                            break;
                    }
                }
            }
        }

        return matchingRows;
    }

    /**
     * Convenience method that retrieves the first row in an array as
     * a bag.
     * <p>
     * <b>Note:</b>The bag is a cloned copy of the table row and
     * should not be used to update the table.  Instead, you should
     * use <code>Field.firstFieldRow()</code>  and perform updates via
     * the <i>FieldRow</i> instance.
     * </p>
     *
     * @param aFieldRows Array of field rows.
     *
     * @return Bag of fields representing the field row.
     */
    public DataBag firstRowAsBag(ArrayList<FieldRow> aFieldRows)
    {
        if ((aFieldRows != null) && (aFieldRows.size() > 0))
            return getRowAsBag(aFieldRows.get(0));
        else
            return null;
    }

    /**
     * Returns one or more field rows that match the search criteria of the
     * parameters provided in a case insensitive manner.  Each row of the
     * table will be examined to determine if a cell identified by name
     * evaluates true when the operator and value(s) are applied to it.
     * <p>
     * <b>Note:</b> This method supports numeric based logical operators only.
     * You should use other <code>findValue()</code> methods for different
     * data types.
     * </p>
     *
     * @param aName Column name.
     * @param anOperator Logical operator.
     * @param aValue1 Primary comparison value.
     * @param aValue2 Secondary comparison value (ignored unless the logical
     *                operator is <i>Field.Operator.BETWEEN</i>).
     *
     * @return Array list of matching field rows or <i>null</i> if none evaluate true.
     */
    public ArrayList<FieldRow> findValue(String aName, Field.Operator anOperator,
                                         float aValue1, float aValue2)
    {
        double doubleValue1 = (double) aValue1;
        double doubleValue2 = (double) aValue2;

        return findValue(aName, anOperator, doubleValue1, doubleValue2);
    }

    final Comparator<FieldRow> ROW_SORT_ASCENDING = new Comparator<FieldRow>()
    {
        public int compare(FieldRow aRow1, FieldRow aRow2)
        {
            int colOffset = offsetByName(mSortFieldName);
            DataField dataField = mColumns.getByOffset(colOffset);
            String cellValue1 = aRow1.getValue(colOffset);
            String cellValue2 = aRow2.getValue(colOffset);

            switch (dataField.getType())
            {
                case Integer:
                    Integer integerValue1 = new Integer(cellValue1);
                    Integer integerValue2 = new Integer(cellValue2);
                    return integerValue1.compareTo(integerValue2);
                case Long:
                    Long longValue1 = new Long(cellValue1);
                    Long longValue2 = new Long(cellValue2);
                    return longValue1.compareTo(longValue2);
                case Float:
                    Float floatValue1 = new Float(cellValue1);
                    Float floatValue2 = new Float(cellValue2);
                    return floatValue1.compareTo(floatValue2);
                case Double:
                    Double doubleValue1 = new Double(cellValue1);
                    Double doubleValue2 = new Double(cellValue2);
                    return doubleValue1.compareTo(doubleValue2);
                case Boolean:
                    Boolean booleanValue1 = Field.isValueTrue(cellValue1);
                    Boolean booleanValue2 = Field.isValueTrue(cellValue2);
                    return booleanValue1.compareTo(booleanValue2);
                case Date:
                case Time:
                case DateTime:
                    Date dateValue1 = Field.createDate(cellValue1);
                    Date dateValue2 = Field.createDate(cellValue2);
                    return dateValue1.compareTo(dateValue2);
                default:
                    return cellValue1.compareToIgnoreCase(cellValue2);
            }
        }
    };

    final Comparator<FieldRow> ROW_SORT_DESCENDING = new Comparator<FieldRow>()
    {
        public int compare(FieldRow aRow1, FieldRow aRow2)
        {
            int colOffset = offsetByName(mSortFieldName);
            DataField dataField = mColumns.getByOffset(colOffset);
            String cellValue1 = aRow1.getValue(colOffset);
            String cellValue2 = aRow2.getValue(colOffset);

            switch (dataField.getType())
            {
                case Integer:
                    Integer integerValue1 = new Integer(cellValue1);
                    Integer integerValue2 = new Integer(cellValue2);
                    return integerValue2.compareTo(integerValue1);
                case Long:
                    Long longValue1 = new Long(cellValue1);
                    Long longValue2 = new Long(cellValue2);
                    return longValue2.compareTo(longValue1);
                case Float:
                    Float floatValue1 = new Float(cellValue1);
                    Float floatValue2 = new Float(cellValue2);
                    return floatValue2.compareTo(floatValue1);
                case Double:
                    Double doubleValue1 = new Double(cellValue1);
                    Double doubleValue2 = new Double(cellValue2);
                    return doubleValue2.compareTo(doubleValue1);
                case Boolean:
                    Boolean booleanValue1 = Field.isValueTrue(cellValue1);
                    Boolean booleanValue2 = Field.isValueTrue(cellValue2);
                    return booleanValue2.compareTo(booleanValue1);
                case Date:
                case Time:
                case DateTime:
                    Date dateValue1 = Field.createDate(cellValue1);
                    Date dateValue2 = Field.createDate(cellValue2);
                    return dateValue2.compareTo(dateValue1);
                default:
                    return cellValue2.compareToIgnoreCase(cellValue1);
            }
        }
    };

    /**
     * Sorts the rows of the table based on the column name and sort
     * order parameters.
     *
     * @param aName Column name.
     * @param anOrder Sort order (ascending, descending).
     */
    public void sortByColumn(String aName, Field.Order anOrder)
    {
        int rowCount = rowCount();
        int colOffset = offsetByName(aName);

        if ((colOffset != -1) && (rowCount > 1) && (anOrder != Field.Order.UNDEFINED))
        {
            mSortFieldName = aName;

            if (anOrder == Field.Order.ASCENDING)
                Collections.sort(mRows, ROW_SORT_ASCENDING);
            else
                Collections.sort(mRows, ROW_SORT_DESCENDING);
            mSortFieldName = StringUtils.EMPTY;
        }
    }

    /**
     * Add a unique feature to this table.  A feature enhances the core
     * capability of the table.  Standard features are listed below.
     * <ul>
     *     <li>Field.FEATURE_OPERATION_NAME</li>
     * </ul>
     *
     * @param aName Name of the feature.
     * @param aValue Value to associate with the feature.
     */
    public void addFeature(String aName, String aValue)
    {
        mFeatures.put(aName, aValue);
    }

    /**
     * Add a unique feature to this table.  A feature enhances the core
     * capability of the table.
     *
     * @param aName Name of the feature.
     * @param aValue Value to associate with the feature.
     */
    public void addFeature(String aName, int aValue)
    {
        addFeature(aName, Integer.toString(aValue));
    }

    /**
     * Enabling the feature will add the name and assign it a
     * value of <i>StrUtl.STRING_TRUE</i>.
     *
     * @param aName Name of the feature.
     */
    public void enableFeature(String aName)
    {
        mFeatures.put(aName, StrUtl.STRING_TRUE);
    }

    /**
     * Disabling a feature will remove its name and value
     * from the internal list.
     *
     * @param aName Name of feature.
     */
    public void disableFeature(String aName)
    {
        mFeatures.remove(aName);
    }

    /**
     * Returns <i>true</i> if the feature was previously
     * added and assigned a value.
     *
     * @param aName Name of feature.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isFeatureAssigned(String aName)
    {
        return (getFeature(aName) != null);
    }

    /**
     * Returns <i>true</i> if the feature was previously
     * added and assigned a value of <i>StrUtl.STRING_TRUE</i>.
     *
     * @param aName Name of feature.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isFeatureTrue(String aName)
    {
        return StrUtl.stringToBoolean(mFeatures.get(aName));
    }

    /**
     * Returns <i>true</i> if the feature was previously
     * added and not assigned a value of <i>StrUtl.STRING_TRUE</i>.
     *
     * @param aName Name of feature.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isFeatureFalse(String aName)
    {
        return !StrUtl.stringToBoolean(mFeatures.get(aName));
    }

    /**
     * Returns <i>true</i> if the feature was previously
     * added and its value matches the one provided as a
     * parameter.
     *
     * @param aName Feature name.
     * @param aValue Feature value to match.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isFeatureEqual(String aName, String aValue)
    {
        String featureValue = getFeature(aName);
        return StringUtils.equalsIgnoreCase(featureValue, aValue);
    }

    /**
     * Count of unique features assigned to this table.
     *
     * @return Feature count.
     */
    public int featureCount()
    {
        return mFeatures.size();
    }

    /**
     * Returns the String associated with the feature name or
     * <i>null</i> if the name could not be found.
     *
     * @param aName Feature name.
     *
     * @return Feature value or <i>null</i>
     */
    public String getFeature(String aName)
    {
        return mFeatures.get(aName);
    }

    /**
     * Returns the int associated with the feature name.
     *
     * @param aName Feature name.
     *
     * @return Feature value or <i>null</i>
     */
    public int getFeatureAsInt(String aName)
    {
        return Field.createInt(getFeature(aName));
    }

    /**
     * Removes all features assigned to this object instance.
     */
    public void clearFeatures()
    {
        mFeatures.clear();
    }

    /**
     * Assigns the hash map of features to the list.
     *
     * @param aFeatures Feature list.
     */
    public void setFeatures(HashMap<String, String> aFeatures)
    {
        if (aFeatures != null)
            mFeatures = new HashMap<String, String>(aFeatures);
    }

    /**
     * Indicates whether some other object is "equal to" this one.
     *
     * @param anObject Reference object with which to compare.
     * @return  {@code true} if this object is the same as the anObject
     *          argument; {@code false} otherwise.
     */
    @Override
    public boolean equals(Object anObject)
    {
        if (this == anObject)
            return true;
        if (anObject == null || getClass() != anObject.getClass())
            return false;

        DataTable dataTable = (DataTable) anObject;

        if (mColumns != null ? !mColumns.equals(dataTable.mColumns) : dataTable.mColumns != null)
            return false;
        if (mRows != null ? !mRows.equals(dataTable.mRows) : dataTable.mRows != null)
            return false;
        if (mName != null ? !mName.equals(dataTable.mName) : dataTable.mName != null)
            return false;

        return !(mFeatures != null ? !mFeatures.equals(dataTable.mFeatures) : dataTable.mFeatures != null);
    }

    /**
     * Returns a hash code value for the object. This method is
     * supported for the benefit of hash tables such as those provided by
     * {@link java.util.HashMap}.
     *
     * @return A hash code value for this object.
     */
    @Override
    public int hashCode()
    {
        int result = mColumns != null ? mColumns.hashCode() : 0;
        result = 31 * result + (mRows != null ? mRows.hashCode() : 0);
        result = 31 * result + (mName != null ? mName.hashCode() : 0);
        result = 31 * result + (mFeatures != null ? mFeatures.hashCode() : 0);

        return result;
    }

    /**
     * Returns a read-only copy of the internal map containing
     * feature list.
     *
     * @return Internal feature map instance.
     */
    public final HashMap<String, String> getFeatures()
    {
        return mFeatures;
    }

    /**
     * Add an application defined property to the table.
     * <p>
     * <b>Notes:</b>
     * </p>
     * <ul>
     * <li>The goal of the DataTable is to strike a balance between
     * providing enough properties to adequately model application
     * related data without overloading it.</li>
     * <li>This method offers a mechanism to capture additional
     * (application specific) properties that may be needed.</li>
     * <li>Properties added with this method are transient and
     * will not be stored when saved.</li>
     * </ul>
     *
     * @param aName    Property name (duplicates are not supported).
     * @param anObject Instance of an object.
     */
    public void addProperty(String aName, Object anObject)
    {
        if (mProperties == null)
            mProperties = new HashMap<String, Object>();
        mProperties.put(aName, anObject);
    }

    /**
     * Returns the object associated with the property name or
     * <i>null</i> if the name could not be matched.
     *
     * @param aName Name of the property.
     *
     * @return Instance of an object.
     */
    public Object getProperty(String aName)
    {
        if (mProperties == null)
            return null;
        else
            return mProperties.get(aName);
    }

    /**
     * Removes all application defined properties assigned to this table.
     */
    public void clearProperties()
    {
        if (mProperties != null)
            mProperties.clear();
    }

    /**
     * Returns the property map instance managed by the table or <i>null</i>
     * if empty.
     *
     * @return Hash map instance.
     */
    public HashMap<String, Object> getProperties()
    {
        return mProperties;
    }
}
