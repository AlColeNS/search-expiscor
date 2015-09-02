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

package com.nridge.core.io.csv;

import com.nridge.core.base.std.StrUtl;
import com.nridge.core.base.field.Field;
import com.nridge.core.base.field.FieldRow;
import com.nridge.core.base.field.data.DataField;
import com.nridge.core.base.field.data.DataTable;

import org.apache.commons.lang3.StringUtils;
import org.supercsv.io.*;
import org.supercsv.prefs.CsvPreference;

import java.io.*;
import java.util.List;

/**
 * The DataTableCSV provides a collection of methods that can generate/load
 * a CSV representation of a <i>DataTable</i> object.
 * <p>
 * This class utilizes the
 * <a href="http://supercsv.sourceforge.net">SuperCSV</a>
 * framework to manage these transformations.
 * </p>
 *
 * @author Al Cole
 * @since 1.0
 */
public class DataTableCSV
{
    private DataTable mDataTable;
    private boolean mIsFieldNamePreferred;

    /**
     * Constructor that identifies a table prior to a save operation.
     *
     * @param aDataTable Data table of fields.
     */
    public DataTableCSV(DataTable aDataTable)
    {
        mDataTable = aDataTable;
    }

    /**
     * Return an instance to the internally managed data table.
     *
     * @return Data table instance.
     */
    public DataTable getTable()
    {
        return mDataTable;
    }

    /**
     * If assigned to <i>true</i>, then the field names will be used
     * for the header row.
     *
     * @param aIsFieldNamePreferred Field name preference flag.
     */
    public void setFieldNamePreferred(boolean aIsFieldNamePreferred)
    {
        mIsFieldNamePreferred = aIsFieldNamePreferred;
    }

    private int visibleColumnCount(DataTable aTable)
    {
        int totalCount = 0;
        DataField dataField;
        int columnCount = aTable.columnCount();

        for (int col = 0; col < columnCount; col++)
        {
            dataField = aTable.getColumn(col);
            if (dataField.isFeatureFalse(Field.FEATURE_IS_HIDDEN))
                totalCount++;
        }
        return totalCount;
    }

    /**
     * Saves the previous assigned table (e.g. via constructor or set method)
     * to the <i>PrintWriter</i> output stream.
     *
     * @param aPW Print writer output stream.
     * @param aWithHeaders If <i>true</i>, then column headers will be stored
     *                     in the CSV file.
     *
     * @throws IOException I/O related exception.
     */
    public void save(PrintWriter aPW, boolean aWithHeaders)
        throws IOException
    {
        int colOffset;

        try (CsvListWriter csvListWriter = new CsvListWriter(aPW, CsvPreference.EXCEL_PREFERENCE))
        {
            FieldRow fieldRow;
            DataField dataField;
            int rowCount = mDataTable.rowCount();
            int colCount = mDataTable.columnCount();

            if (aWithHeaders)
            {
                colOffset = 0;
                String headerName;
                String[] headerColumns = new String[visibleColumnCount(mDataTable)];
                for (int col = 0; col < colCount; col++)
                {
                    dataField = mDataTable.getColumn(col);
                    if (dataField.isFeatureFalse(Field.FEATURE_IS_HIDDEN))
                    {
                        if (mIsFieldNamePreferred)
                            headerName = dataField.getName();
                        else if (StringUtils.isNotEmpty(dataField.getTitle()))
                            headerName = dataField.getTitle();
                        else
                            headerName = dataField.getName();
                        headerColumns[colOffset++] = headerName;
                    }
                }
                csvListWriter.writeHeader(headerColumns);
            }
            String[] rowCells = new String[visibleColumnCount(mDataTable)];
            for (int row = 0; row < rowCount; row++)
            {
                fieldRow = mDataTable.getRow(row);

                colOffset = 0;
                for (int col = 0; col < colCount; col++)
                {
                    dataField = mDataTable.getColumn(col);
                    if (dataField.isFeatureFalse(Field.FEATURE_IS_HIDDEN))
                        rowCells[colOffset++] = fieldRow.collapse(col);
                }
                csvListWriter.write(rowCells);
            }
        }
    }

    /**
     * Saves the previous assigned table (e.g. via constructor or set method)
     * to the <i>PrintWriter</i> output stream.
     *
     * @param aPathFileName Absolute file name.
     * @param aWithHeaders If <i>true</i>, then column headers will be stored
     *                     in the CSV file.
     *
     * @throws IOException I/O related exception.
     */
    public void save(String aPathFileName, boolean aWithHeaders)
        throws IOException
    {
        try (PrintWriter printWriter = new PrintWriter(aPathFileName, StrUtl.CHARSET_UTF_8))
        {
            save(printWriter, aWithHeaders);
        }
        catch (Exception e)
        {
            throw new IOException(aPathFileName + ": " + e.getMessage());
        }
    }

    /**
     * Parses a CSV file identified by the path/file name parameter
     * and loads it into a <i>DataTable</i> depending on how it was
     * typed in the XML file.
     *
     * @param aReader File reader input stream.
     * @param aWithHeaders If <i>true</i>, then column headers will be
     *                     recognized in the CSV file.
     *
     * @throws IOException I/O related exception.
     */
    @SuppressWarnings({"UnusedDeclaration"})
    public void load(Reader aReader, boolean aWithHeaders)
        throws IOException
    {
        try (CsvListReader csvListReader = new CsvListReader(aReader, CsvPreference.EXCEL_PREFERENCE))
        {
            String cellValue;
            String mvDelimiter;
            DataField dataField;
            List<String> rowCells;
            String[] columnHeaders;
            int columnCount, adjCount;

// We are grabbing a reference in case we want to use it in the future (unused now).

            if (aWithHeaders)
                columnHeaders = csvListReader.getHeader(aWithHeaders);

            columnCount = mDataTable.columnCount();
            do
            {
                rowCells = csvListReader.read();
                if (rowCells != null)
                {
                    adjCount = Math.min(rowCells.size(), columnCount);
                    mDataTable.newRow();
                    for (int col = 0; col < adjCount; col++)
                    {
                        cellValue = rowCells.get(col);
                        if (StringUtils.isNotEmpty(cellValue))
                        {
                            dataField = mDataTable.getColumn(col);
                            if (dataField.isMultiValue())
                            {
                                mvDelimiter = dataField.getFeature(Field.FEATURE_MV_DELIMITER);
                                if (StringUtils.isNotEmpty(mvDelimiter))
                                    mDataTable.setValuesByColumn(col, StrUtl.expandToList(cellValue, mvDelimiter.charAt(0)));
                                else
                                    mDataTable.setValuesByColumn(col, StrUtl.expandToList(cellValue, StrUtl.CHAR_PIPE));
                            }
                            else
                                mDataTable.setValueByColumn(col, cellValue);
                        }
                    }
                    mDataTable.addRow();
                }
            }
            while (rowCells != null);

        }
        catch (Exception e)
        {
            throw new IOException(e.getMessage());
        }
    }

    /**
     * Parses a CSV file identified by the path/file name parameter
     * and loads it into a <i>DataTable</i>.
     *
     * @param aPathFileName Absolute file name.
     * @param aWithHeaders If <i>true</i>, then column headers will be
     *                     recognized in the CSV file.
     *
     * @throws IOException I/O related exception.
     */
    public void load(String aPathFileName, boolean aWithHeaders)
        throws IOException
    {
        File csvFile = new File(aPathFileName);
        if (!csvFile.exists())
            throw new IOException(aPathFileName + ": Does not exist.");

        try (FileReader fileReader = new FileReader(csvFile))
        {
            load(fileReader, aWithHeaders);
        }
        catch (Exception e)
        {
            throw new IOException(aPathFileName + ": " + e.getMessage());
        }
    }
}
