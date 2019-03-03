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

import com.nridge.core.base.field.Field;
import com.nridge.core.base.field.data.DataField;
import com.nridge.core.base.field.data.DataBag;
import com.nridge.core.base.field.data.DataTextField;
import com.nridge.core.base.std.StrUtl;
import org.apache.commons.lang3.StringUtils;
import org.supercsv.io.CsvListReader;
import org.supercsv.io.CsvListWriter;
import org.supercsv.prefs.CsvPreference;

import java.io.*;
import java.util.List;

/**
 * The DataBagCSV provides a collection of methods that can generate/load
 * a CSV representation of a <i>DataBag</i> object.  Use this class when
 * you need to stream a very large table to/from disk in a CSV format.
 * <p>
 * This class utilizes the
 * <a href="http://supercsv.sourceforge.net">SuperCSV</a>
 * framework to manage these transformations.
 * </p>
 *
 * @author Al Cole
 * @since 1.0
 */
public class DataBagCSV
{
    private int mRowId;
    private DataBag mDataBag;
    private FileReader mFileReader;
    private PrintWriter mPrintWriter;
    private CsvListReader mCSVListReader;
    private CsvListWriter mCSVListWriter;
    private boolean mIsFieldNamePreferred;

    /**
     * Constructor to use when loading a CSV file stream.
     *
     */
    public DataBagCSV()
    {
    }

    /**
     * Constructor that identifies a bag prior to a save operation.
     *
     * @param aDataBag Data bag of fields.
     */
    public DataBagCSV(DataBag aDataBag)
    {
        mDataBag = aDataBag;
    }

    /**
     * Return an instance to the internally managed data bag.
     *
     * @return Data bag instance.
     */
    public DataBag getBag()
    {
        return mDataBag;
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

    private int visibleFieldCount(DataBag aBag)
    {
        int totalCount = 0;

        for (DataField dataField : aBag.getFields())
        {
            if (dataField.isFeatureFalse(Field.FEATURE_IS_HIDDEN))
                totalCount++;
        }

        return totalCount;
    }

    private String dataFieldToColumnName(DataField aDataField, boolean anIsTitleOnly)
    {
        if (anIsTitleOnly)
        {
            String fieldTitle = aDataField.getTitle();
            if (StringUtils.isEmpty(fieldTitle))
                fieldTitle = Field.nameToTitle(aDataField.getName());

            return fieldTitle;
        }
        else
        {
            StringBuilder stringBuilder = new StringBuilder(aDataField.getName());

            stringBuilder.append(String.format("[%s]", aDataField.getType().name()));
            String fieldTitle = aDataField.getTitle();
            if (StringUtils.isNotEmpty(fieldTitle))
                stringBuilder.append(String.format("(%s)", fieldTitle));

            return stringBuilder.toString();
        }
    }

    private DataField fieldTypeLabelToDataField(String aFieldTypeLabel, int aColumnOffset)
    {
        DataField dataField;
        Field.Type fieldType = Field.Type.Text;

        if (StringUtils.isNotEmpty(aFieldTypeLabel))
        {
            String fieldName = aFieldTypeLabel;
            String fieldTitle = StringUtils.EMPTY;
            String typeName = Field.Type.Text.name();

            int typeOffsetStart = aFieldTypeLabel.indexOf(StrUtl.CHAR_BRACKET_OPEN);
            int typeOffsetFinish = aFieldTypeLabel.indexOf(StrUtl.CHAR_BRACKET_CLOSE);
            int labelOffsetStart = aFieldTypeLabel.indexOf(StrUtl.CHAR_PAREN_OPEN);
            int labelOffsetFinish = aFieldTypeLabel.indexOf(StrUtl.CHAR_PAREN_CLOSE);

            if ((typeOffsetStart > 0) && (typeOffsetFinish > 0))
            {
                fieldName = aFieldTypeLabel.substring(0, typeOffsetStart);
                typeName = aFieldTypeLabel.substring(typeOffsetStart+1, typeOffsetFinish);
                fieldType = Field.stringToType(typeName);
            }
            if ((labelOffsetStart > 0) && (labelOffsetFinish > 0))
            {
                if (typeOffsetStart == -1)
                    fieldName = aFieldTypeLabel.substring(0, labelOffsetStart);
                fieldTitle = aFieldTypeLabel.substring(labelOffsetStart+1, labelOffsetFinish);
            }

            if (StringUtils.isEmpty(fieldTitle))
                dataField = new DataField(fieldType, fieldName);
            else
                dataField = new DataField(fieldType, fieldName, fieldTitle);
        }
        else
        {
            String fieldName = String.format("field_name_%02d", aColumnOffset);
            dataField = new DataField(fieldType, fieldName);
        }

        return dataField;
    }

    /**
     * Opens the CSV data stream for writing using the PrintWriter stream.
     *
     * @param aPW Print writer output stream.
     *
     * @throws IOException I/O related exception.
     */
    public void open(PrintWriter aPW)
        throws IOException
    {
        close();
        mRowId = 1;
        mCSVListWriter = new CsvListWriter(aPW, CsvPreference.EXCEL_PREFERENCE);
    }

    /**
     * Opens the CSV data stream for writing using the path file name.
     *
     * @param aPathFileName Absolute file name.
     * @throws IOException I/O related exception.
     */
    public void openForWrite(String aPathFileName)
        throws IOException
    {
        close();
        mRowId = 1;
        mPrintWriter = new PrintWriter(aPathFileName, StrUtl.CHARSET_UTF_8);
        mCSVListWriter = new CsvListWriter(mPrintWriter, CsvPreference.EXCEL_PREFERENCE);
    }

    /**
     * Opens the CSV data stream for reading using the Reader input stream.
     *
     * @param aReader File reader input stream.
     *
     * @throws IOException I/O related exception.
     */
    public void open(Reader aReader)
        throws IOException
    {
        close();
        mRowId = 1;
        mCSVListReader = new CsvListReader(aReader, CsvPreference.EXCEL_PREFERENCE);
    }

    /**
     * Opens the CSV data stream for reading using the path file name.
     *
     * @param aPathFileName Absolute file name.
     *
     * @throws IOException I/O related exception.
     */
    public void openForRead(String aPathFileName)
        throws IOException
    {
        close();
        mRowId = 1;
        File csvFile = new File(aPathFileName);
        if (!csvFile.exists())
            throw new IOException(aPathFileName + ": Does not exist.");

        mFileReader = new FileReader(csvFile);
        mCSVListReader = new CsvListReader(mFileReader, CsvPreference.EXCEL_PREFERENCE);
        mDataBag = new DataBag(aPathFileName);
    }

    /**
     * Closes the internally managed CSV I/O streams.
     *
     * @throws IOException I/O related exception.
     */
    public void close()
        throws IOException
    {
        if (mCSVListWriter != null)
        {
            mCSVListWriter.close();
            mCSVListWriter = null;
        }
        if (mPrintWriter != null)
        {
            mPrintWriter.close();
            mPrintWriter = null;
        }

        if (mCSVListReader != null)
        {
            mCSVListReader.close();
            mCSVListReader = null;
        }
        if (mFileReader != null)
        {
            mFileReader.close();
            mFileReader = null;
        }
        mRowId = 0;
    }

    /**
     * Saves a CSV header row based on the previously provided data bag.
     * You must invoke the <code>open()</code> method prior to call this.
     *
     * @param anIsTitleOnly Limit the header a title.
     *
     * @throws IOException I/O related exception.
     */
    public void saveHeader(boolean anIsTitleOnly)
        throws IOException
    {
        if (mCSVListWriter == null)
            throw new IOException("CSV list writer is not open.");

        int colOffset = 0;
        String headerName;
        String[] headerColumns = new String[visibleFieldCount(mDataBag)];
        for (DataField dataField : mDataBag.getFields())
        {
            if (dataField.isFeatureFalse(Field.FEATURE_IS_HIDDEN))
            {
                if (mIsFieldNamePreferred)
                    headerName = dataField.getName();
                else
                    headerName = dataFieldToColumnName(dataField, anIsTitleOnly);
                headerColumns[colOffset++] = headerName;
            }
        }
        mCSVListWriter.writeHeader(headerColumns);
    }

    /**
     * Saves a CSV row based on the data bag fields. You must invoke the
     * <code>open()</code> method prior to call this.
     *
     * @param aBag Data bag of fields.
     * @throws IOException I/O related exception.
     */
    public void save(DataBag aBag)
        throws IOException
    {
        if (mCSVListWriter == null)
            throw new IOException("CSV list writer is not open.");

        mRowId++;
        int colOffset = 0;
        String[] rowCells = new String[visibleFieldCount(aBag)];
        for (DataField dataField : aBag.getFields())
        {
            if (dataField.isFeatureFalse(Field.FEATURE_IS_HIDDEN))
            {
                if (dataField.isFeatureFalse(Field.FEATURE_IS_HIDDEN))
                {
                    if (dataField.isMultiValue())
                        rowCells[colOffset++] = dataField.collapse();
                    else
                        rowCells[colOffset++] = dataField.getValue();
                }
            }
        }
        mCSVListWriter.write(rowCells);
    }

    /**
     * Reads the first row of information from the CSV input stream and
     * captures those fields in the internally managed data bag instance.
     * You must invoke the <code>open()</code> method prior to call this.
     *
     * @throws IOException I/O related exception.
     */
    public void readHeader()
        throws IOException
    {
        DataField dataField;

        if (mCSVListReader == null)
            throw new IOException("CSV list reader is not open.");

        if (mDataBag != null)
            mDataBag = new DataBag(mDataBag.getName());
        else
            mDataBag = new DataBag("Data Bag CSV Header");

        int columnCount = 0;
        String[] columnHeaders = mCSVListReader.getHeader(true);
        for (String columnName : columnHeaders)
        {
            columnCount++;
            dataField = fieldTypeLabelToDataField(columnName, columnCount);
            mDataBag.add(dataField);
        }
    }

    /**
     * Reads the next row of information from the CSV input stream and
     * captures those fields in new data bag instance. You must invoke
     * the <code>open()</code> method prior to call this.
     *
     * @return DataBag Data bag of fields from the row or
     *                 <code>null</code> to signify EOF.
     *
     * @throws IOException I/O related exception.
     */
    public DataBag readRow()
        throws IOException
    {
        if (mCSVListReader == null)
            throw new IOException("CSV list reader is not open.");

        DataBag dbRow = null;
        List<String> rowCells = mCSVListReader.read();
        if (rowCells != null)
        {
            DataField dbField;
            String cellValue, mvDelimiter;
            int cellCount = rowCells.size();
            String rowTitle = String.format("Row %d", mRowId);

            if ((mDataBag != null) && (mDataBag.count() > 0))
            {
                int cellOffset = 0;
                dbRow = new DataBag(mDataBag);
                dbRow.setTitle(rowTitle);
                int adjCount = Math.min(cellCount, mDataBag.count());

                for (DataField dfHeader : mDataBag.getFields())
                {
                    if (cellOffset < adjCount)
                    {
                        cellValue = rowCells.get(cellOffset++);
                        dbField = dbRow.getFieldByName(dfHeader.getName());
                        if (dbField != null)
                        {
                            if (StringUtils.isNotEmpty(cellValue))
                            {
                                if (dfHeader.isMultiValue())
                                {
                                    mvDelimiter = dfHeader.getFeature(Field.FEATURE_MV_DELIMITER);
                                    if (StringUtils.isNotEmpty(mvDelimiter))
                                        dbField.setValues(StrUtl.expandToList(cellValue, mvDelimiter.charAt(0)));
                                    else
                                        dbField.setValues(StrUtl.expandToList(cellValue, StrUtl.CHAR_PIPE));
                                }
                                else
                                    dbField.setValue(cellValue);
                            }
                        }
                    }
                    else
                        break;
                }
            }
            else
            {
                String fieldTitle;
                dbRow = new DataBag(rowTitle, rowTitle);
                for (int cellOffset = 0; cellOffset < cellCount; cellOffset++)
                {
                    cellValue = rowCells.get(cellOffset);
                    fieldTitle = String.format("Field Name [%d,%d]", mRowId, cellOffset+1);
                    dbField = new DataTextField(String.format("field_name_%d", cellOffset+1), fieldTitle);
                    if (StringUtils.isNotEmpty(cellValue))
                        dbField.setValue(cellValue);
                    dbRow.add(dbField);
                }
            }
            mRowId++;
        }

        return dbRow;
    }
}
