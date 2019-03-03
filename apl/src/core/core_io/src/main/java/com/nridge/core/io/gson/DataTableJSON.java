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

package com.nridge.core.io.gson;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import com.nridge.core.base.field.Field;
import com.nridge.core.base.field.FieldRow;
import com.nridge.core.base.field.data.DataField;
import com.nridge.core.base.field.data.DataTable;
import com.nridge.core.base.io.IO;
import com.nridge.core.base.std.StrUtl;
import org.apache.commons.lang3.StringUtils;

import java.io.*;

/**
 * The DataTableJSON provides a collection of methods that can generate/load
 * a JSON representation of a <i>DataTable</i> object.
 * <p>
 * This class utilizes the
 * <a href="https://code.google.com/p/google-gson/">Gson</a>
 * framework to manage these transformations.
 * </p>
 *
 * @author Al Cole
 * @since 1.0
 */
public class DataTableJSON
{
    private int mContextTotal;
    private int mContextStart;
    private int mContextLimit;
    private DataTable mDataTable;

    /**
     * Default constructor.
     */
    public DataTableJSON()
    {
        mDataTable = new DataTable();
    }

    /**
     * Constructor accepts a table as a parameter.
     *
     * @param aDataTable Data table instance.
     */
    public DataTableJSON(DataTable aDataTable)
    {
        mDataTable = aDataTable;
    }

    /**
     * Constructor accepts a table as a parameter.
     *
     * @param aDataTable Data table instance.
     * @param aStart Context starting offset.
     * @param aLimit Context limit.
     * @param aTotal Context total.
     */
    public DataTableJSON(DataTable aDataTable, int aStart, int aLimit, int aTotal)
    {
        mContextStart = aStart;
        mContextLimit = aLimit;
        mContextTotal = aTotal;
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
     * Returns the context total value.
     *
     * @return Context total.
     */
    public int getContextTotal()
    {
        return mContextTotal;
    }

    /**
     * Returns the context starting value.
     *
     * @return Context starting offset.
     */
    public int getContextStart()
    {
        return mContextStart;
    }

    /**
     * Return the context limit value.
     *
     * @return Context limit value.
     */
    public int getContextLimit()
    {
        return mContextLimit;
    }

    /**
     * Resets the context values to zero.
     */
    public void resetContext()
    {
        mContextStart = 0;
        mContextLimit = 0;
        mContextTotal = 0;
    }

    /**
     * Saves the previous assigned bag/table (e.g. via constructor or set method)
     * to the stream specified as a parameter.
     *
     * @param aWriter Json writer stream instance.
     * @param anIsValueObject If true, then a value object is saved.
     *
     * @throws java.io.IOException I/O related exception.
     */
    public void save(JsonWriter aWriter, boolean anIsValueObject)
        throws IOException
    {
        String cellValue;
        DataField dataField;

        int rowCount = mDataTable.rowCount();
        int columnCount = mDataTable.columnCount();

        if (anIsValueObject)
            aWriter.name(IO.JSON_TABLE_OBJECT_NAME).beginObject();
        else
            aWriter.beginObject();

        IOJSON.writeNameValue(aWriter, IO.JSON_NAME_MEMBER_NAME, mDataTable.getName());
        IOJSON.writeNameValue(aWriter, IO.JSON_VERSION_MEMBER_NAME, IO.DATATABLE_JSON_FORMAT_VERSION);
        IOJSON.writeNameValue(aWriter, IO.JSON_DIMENSIONS_MEMBER_NAME, String.format("%d cols x %d rows", columnCount, rowCount));
        if ((mContextTotal != 0) || (mContextLimit != 0))
        {
            aWriter.name(IO.JSON_CONTEXT_OBJECT_NAME).beginObject();
            IOJSON.writeNameValue(aWriter, IO.JSON_START_MEMBER_NAME, mContextStart);
            IOJSON.writeNameValueNonZero(aWriter, IO.JSON_LIMIT_MEMBER_NAME, mContextLimit);
            IOJSON.writeNameValueNonZero(aWriter, IO.JSON_TOTAL_MEMBER_NAME, mContextTotal);
            aWriter.endObject();
        }

        DataBagJSON dataBagJSON = new DataBagJSON(mDataTable.getColumnBag());
        dataBagJSON.save(aWriter, true);

        if (rowCount > 0)
        {
            aWriter.name(IO.JSON_ROWS_ARRAY_NAME).beginArray();
            for (int row = 0; row < rowCount; row++)
            {
                aWriter.beginObject();
                for (int col = 0; col < columnCount; col++)
                {
                    dataField = mDataTable.getFieldByRowCol(row, col);
                    cellValue = dataField.collapse();
                    aWriter.name(IO.JSON_CELL_MEMBER_NAME).value(cellValue);
                }
                aWriter.endObject();
            }
            aWriter.endArray();
        }

        aWriter.endObject();
    }

    /**
     * Saves the previous assigned bag/table (e.g. via constructor or set method)
     * to the writer stream specified as a parameter.
     *
     * @param aWriter Json writer stream instance.
     *
     * @throws java.io.IOException I/O related exception.
     */
    public void save(JsonWriter aWriter)
        throws IOException
    {
        save(aWriter, false);
    }

    /**
     * Saves the previous assigned bag/table (e.g. via constructor or set method)
     * to the writer stream specified as a parameter.
     *
     * @param anOS Output stream instance.
     *
     * @throws java.io.IOException I/O related exception.
     */
    public void save(OutputStream anOS)
        throws IOException
    {
        OutputStreamWriter outputStreamWriter = new OutputStreamWriter(anOS);
        JsonWriter jsonWriter = new JsonWriter(outputStreamWriter);
        save(jsonWriter, false);
    }

    /**
     * Saves the previous assigned bag/table (e.g. via constructor or set method)
     * to the writer stream specified as a parameter.
     *
     * @param aPathFileName Output path file name.
     *
     * @throws java.io.IOException I/O related exception.
     */
    public void save(String aPathFileName)
        throws IOException
    {
        try (FileOutputStream fileOutputStream = new FileOutputStream(aPathFileName))
        {
            save(fileOutputStream);
        }
    }

    /**
     * Parses an JSON stream and loads it into a bag/table.
     *
     * @param aReader Json reader stream instance.
     *
     * @throws java.io.IOException I/O related exception.
     */
    public void load(JsonReader aReader)
        throws IOException
    {
        int columnOffset;
        FieldRow fieldRow;
        DataField dataField;
        String jsonName, jsonValue, mvDelimiter;

        resetContext();
        aReader.beginObject();
        while (aReader.hasNext())
        {
            jsonName = aReader.nextName();
            if (StringUtils.equals(jsonName, IO.JSON_NAME_MEMBER_NAME))
                mDataTable.setName(aReader.nextString());
            else if (StringUtils.equals(jsonName, IO.JSON_CONTEXT_OBJECT_NAME))
            {
                aReader.beginObject();
                while (aReader.hasNext())
                {
                    jsonName = aReader.nextName();
                    if (StringUtils.equals(jsonName, IO.JSON_START_MEMBER_NAME))
                        mContextStart = aReader.nextInt();
                    else if (StringUtils.equals(jsonName, IO.JSON_LIMIT_MEMBER_NAME))
                        mContextLimit = aReader.nextInt();
                    else if (StringUtils.equals(jsonName, IO.JSON_TOTAL_MEMBER_NAME))
                        mContextTotal = aReader.nextInt();
                    else
                        aReader.skipValue();
                }
                aReader.endObject();
            }
            else if (StringUtils.equals(jsonName, IO.JSON_BAG_OBJECT_NAME))
            {
                DataBagJSON dataBagJSON = new DataBagJSON();
                dataBagJSON.load(aReader);
                mDataTable.setColumns(dataBagJSON.getBag());
            }
            else if (StringUtils.equals(jsonName, IO.JSON_ROWS_ARRAY_NAME))
            {
                aReader.beginArray();
                while (aReader.hasNext())
                {
                    columnOffset = 0;
                    fieldRow = mDataTable.newRow();

                    aReader.beginObject();
                    while (aReader.hasNext())
                    {
                        jsonName = aReader.nextName();
                        if (StringUtils.equals(jsonName, IO.JSON_CELL_MEMBER_NAME))
                        {
                            jsonValue = aReader.nextString();
                            dataField = mDataTable.getColumn(columnOffset);
                            if (dataField != null)
                            {
                                if (dataField.isMultiValue())
                                {
                                    mvDelimiter = dataField.getFeature(Field.FEATURE_MV_DELIMITER);
                                    if (StringUtils.isNotEmpty(mvDelimiter))
                                        fieldRow.setValues(columnOffset, StrUtl.expandToList(jsonValue, mvDelimiter.charAt(0)));
                                    else
                                        fieldRow.setValues(columnOffset, StrUtl.expandToList(jsonValue, StrUtl.CHAR_PIPE));
                                }
                                else
                                    fieldRow.setValue(columnOffset, jsonValue);
                                columnOffset++;
                            }
                        }
                        else
                            aReader.skipValue();
                    }
                    aReader.endObject();

                    mDataTable.addRow(fieldRow);

                }
                aReader.endArray();
            }
            else
                aReader.skipValue();
        }
        aReader.endObject();
    }

    /**
     * Parses an input stream and loads it into a bag/table.
     *
     * @param anIS Input stream instance.
     *
     * @throws java.io.IOException I/O related exception.
     */
    public void load(InputStream anIS)
        throws IOException
    {
        InputStreamReader inputStreamReader = new InputStreamReader(anIS);
        JsonReader jsonReader = new JsonReader(inputStreamReader);
        load(jsonReader);
    }

    /**
     * Parses a JSON formatted path/file name and loads it into a bag/table.
     *
     * @param aPathFileName Input path file name.
     *
     * @throws java.io.IOException I/O related exception.
     */
    public void load(String aPathFileName)
        throws IOException
    {
        File jsonFile = new File(aPathFileName);
        if (! jsonFile.exists())
            throw new IOException(aPathFileName + ": Does not exist.");

        try (FileInputStream fileInputStream = new FileInputStream(jsonFile))
        {
            load(fileInputStream);
        }
    }
}
