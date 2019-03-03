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
import com.nridge.core.base.field.data.DataBag;
import com.nridge.core.base.field.data.DataField;
import com.nridge.core.base.io.IO;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.util.Map;

/**
 * The DataBagJSON provides a collection of methods that can generate/load
 * a JSON representation of a <i>DataBag</i> object.
 * <p>
 * This class utilizes the
 * <a href="https://code.google.com/p/google-gson/">Gson</a>
 * framework to manage these transformations.
 * </p>
 *
 * @author Al Cole
 * @since 1.0
 */
public class DataBagJSON
{
    private DataBag mBag;
    private DataFieldJSON mDataFieldJSON;

    /**
     * Default constructor.
     */
    public DataBagJSON()
    {
        mBag = new DataBag();
        mDataFieldJSON = new DataFieldJSON();
    }

    /**
     * Constructor accepts a bag as a parameter.
     *
     * @param aBag Bag instance.
     */
    public DataBagJSON(DataBag aBag)
    {
        setBag(aBag);
        mDataFieldJSON = new DataFieldJSON();
    }

    /**
     * Assigns the bag parameter to the internally managed bag instance.
     *
     * @param aBag Bag instance.
     */
    public void setBag(DataBag aBag)
    {
        mBag = aBag;
    }

    /**
     * Returns a reference to the internally managed bag instance.
     *
     * @return Bag instance.
     */
    public DataBag getBag()
    {
        return mBag;
    }

    /**
     * Saves the previous assigned bag/table (e.g. via constructor or set method)
     * to the writer stream specified as a parameter.
     *
     * @param aWriter Json writer stream instance.
     * @param anIsValueObject If true, then a value object is saved.
     *
     * @throws java.io.IOException I/O related exception.
     */
    public void save(JsonWriter aWriter, boolean anIsValueObject)
        throws IOException
    {
        if (anIsValueObject)
            aWriter.name("bag").beginObject();
        else
            aWriter.beginObject();

        IOJSON.writeNameValue(aWriter, IO.JSON_NAME_MEMBER_NAME, mBag.getName());
        IOJSON.writeNameValue(aWriter, IO.JSON_VERSION_MEMBER_NAME, IO.DATABAG_JSON_FORMAT_VERSION);
        IOJSON.writeNameValue(aWriter, IO.JSON_TITLE_MEMBER_NAME, mBag.getTitle());
        IOJSON.writeNameValue(aWriter, IO.JSON_FEATURES_ARRAY_NAME, mBag.getFeatures());

        aWriter.name("fields").beginArray();
        for (DataField dataField : mBag.getFields())
            mDataFieldJSON.save(aWriter, dataField, false);
        aWriter.endArray();

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
    public void save(Writer aWriter)
        throws IOException
    {
        JsonWriter jsonWriter = new JsonWriter(aWriter);
        save(jsonWriter, false);
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
        DataField dataField;
        String jsonName, jsonValue;

        aReader.beginObject();
        while (aReader.hasNext())
        {
            jsonName = aReader.nextName();
            if (StringUtils.equals(jsonName, IO.JSON_NAME_MEMBER_NAME))
                mBag.setName(aReader.nextString());
            else if (StringUtils.equals(jsonName, IO.JSON_TITLE_MEMBER_NAME))
                mBag.setTitle(aReader.nextString());
            else if (StringUtils.equals(jsonName, IO.JSON_FEATURES_ARRAY_NAME))
            {
                aReader.beginObject();
                while (aReader.hasNext())
                {
                    jsonName = aReader.nextName();
                    jsonValue = aReader.nextString();
                    mBag.addFeature(jsonName, jsonValue);
                }
                aReader.endObject();
            }
            else if (StringUtils.equals(jsonName, IO.JSON_FIELDS_ARRAY_NAME))
            {
                aReader.beginArray();
                while (aReader.hasNext())
                {
                    dataField = mDataFieldJSON.load(aReader);
                    if (dataField != null)
                        mBag.add(dataField);
                }
                aReader.endArray();
            }
            else
                aReader.skipValue();
        }

        aReader.endObject();
    }

    /**
     * Parses aJSON formatted input reader stream and loads it into a bag/table.
     *
     * @param aReader Input reader stream instance.
     *
     * @throws java.io.IOException I/O related exception.
     */
    public void load(Reader aReader)
        throws IOException
    {
        JsonReader jsonReader = new JsonReader(aReader);
        load(jsonReader);
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