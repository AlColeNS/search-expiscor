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
import com.nridge.core.base.ds.DSCriteria;
import com.nridge.core.base.ds.DSCriterion;
import com.nridge.core.base.ds.DSCriterionEntry;
import com.nridge.core.base.field.Field;
import com.nridge.core.base.field.data.DataField;
import com.nridge.core.base.io.IO;
import com.nridge.core.base.io.xml.DataFieldXML;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.util.ArrayList;

/**
 * The DataBagJSON provides a collection of methods that can generate/load
 * a JSON representation of a <i>DSCriteria</i> object.
 * <p>
 * This class utilizes the
 * <a href="https://code.google.com/p/google-gson/">Gson</a>
 * framework to manage these transformations.
 * </p>
 *
 * @author Al Cole
 * @since 1.0
 */
public class DSCriteriaJSON
{
    private DSCriteria mDSCriteria;
    private DataFieldJSON mDataFieldJSON;

    /**
     * Default constructor.
     */
    public DSCriteriaJSON()
    {
        mDSCriteria = new DSCriteria();
        mDataFieldJSON = new DataFieldJSON();
    }

    /**
     * Constructor that identifies a criteria prior to a save operation.
     *
     * @param aCriteria Data source criteria.
     */
    public DSCriteriaJSON(DSCriteria aCriteria)
    {
        mDSCriteria = aCriteria;
        mDataFieldJSON = new DataFieldJSON();
    }

    /**
     * Returns a reference to the {@link DSCriteria} being managed by
     * this class.
     *
     * @return Data source criteria.
     */
    public DSCriteria getCriteria()
    {
        return mDSCriteria;
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
        DataField dataField;
        DSCriterion dsCriterion;

        ArrayList<DSCriterionEntry> dsCriterionEntries = mDSCriteria.getCriterionEntries();
        int ceCount = dsCriterionEntries.size();

        if (anIsValueObject)
            aWriter.name(IO.JSON_CRITERIA_OBJECT_NAME).beginObject();
        else
            aWriter.beginObject();

        IOJSON.writeNameValue(aWriter, IO.JSON_NAME_MEMBER_NAME, mDSCriteria.getName());
        IOJSON.writeNameValue(aWriter, IO.JSON_VERSION_MEMBER_NAME, IO.CRITERIA_JSON_FORMAT_VERSION);
        IOJSON.writeNameValue(aWriter, IO.JSON_OPERATOR_MEMBER_NAME, Field.operatorToString(Field.Operator.AND));
        IOJSON.writeNameValue(aWriter, IO.JSON_FEATURES_ARRAY_NAME, mDSCriteria.getFeatures());
        IOJSON.writeNameValue(aWriter, IO.JSON_COUNT_MEMBER_NAME, ceCount);

        if (ceCount > 0)
        {
            aWriter.name(IO.JSON_FIELDS_ARRAY_NAME).beginArray();
            for (DSCriterionEntry ce : dsCriterionEntries)
            {
                dsCriterion = ce.getCriterion();

                dataField = new DataField(dsCriterion.getField());
                dataField.addFeature(IO.JSON_OPERATOR_MEMBER_NAME, Field.operatorToString(ce.getLogicalOperator()));
                mDataFieldJSON.save(aWriter, dataField, false);
            }
            aWriter.endArray();
        }

        aWriter.endObject();
    }

    /**
     * Saves the previous assigned criteria (e.g. via constructor or set method)
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
     * Saves the previous assigned criteria (e.g. via constructor or set method)
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
     * Saves the previous assigned criteria (e.g. via constructor or set method)
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
        String jsonName, jsonValue, logicalOperator;

        mDSCriteria.reset();

        aReader.beginObject();
        while (aReader.hasNext())
        {
            jsonName = aReader.nextName();
            if (StringUtils.equals(jsonName, IO.JSON_NAME_MEMBER_NAME))
                mDSCriteria.setName(aReader.nextString());
            else if (StringUtils.equals(jsonName, IO.JSON_FEATURES_ARRAY_NAME))
            {
                aReader.beginObject();
                while (aReader.hasNext())
                {
                    jsonName = aReader.nextName();
                    jsonValue = aReader.nextString();
                    mDSCriteria.addFeature(jsonName, jsonValue);
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
                    {
                        logicalOperator = dataField.getFeature(IO.JSON_OPERATOR_MEMBER_NAME);
                        if (StringUtils.isEmpty(logicalOperator))
                            logicalOperator = Field.operatorToString(Field.Operator.EQUAL);
                        mDSCriteria.add(dataField, Field.stringToOperator(logicalOperator));
                    }
                }
                aReader.endArray();
            }
            else
                aReader.skipValue();
        }
        aReader.endObject();
    }

    /**
     * Parses an JSON stream and loads it into a criteria.
     *
     * @param aReader Reader stream instance.
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
     * Parses an input stream and loads it into a criteria.
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
     * Parses a JSON formatted path/file name and loads it into a criteria.
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
