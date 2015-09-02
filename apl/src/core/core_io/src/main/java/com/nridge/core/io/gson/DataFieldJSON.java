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

package com.nridge.core.io.gson;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import com.nridge.core.base.field.Field;
import com.nridge.core.base.field.data.DataField;
import com.nridge.core.base.io.IO;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.Map;

/**
 * The DataFieldJSON class provides JSON helper methods.
 */
public class DataFieldJSON
{
    private RangeJSON mRangeJSON;

    /**
     * Default constructor.
     */
    public DataFieldJSON()
    {
        mRangeJSON = new RangeJSON();
    }

    /**
     * Saves the data field to the writer stream specified as a parameter.
     *
     * @param aWriter Json writer stream instance.
     * @param aDataField Data field instance.
     * @param anIsValueObject If true, then a value object is saved.
     *
     * @throws IOException I/O related exception.
     */
    public void save(JsonWriter aWriter, DataField aDataField, boolean anIsValueObject)
        throws IOException
    {
        String singleValue;

        if (anIsValueObject)
            aWriter.name("field").beginObject();
        else
            aWriter.beginObject();

        IOJSON.writeNameValue(aWriter, IO.JSON_NAME_MEMBER_NAME, aDataField.getName());
        IOJSON.writeNameValue(aWriter, IO.JSON_TYPE_MEMBER_NAME, Field.typeToString(aDataField.getType()));
        IOJSON.writeNameValue(aWriter, IO.JSON_TITLE_MEMBER_NAME, aDataField.getTitle());
        IOJSON.writeNameValueNonZero(aWriter, "displaySize", aDataField.getDisplaySize());
        IOJSON.writeNameValue(aWriter, "defaultValue", aDataField.getDefaultValue());
        if (aDataField.getSortOrder() != Field.Order.UNDEFINED)
            aWriter.name("sortOrder").value(aDataField.getSortOrder().name());
        if (aDataField.isRangeAssigned())
            mRangeJSON.save(aWriter, aDataField.getRange());
        IOJSON.writeNameValue(aWriter, IO.JSON_FEATURES_ARRAY_NAME, aDataField.getFeatures());
        if (aDataField.isMultiValue())
        {
            aWriter.name("isMultiValue").value(aDataField.isMultiValue());
            String mvDelimiter = aDataField.getFeature(Field.FEATURE_MV_DELIMITER);
            if (StringUtils.isNotEmpty(mvDelimiter))
                singleValue = aDataField.collapse(mvDelimiter.charAt(0));
            else
                singleValue = aDataField.collapse();
        }
        else
            singleValue = aDataField.getValue();
        IOJSON.writeNameValue(aWriter, IO.JSON_VALUE_MEMBER_NAME, singleValue);

        aWriter.endObject();
    }

    private void assignFieldNameValue(DataField aField, String aName, JsonReader aReader)
        throws IOException
    {
        if (StringUtils.equals(aName, IO.JSON_TITLE_MEMBER_NAME))
            aField.setTitle(aReader.nextString());
        else if (StringUtils.equals(aName, "displaySize"))
            aField.setDisplaySize(aReader.nextInt());
        else if (StringUtils.equals(aName, "defaultValue"))
            aField.setDefaultValue(aReader.nextString());
        else if (StringUtils.equals(aName, "sortOrder"))
            aField.setSortOrder(Field.Order.valueOf(aReader.nextString()));
        else if (StringUtils.equals(aName, "isMultiValue"))
            aField.setMultiValueFlag(aReader.nextBoolean());
        else if (StringUtils.equals(aName, IO.JSON_RANGE_OBJECT_NAME))
            aField.setRange(mRangeJSON.load(aReader));
        else if (StringUtils.equals(aName, IO.JSON_FEATURES_ARRAY_NAME))
        {
            String jsonName, jsonValue;

            aReader.beginObject();
            while (aReader.hasNext())
            {
                jsonName = aReader.nextName();
                jsonValue = aReader.nextString();
                aField.addFeature(jsonName, jsonValue);
            }
            aReader.endObject();
        }
        else if (StringUtils.equals(aName, IO.JSON_VALUE_MEMBER_NAME))
        {
            String jsonValue = aReader.nextString();
            if (aField.isMultiValue())
            {
                String mvDelimiter = aField.getFeature(Field.FEATURE_MV_DELIMITER);
                if (StringUtils.isNotEmpty(mvDelimiter))
                    aField.expand(jsonValue, mvDelimiter.charAt(0));
                else
                    aField.expand(jsonValue);
            }
            else
                aField.setValue(jsonValue);
        }
    }

    /**
     * Parses aJSON formatted input reader stream and loads it into a field.
     *
     * @param aReader Input reader stream instance.
     *
     * @return DataField instance.
     *
     * @throws java.io.IOException I/O related exception.
     */
    public DataField load(JsonReader aReader)
        throws IOException
    {
        String jsonName;

        DataField dataField = null;
        String fieldName = StringUtils.EMPTY;
        String fieldType = Field.typeToString(Field.Type.Text);

        aReader.beginObject();
        while (aReader.hasNext())
        {
            jsonName = aReader.nextName();
            if (StringUtils.equals(jsonName, IO.JSON_NAME_MEMBER_NAME))
                fieldName = aReader.nextString();
            else if (StringUtils.equals(jsonName, IO.JSON_TYPE_MEMBER_NAME))
                fieldType = aReader.nextString();
            else
            {
                if (dataField == null)
                {
                    if ((StringUtils.isNotEmpty(fieldName)) && (StringUtils.isNotEmpty(fieldType)))
                        dataField = new DataField(Field.stringToType(fieldType), fieldName);
                    else
                        throw new IOException("JSON Parser: Data field is missing name/type field.");
                }
                assignFieldNameValue(dataField, jsonName, aReader);
            }
        }
        aReader.endObject();

// OK - if we get here and the data field is null, then the JSON lacked a value field.

        if (dataField == null)
        {
            if ((StringUtils.isNotEmpty(fieldName)) && (StringUtils.isNotEmpty(fieldType)))
                dataField = new DataField(Field.stringToType(fieldType), fieldName);
            else
                throw new IOException("JSON Parser: Data field is missing name/type field.");
        }

        return dataField;
    }
}
