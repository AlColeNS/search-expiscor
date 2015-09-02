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
import com.nridge.core.base.field.FieldRange;
import com.nridge.core.base.io.IO;
import com.nridge.core.base.std.StrUtl;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.Date;

/**
 * The RangeJSON provides a collection of methods that can generate/load
 * a JSON representation of a <i>FieldRange</i> object.
 * <p>
 * This class utilizes the
 * <a href="https://code.google.com/p/google-gson/">Gson</a>
 * framework to manage these transformations.
 * </p>
 *
 * @author Al Cole
 * @since 1.0
 */
public class RangeJSON
{
    /**
     * Default constructor.
     */
    public RangeJSON()
    {
    }

    /**
     * Saves the data field to the writer stream specified as a parameter.
     *
     * @param aWriter Json writer stream instance.
     * @param aFieldRange Field range instance.
     *
     * @throws IOException I/O related exception.
     */
    public void save(JsonWriter aWriter, FieldRange aFieldRange)
        throws IOException
    {
        aWriter.name(IO.JSON_RANGE_OBJECT_NAME).beginObject();

        IOJSON.writeNameValue(aWriter, IO.JSON_TYPE_MEMBER_NAME, Field.typeToString(aFieldRange.getType()));
        if (aFieldRange.getType() == Field.Type.Text)
        {
            IOJSON.writeNameValue(aWriter, IO.JSON_DELIMITER_MEMBER_NAME, Character.toString(aFieldRange.getDelimiterChar()));
            String singleString = StrUtl.collapseToSingle(aFieldRange.getItems(), aFieldRange.getDelimiterChar());
            aWriter.name(IO.JSON_VALUE_MEMBER_NAME).value(singleString);
        }
        else
        {
            switch (aFieldRange.getType())
            {
                case Long:
                    IOJSON.writeNameValue(aWriter, "min", aFieldRange.getMinLong());
                    IOJSON.writeNameValue(aWriter, "max", aFieldRange.getMaxLong());
                    break;
                case Integer:
                    IOJSON.writeNameValue(aWriter, "min", aFieldRange.getMinInteger());
                    IOJSON.writeNameValue(aWriter, "max", aFieldRange.getMaxInteger());
                    break;
                case Double:
                    IOJSON.writeNameValue(aWriter, "min", aFieldRange.getMinDouble());
                    IOJSON.writeNameValue(aWriter, "max", aFieldRange.getMaxDouble());
                    break;
                case DateTime:
                    IOJSON.writeNameValue(aWriter, "min", aFieldRange.getMinString());
                    IOJSON.writeNameValue(aWriter, "max", aFieldRange.getMaxString());
                    break;
            }
        }

        aWriter.endObject();
    }

    /**
     * Parses an JSON stream and loads it into a field range.
     *
     * @param aReader Json reader stream instance.
     *
     * @throws java.io.IOException I/O related exception.
     */
    public FieldRange load(JsonReader aReader)
        throws IOException
    {
        String jsonName;

        boolean isFirst = true;
        Date firstDate = new Date();
        long firstLong = Long.MIN_VALUE;
        int firstInt = Integer.MIN_VALUE;
        double firstDouble = Double.MIN_VALUE;

        Field.Type rangeType = Field.Type.Text;
        FieldRange fieldRange = new FieldRange();

        aReader.beginObject();
        while (aReader.hasNext())
        {
            jsonName = aReader.nextName();
            if (StringUtils.equals(jsonName, IO.JSON_TYPE_MEMBER_NAME))
                rangeType = Field.stringToType(aReader.nextString());
            else if (StringUtils.equals(jsonName, IO.JSON_DELIMITER_MEMBER_NAME))
                fieldRange.setDelimiterChar(aReader.nextString());
            else if (StringUtils.equals(jsonName, IO.JSON_VALUE_MEMBER_NAME))
                fieldRange.setItems(StrUtl.expandToList(aReader.nextString(), fieldRange.getDelimiterChar()));
            else if (StringUtils.equals(jsonName, "min"))
            {
                switch (rangeType)
                {
                    case Long:
                        if (isFirst)
                        {
                            isFirst = false;
                            firstLong = aReader.nextLong();
                        }
                        else
                            fieldRange = new FieldRange(aReader.nextLong(), firstLong);
                        break;
                    case Integer:
                        if (isFirst)
                        {
                            isFirst = false;
                            firstInt = aReader.nextInt();
                        }
                        else
                            fieldRange = new FieldRange(aReader.nextInt(), firstInt);
                        break;
                    case Double:
                        if (isFirst)
                        {
                            isFirst = false;
                            firstDouble = aReader.nextDouble();
                        }
                        else
                            fieldRange = new FieldRange(aReader.nextDouble(), firstDouble);
                        break;
                    case DateTime:
                        if (isFirst)
                        {
                            isFirst = false;
                            firstDate = Field.createDate(aReader.nextString());
                        }
                        else
                            fieldRange = new FieldRange(Field.createDate(aReader.nextString()), firstDate);
                        break;
                    default:
                        aReader.skipValue();
                        break;
                }
            }
            else if (StringUtils.equals(jsonName, "max"))
            {
                switch (rangeType)
                {
                    case Long:
                        if (isFirst)
                        {
                            isFirst = false;
                            firstLong = aReader.nextLong();
                        }
                        else
                            fieldRange = new FieldRange(firstLong, aReader.nextLong());
                        break;
                    case Integer:
                        if (isFirst)
                        {
                            isFirst = false;
                            firstInt = aReader.nextInt();
                        }
                        else
                            fieldRange = new FieldRange(firstInt, aReader.nextInt());
                        break;
                    case Double:
                        if (isFirst)
                        {
                            isFirst = false;
                            firstDouble = aReader.nextDouble();
                        }
                        else
                            fieldRange = new FieldRange(firstDouble, aReader.nextDouble());
                        break;
                    case DateTime:
                        if (isFirst)
                        {
                            isFirst = false;
                            firstDate = Field.createDate(aReader.nextString());
                        }
                        else
                            fieldRange = new FieldRange(firstDate, Field.createDate(aReader.nextString()));
                        break;
                    default:
                        aReader.skipValue();
                        break;
                }
            }
            else
                aReader.skipValue();
        }

        aReader.endObject();

        return fieldRange;
    }
}
