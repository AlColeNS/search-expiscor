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

package com.nridge.core.io.log;

import com.nridge.core.base.field.Field;
import com.nridge.core.base.field.data.DataField;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.util.Map;

/**
 * The DataFieldLogger class provides logger helper methods.
 */
public class DataFieldLogger
{
    protected Logger mLogger;

    public DataFieldLogger(Logger aLogger)
    {
        mLogger = aLogger;
    }

    public void writeNV(String aName, String aValue)
    {
        if (StringUtils.isNotEmpty(aValue))
            mLogger.debug(aName + ": " + aValue);
    }

    public void writeNV(String aName, int aValue)
    {
        if (aValue > 0)
            writeNV(aName, Integer.toString(aValue));
    }

    public void writeNV(String aName, long aValue)
    {
        if (aValue > 0)
            writeNV(aName, Long.toString(aValue));
    }

    public void writeFull(DataField aField)
    {
        if (aField != null)
        {
            writeNV("Name", aField.getName());
            writeNV("Type", Field.typeToString(aField.getType()));
            writeNV("Title", aField.getTitle());
            writeNV("Value", aField.collapse());
            if (aField.getSortOrder() != Field.Order.UNDEFINED)
                writeNV("Sort Order", aField.getSortOrder().name());
            writeNV("Display Size", aField.getDisplaySize());
            writeNV("Default Value", aField.getDefaultValue());

            String nameString;
            int featureOffset = 0;
            for (Map.Entry<String, String> featureEntry : aField.getFeatures().entrySet())
            {
                nameString = String.format(" F[%02d] %s", featureOffset++, featureEntry.getKey());
                writeNV(nameString, featureEntry.getValue());
            }
        }
    }

    public void writeSimple(DataField aField)
    {
        if (aField != null)
            mLogger.debug(aField.toString());
    }

    public void write(DataField aField)
    {
        writeFull(aField);
    }
}
