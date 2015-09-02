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

import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * The DataBagLogger class provides logger helper methods.
 */
public class PropertyLogger
{
    private Logger mLogger;
    private DataFieldLogger mDataFieldLogger;

    public PropertyLogger(Logger aLogger)
    {
        mLogger = aLogger;
        mDataFieldLogger = new DataFieldLogger(aLogger);
    }

    public void writeSimple(HashMap<String, Object> aProperties)
    {
        if (aProperties != null)
        {
            for (Map.Entry<String, Object> propertyEntry : aProperties.entrySet())
                mDataFieldLogger.writeNV(propertyEntry.getKey(), propertyEntry.getValue().toString());
        }
    }

    public void writeFull(HashMap<String, Object> aProperties)
    {
        writeSimple(aProperties);
    }

    public void write(HashMap<String, Object> aProperties)
    {
        writeSimple(aProperties);
    }
}
