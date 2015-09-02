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

import com.nridge.core.base.field.data.DataBag;
import com.nridge.core.base.field.data.DataField;
import org.slf4j.Logger;

import java.util.Map;

/**
 * The DataBagLogger class provides logger helper methods.
 */
public class DataBagLogger
{
    private Logger mLogger;
    private DataFieldLogger mDataFieldLogger;

    public DataBagLogger(Logger aLogger)
    {
        mLogger = aLogger;
        mDataFieldLogger = new DataFieldLogger(aLogger);
    }

    public void writeFull(DataBag aBag)
    {
        if (aBag != null)
        {
            mDataFieldLogger.writeNV("Name", aBag.getName());
            mDataFieldLogger.writeNV("Title", aBag.getTitle());

            String nameString;
            int featureOffset = 0;
            for (Map.Entry<String, String> featureEntry : aBag.getFeatures().entrySet())
            {
                nameString = String.format(" F[%02d] %s", featureOffset++, featureEntry.getKey());
                mDataFieldLogger.writeNV(nameString, featureEntry.getValue());
            }
            for (DataField dataField : aBag.getFields())
                mDataFieldLogger.writeSimple(dataField);
            PropertyLogger propertyLogger = new PropertyLogger(mLogger);
            propertyLogger.writeFull(aBag.getProperties());
        }
    }

    public void writeSimple(DataBag aBag)
    {
        if (aBag != null)
        {
            mDataFieldLogger.writeNV("Name", aBag.getName());
            mDataFieldLogger.writeNV("Title", aBag.getTitle());
            for (DataField dataField : aBag.getFields())
                mDataFieldLogger.writeSimple(dataField);
        }
    }

    public void write(DataBag aBag)
    {
        writeFull(aBag);
    }
}
