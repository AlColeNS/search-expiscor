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

import com.nridge.core.base.ds.DSCriteria;
import com.nridge.core.base.ds.DSCriterion;
import com.nridge.core.base.ds.DSCriterionEntry;
import com.nridge.core.base.field.Field;
import com.nridge.core.base.field.data.DataField;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Map;

/**
 * The DataBagLogger class provides logger helper methods.
 */
public class DSCriteriaLogger
{
    private String mPrefix;
    private Logger mLogger;
    private DataFieldLogger mDataFieldLogger;

    public DSCriteriaLogger(Logger aLogger)
    {
        mLogger = aLogger;
        mPrefix = StringUtils.EMPTY;
        mDataFieldLogger = new DataFieldLogger(aLogger);
    }

    public void setPrefix(String aPrefix)
    {
        if (aPrefix != null)
            mPrefix = String.format("[%s] ", aPrefix);
    }

    public void writeFull(DSCriteria aCriteria)
    {
        if (aCriteria != null)
        {
            int ceIndex = 1;
            DataField dataField;
            DSCriterion dsCriterion;
            String nameString, logString;

            mDataFieldLogger.writeNV(mPrefix + "Name", aCriteria.getName());
            int featureOffset = 0;
            for (Map.Entry<String, String> featureEntry : aCriteria.getFeatures().entrySet())
            {
                nameString = String.format("%s F(%02d) %s", mPrefix, featureOffset++, featureEntry.getKey());
                mDataFieldLogger.writeNV(nameString, featureEntry.getValue());
            }
            ArrayList<DSCriterionEntry> dsCriterionEntries = aCriteria.getCriterionEntries();
            int ceCount = dsCriterionEntries.size();
            if (ceCount > 0)
            {
                for (DSCriterionEntry ce : dsCriterionEntries)
                {
                    dsCriterion = ce.getCriterion();

                    dataField = dsCriterion.getField();
                    logString = String.format("%s(%d/%d) %s %s %s", mPrefix, ceIndex++,
                                               ceCount, dataField.getName(),
                                               Field.operatorToString(ce.getLogicalOperator()),
                                               dataField.collapse());
                    mLogger.debug(logString);
                }
            }
            PropertyLogger propertyLogger = new PropertyLogger(mLogger);
            propertyLogger.writeFull(aCriteria.getProperties());
        }
    }

    public void writeSimple(DSCriteria aCriteria)
    {
        if (aCriteria != null)
        {
            int ceIndex = 1;
            String logString;
            DataField dataField;
            DSCriterion dsCriterion;

            mDataFieldLogger.writeNV(mPrefix + "Name", aCriteria.getName());
            ArrayList<DSCriterionEntry> dsCriterionEntries = aCriteria.getCriterionEntries();
            int ceCount = dsCriterionEntries.size();
            if (ceCount > 0)
            {
                for (DSCriterionEntry ce : dsCriterionEntries)
                {
                    dsCriterion = ce.getCriterion();

                    dataField = dsCriterion.getField();
                    logString = String.format("%s(%d/%d) %s %s %s", mPrefix, ceIndex++,
                                              ceCount, dataField.getName(),
                                              Field.operatorToString(ce.getLogicalOperator()),
                                              dataField.collapse());
                    mLogger.debug(logString);
                }
            }
        }
    }

    public void write(DSCriteria aCriteria)
    {
        writeFull(aCriteria);
    }
}
