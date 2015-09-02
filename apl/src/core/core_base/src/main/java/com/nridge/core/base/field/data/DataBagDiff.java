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

package com.nridge.core.base.field.data;

import com.nridge.core.base.field.Field;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * The DataBagDiff is responsible for comparing to DataBag
 * instances and determining if they differ and how.
 *
 * @author Al Cole
 * @since 1.0
 */
public class DataBagDiff
{
    private boolean mIsCompared;
    private DataTable mDiffTable;

    public DataBagDiff()
    {
        initMembers();
    }

    private void initMembers()
    {
        mDiffTable = new DataTable("data_diff_bag");

        mDiffTable.add(new DataTextField("name", "Name"));
        mDiffTable.add(new DataTextField("status", "status"));
        mDiffTable.add(new DataTextField("description", "Description"));
    }

    private void addStatus(String aName, String aStatus, String aDescription)
    {
        mDiffTable.newRow();

        mDiffTable.setValueByName("name", aName);
        mDiffTable.setValueByName("status", aStatus);
        mDiffTable.setValueByName("description", aDescription);

        mDiffTable.addRow();
    }

    public void compare(DataField aField1, DataField aField2)
    {
        if (aField1.getType() != aField2.getType())
            addStatus(aField1.getName(), Field.DIFF_STATUS_UPDATED, "Field data type differs.");
        if (! StringUtils.equals(aField1.getTitle(), aField2.getTitle()))
            addStatus(aField1.getName(), Field.DIFF_STATUS_UPDATED, "Field title differs.");
        if (aField1.getDisplaySize() != aField2.getDisplaySize())
            addStatus(aField1.getName(), Field.DIFF_STATUS_UPDATED, "Field display size differs.");
        if (aField1.getSortOrder() != aField2.getSortOrder())
            addStatus(aField1.getName(), Field.DIFF_STATUS_UPDATED, "Field sort order differs.");
        if (aField1.isRangeAssigned() == aField2.isRangeAssigned())
        {
            if ((aField1.isRangeAssigned()) && (! aField1.getRange().isEqual(aField2.getRange())))
                addStatus(aField1.getName(), Field.DIFF_STATUS_UPDATED, "Field ranges differ.");
        }
        if (! aField1.isValueEqual(aField2))
            addStatus(aField1.getName(), Field.DIFF_STATUS_UPDATED, "Field values differ.");
        int featureCount1 = aField1.featureCount();
        int featureCount2 = aField2.featureCount();
        if (featureCount1 != featureCount2)
            addStatus(aField1.getName(), Field.DIFF_STATUS_UPDATED, "Field features differ.");
        else if (featureCount1 > 0)
        {
            String keyName1, keyValue1;

            for (Map.Entry<String, String> featureEntry : aField1.getFeatures().entrySet())
            {
                keyName1 = featureEntry.getKey();
                keyValue1 = featureEntry.getValue();
                if (! aField2.getFeature(keyName1).equals(keyValue1))
                {
                    addStatus(aField1.getName(), Field.DIFF_STATUS_UPDATED, "Field features differ.");
                    break;
                }
            }
        }
    }

    /**
     * Compares the two bags for differences.  The comparison will include
     * the meta data and values of the object instances.  Once this method
     * has been called, you can use the <code>isEqual()</code> and
     * <code>getDetails()</code> methods.
     *
     * @param aBag1 Data bag 1 instance.
     * @param aBag2 Data bag 2 instance.
     */
    public void compare(DataBag aBag1, DataBag aBag2)
    {
        reset();
        if ((aBag1 != null) && (aBag2 != null))
        {
            if (aBag1.getTypeId() != aBag2.getTypeId())
                addStatus(aBag1.getName(), Field.DIFF_STATUS_UPDATED, "Bag type ids differ.");
            if (! StringUtils.equals(aBag1.getName(), aBag2.getName()))
                addStatus(aBag1.getName(), Field.DIFF_STATUS_UPDATED, "Bag names differ.");
            if (! StringUtils.equals(aBag1.getTitle(), aBag2.getTitle()))
                addStatus(aBag1.getName(), Field.DIFF_STATUS_UPDATED, "Bag titles differ.");
            int featureCount1 = aBag1.featureCount();
            int featureCount2 = aBag2.featureCount();
            if (featureCount1 != featureCount2)
                addStatus(aBag1.getName(), Field.DIFF_STATUS_UPDATED, "Bag features differ.");
            else if (featureCount1 > 0)
            {
                String keyName1, keyValue1;

                for (Map.Entry<String, String> featureEntry : aBag1.getFeatures().entrySet())
                {
                    keyName1 = featureEntry.getKey();
                    keyValue1 = featureEntry.getValue();
                    if (! aBag2.getFeature(keyName1).equals(keyValue1))
                    {
                        addStatus(aBag1.getName(), Field.DIFF_STATUS_UPDATED, "Bag features differ.");
                        break;
                    }
                }
            }

            boolean isFound;
            for (DataField dataField1 : aBag1.getFields())
            {
                isFound = false;
                for (DataField dataField2 : aBag2.getFields())
                {
                    if (dataField1.getName().equals(dataField2.getName()))
                    {
                        compare(dataField1, dataField2);
                        isFound = true;
                        break;
                    }
                }
                if (! isFound)
                    addStatus(dataField1.getName(), Field.DIFF_STATUS_DELETED, "Field not found in second bag.");
            }

            for (DataField dataField2 : aBag2.getFields())
            {
                isFound = false;
                for (DataField dataField1 : aBag1.getFields())
                {
                    if (dataField2.getName().equals(dataField1.getName()))
                    {
                        isFound = true;
                        break;
                    }
                }
                if (! isFound)
                    addStatus(dataField2.getName(), Field.DIFF_STATUS_ADDED, "Field added to second bag.");
            }
            mIsCompared = true;
        }
    }

    /**
     * Determines if the previously compared bags are equal.
     *
     * @return <i>true</i> if equal, <i>false</i> otherwise.
     */
    public boolean isEqual()
    {
        return (mIsCompared) && (mDiffTable.rowCount() == 0);
    }

    /**
     * Returns a details table containing the results of a previously
     * compared bag operation.
     *
     * @return Data table with details.
     */
    public final DataTable getDetails()
    {
        return mDiffTable;
    }

    /**
     * Resets the state of the comparison logic in anticipation
     * of another comparison operation.
     */
    public void reset()
    {
        mIsCompared = false;
        mDiffTable.emptyRows();
    }
}
