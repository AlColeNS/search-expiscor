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

package com.nridge.core.base.field.data;

/**
 * The Data Field Analyzer class will examine small-to-medium sized
 * data sets to determine their type and value composition.
 *
 * @author Al Cole
 * @since 1.0
 */
public class DataAnalyzer
{
    public final int SAMPLE_COUNT_DEFAULT = 10;

    private DataBag mBag;
    private DataTable mTable;
    private int mSampleCount;

    /**
     * Constructor where the data bag definition is used to identify
     * the fields and a default sample count is used.
     *
     * @param aBag Data bag instance.
     */
    public DataAnalyzer(DataBag aBag)
    {
        initMembers(aBag, SAMPLE_COUNT_DEFAULT);
    }

    /**
     * Constructor where the data bag definition is used to identify
     * the fields and a the sample count is driven by the parameter.
     *
     * @param aBag Data bag instance.
     * @param aSampleCount Sample value count.
     */
    public DataAnalyzer(DataBag aBag, int aSampleCount)
    {
        initMembers(aBag, aSampleCount);
    }

    private void initMembers(DataBag aBag, int aSampleCount)
    {
        String fieldName;
        DataFieldAnalyzer dataFieldAnalyzer;

        mBag = aBag;
        mSampleCount = aSampleCount;
        dataFieldAnalyzer = new DataFieldAnalyzer("Data Analyzer");
        mTable = new DataTable(dataFieldAnalyzer.createDefinition(aSampleCount));
        for (DataField dataField : aBag.getFields())
        {
            fieldName = dataField.getName();
            dataFieldAnalyzer = new DataFieldAnalyzer(fieldName);
            mTable.addProperty(fieldName, dataFieldAnalyzer);
        }
    }

    /**
     * Scans the data field values from within the data bag instance
     * to determine their type and metric information.
     *
     * @param aBag Data bag instance.
     */
    public void scan(DataBag aBag)
    {
        DataFieldAnalyzer dataFieldAnalyzer;

        for (DataField dataField : aBag.getFields())
        {
            dataFieldAnalyzer = (DataFieldAnalyzer) mTable.getProperty(dataField.getName());
            if (dataFieldAnalyzer != null)
                dataFieldAnalyzer.scan(dataField.getValue());
        }
    }

    /**
     * Scans the data field values from within the data table instance
     * to determine their type and metric information.
     *
     * @param aTable Data bag instance.
     */
    public void scan(DataTable aTable)
    {
        int rowCount = aTable.rowCount();
        for (int row = 0; row < rowCount; row++)
            scan(aTable.getRowAsBag(row));
    }

    /**
     * Returns a data table of fields describing the scanned value data.
     * The table will contain the field name, derived type, populated
     * and unique counts, null count and a sample count of values (with
     * overall percentages) that repeated most often.
     *
     * @return Data table of analysis details.
     */
    public DataTable getDetails()
    {
        DataBag dfaBag;
        DataFieldAnalyzer dataFieldAnalyzer;

        for (DataField dataField : mBag.getFields())
        {
            dataFieldAnalyzer = (DataFieldAnalyzer) mTable.getProperty(dataField.getName());
            if (dataFieldAnalyzer != null)
            {
                dfaBag = dataFieldAnalyzer.getDetails(mSampleCount);
                mTable.addRow(dfaBag);
            }
        }
        mTable.clearProperties();

        return mTable;
    }
}
