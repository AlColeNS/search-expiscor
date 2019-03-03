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

import com.nridge.core.base.field.Field;
import com.nridge.core.base.std.DatUtl;
import com.nridge.core.base.std.StrUtl;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * The Data Field Analyzer class will examine small-to-medium sized
 * data sets to determine their type and value composition.
 *
 * A good enhancement to this package would be the inclusion of the
 * Apache Commons Math features - see link below.
 *
 * @see <a href="http://commons.apache.org/proper/commons-math/userguide/stat.html">Apache Commons Math</a>
 *
 * @author Al Cole
 * @since 1.0
 */
public class DataFieldAnalyzer
{
    private String mName;
    private int mNullCount;
    private boolean mIsDate;
    private boolean mIsFloat;
    private int mTotalValues;
    private boolean mIsInteger;
    private boolean mIsBoolean;
    private Map<String,Integer> mValueCount;

    /**
     * Constructor with a unique field name.
     *
     * @param aName Field name.
     */
    public DataFieldAnalyzer(String aName)
    {
        reset(aName);
    }

    /**
     * Use this if you wish to reuse the object instance.
     *
     * @param aName Field name.
     */
    public void reset(String aName)
    {
        mName = aName;
        mNullCount = 0;
        mIsDate = true;
        mIsFloat = true;
        mTotalValues = 0;
        mIsInteger = true;
        mIsBoolean = true;
        mValueCount = new HashMap<>();
    }

    /**
     * Convenience method for the DataAnalyzer class to generate a
     * table definition instance.
     *
     * @param aSampleCount Sample of top counts of data values.
     *
     * @return DataBag instance.
     */
    public DataBag createDefinition(int aSampleCount)
    {
        String fieldName, fieldTitle;

        DataBag detailsBag = new DataBag(mName);
        detailsBag.add(new DataTextField("name", "Name"));
        detailsBag.add(new DataTextField("type", "Type"));
        detailsBag.add(new DataIntegerField("total_count", "Total Count"));
        detailsBag.add(new DataIntegerField("null_count", "Null Count"));
        detailsBag.add(new DataIntegerField("unique_count", "Unique Count"));
        detailsBag.add(new DataTextField("minimum", "Minimum"));
        detailsBag.add(new DataTextField("maximum", "Maximum"));
        detailsBag.add(new DataTextField("mean", "Mean"));
        detailsBag.add(new DataTextField("standard_deviation", "Deviation"));
        for (int col = 0; col < aSampleCount; col++)
        {
            fieldName = String.format("value_%02d", col+1);
            fieldTitle = String.format("Value %02d", col+1);
            detailsBag.add(new DataTextField(fieldName, fieldTitle));
            fieldName = String.format("count_%02d", col+1);
            fieldTitle = String.format("Count %02d", col+1);
            detailsBag.add(new DataIntegerField(fieldName, fieldTitle));
            fieldName = String.format("percent_%02d", col+1);
            fieldTitle = String.format("Percent %02d", col+1);
            detailsBag.add(new DataTextField(fieldName, fieldTitle));
        }

        return detailsBag;
    }

    private boolean isNumberType()
    {
        return mIsFloat || mIsInteger;
    }

    private void scanType(String aValue)
    {
        if (isNumberType())
        {
            if (NumberUtils.isParsable(aValue))
            {
                int offset = aValue.indexOf(StrUtl.CHAR_DOT);
                if ((mIsInteger) && (offset != -1))
                    mIsInteger = false;
            }
            else
            {
                if (mIsInteger)
                    mIsInteger = false;
                if (mIsFloat)
                    mIsFloat = false;
            }
        }
        if (mIsDate)
        {
            Date fieldDate = DatUtl.detectCreateDate(aValue);
            if (fieldDate == null)
                mIsDate = false;
        }
        if (mIsBoolean)
        {
            if ((! aValue.equalsIgnoreCase(StrUtl.STRING_TRUE)) &&
                (! aValue.equalsIgnoreCase(StrUtl.STRING_YES)) &&
                (! aValue.equalsIgnoreCase(StrUtl.STRING_FALSE)) &&
                (! aValue.equalsIgnoreCase(StrUtl.STRING_NO)))
                mIsBoolean = false;
        }
    }

    /**
     * Scans the data value to determine its type and metric information.
     *
     * @param aValue Data value.
     */
    public void scan(String aValue)
    {
        mTotalValues++;
        if (StringUtils.isNotEmpty(aValue))
        {
            scanType(aValue);
            Integer curCount = mValueCount.get(aValue);
            if (curCount == null)
                mValueCount.put(aValue, 1);
            else
                mValueCount.put(aValue, curCount+1);
        }
        else
            mNullCount++;
    }

    /**
     * Returns the derived type information once the scanning process
     * is complete.
     *
     * @return Field type.
     */
    public Field.Type getType()
    {
        if (mIsBoolean)
            return Field.Type.Boolean;
        else if (mIsInteger)
            return Field.Type.Integer;
        else if (mIsFloat)
            return Field.Type.Float;
        else if (mIsDate)
            return Field.Type.DateTime;
        else
            return Field.Type.Text;
    }

    /**
     * Returns a data bag of fields describing the scanned value data.
     * The bag will contain the field name, derived type, populated
     * count, null count and a sample count of values (with overall
     * percentages) that repeated most often.
     *
     * @param aSampleCount Identifies the top count of values.
     *
     * @return Data bag of analysis details.
     */
    public DataBag getDetails(int aSampleCount)
    {
        Date dateValue;
        Integer valueCount;
        String fieldName, fieldTitle, dataValue;
        Double valuePercentage, minValue, maxValue;

        Field.Type fieldType = getType();
        int uniqueValues = mValueCount.size();
        DataBag detailsBag = new DataBag(mName);
        detailsBag.add(new DataTextField("name", "Name", mName));
        detailsBag.add(new DataTextField("type", "Type", Field.typeToString(fieldType)));
        detailsBag.add(new DataIntegerField("total_count", "Total Count", mTotalValues));
        detailsBag.add(new DataIntegerField("null_count", "Null Count", mNullCount));
        detailsBag.add(new DataIntegerField("unique_count", "Unique Count", uniqueValues));

// Create a table from the values map and use sorting to get our top sample size.

        DataTable valuesTable = new DataTable(mName);
        valuesTable.add(new DataTextField("value", "Value"));
        valuesTable.add(new DataIntegerField("count", "Count"));
        valuesTable.add(new DataDoubleField("percentage", "Percentage"));

        minValue = Double.MAX_VALUE;
        maxValue = Double.MIN_VALUE;
        for (Map.Entry<String, Integer> entry : mValueCount.entrySet())
        {
            valuesTable.newRow();
            dataValue = entry.getKey();
            valueCount = entry.getValue();
            if (mTotalValues == 0)
                valuePercentage = 0.0;
            else
                valuePercentage = valueCount.doubleValue() / mTotalValues * 100.0;

            valuesTable.newRow();
            valuesTable.setValueByName("value", dataValue);
            valuesTable.setValueByName("count", valueCount);
            valuesTable.setValueByName("percentage", String.format("%.2f", valuePercentage));
            if (Field.isText(fieldType))
            {
                minValue = Math.min(minValue, dataValue.length());
                maxValue = Math.max(maxValue, dataValue.length());
            }
            else if (Field.isNumber(fieldType))
            {
                minValue = Math.min(minValue, Double.parseDouble(dataValue));
                maxValue = Math.max(maxValue, Double.parseDouble(dataValue));
            }
            else if (Field.isDateOrTime(fieldType))
            {

// While we are decomposing the date to milliseconds of time, you can do a Date(milliseconds)
// reconstruction.

                dateValue = DatUtl.detectCreateDate(dataValue);
                if (dataValue != null)
                {
                    minValue = Math.min(minValue, dateValue.getTime());
                    maxValue = Math.max(maxValue, dateValue.getTime());
                }
            }
            valuesTable.addRow();
        }
        valuesTable.sortByColumn("count", Field.Order.DESCENDING);

        if (Field.isBoolean(fieldType))
        {
            detailsBag.add(new DataTextField("minimum", "Minimum", StrUtl.STRING_FALSE));
            detailsBag.add(new DataTextField("maximum", "Maximum", StrUtl.STRING_TRUE));
        }
        else if (Field.isDateOrTime(fieldType))
        {
            detailsBag.add(new DataTextField("minimum", "Minimum", Field.dateValueFormatted(new Date(minValue.longValue()), Field.FORMAT_DATETIME_DEFAULT)));
            detailsBag.add(new DataTextField("maximum", "Maximum", Field.dateValueFormatted(new Date(maxValue.longValue()), Field.FORMAT_DATETIME_DEFAULT)));
        }
        else
        {
            detailsBag.add(new DataTextField("minimum", "Minimum", String.format("%.2f", minValue)));
            detailsBag.add(new DataTextField("maximum", "Maximum", String.format("%.2f", maxValue)));
        }

// Create columns for the top sample sizes (value, matching count, matching percentage)

        int adjCount = Math.min(aSampleCount, valuesTable.rowCount());
        for (int row = 0; row < adjCount; row++)
        {
            fieldName = String.format("value_%02d", row+1);
            fieldTitle = String.format("Value %02d", row+1);
            dataValue = valuesTable.getValueByName(row, "value");
            detailsBag.add(new DataTextField(fieldName, fieldTitle, dataValue));
            fieldName = String.format("count_%02d", row+1);
            fieldTitle = String.format("Count %02d", row+1);
            detailsBag.add(new DataIntegerField(fieldName, fieldTitle, valuesTable.getValueByName(row, "count")));
            fieldName = String.format("percent_%02d", row+1);
            fieldTitle = String.format("Percent %02d", row+1);
            detailsBag.add(new DataDoubleField(fieldName, fieldTitle, valuesTable.getValueByName(row, "percentage")));
        }

        return detailsBag;
    }
}
