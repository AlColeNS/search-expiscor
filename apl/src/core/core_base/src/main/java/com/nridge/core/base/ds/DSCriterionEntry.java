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

package com.nridge.core.base.ds;

import com.nridge.core.base.field.Field;
import com.nridge.core.base.field.data.DataField;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Date;

/**
 * A DSCriterionEntry is responsible for encapsulating a criterion and
 * a boolean operator.  It is exclusively referenced from within a
 * {@link DSCriteria} object.
 *
 * @author Al Cole
 * @since 1.0
 */
public class DSCriterionEntry
{
    private ArrayList<DSCriterion> mDSCriterions;
    private Field.Operator mConditionalOperator = Field.Operator.AND;

    /**
     * Constructor accepts a {@link DSCriterion} initializes the
     * DSCriterionEntry accordingly.
     *
     * @param aDSCriterion Data source criterion instance.
     */
    public DSCriterionEntry(DSCriterion aDSCriterion)
    {
        mDSCriterions = new ArrayList<DSCriterion>();
        add(aDSCriterion);
    }

    /**
     * Constructor accepts a logical operator and a {@link DSCriterion}
     * and initializes the DSCriterionEntry accordingly.
     *
     * @param anOperator Field operator.
     * @param aDSCriterion Data source criterion instance.
     */
    public DSCriterionEntry(Field.Operator anOperator, DSCriterion aDSCriterion)
    {
        mDSCriterions = new ArrayList<DSCriterion>();
        setBooleanOperator(anOperator);
        add(aDSCriterion);
    }

    /**
     * Returns a string summary representation of a criterion entry.
     *
     * @return String summary representation of a criterion  entry.
     */
    @Override
    public String toString()
    {
        return String.format("%s [%d criterions]", mConditionalOperator.name(), mDSCriterions.size());
    }

    /**
     * Adds the criterion instance to the criterion entry.
     *
     * @param aDSCriterion Data source criterion instance.
     */
    public void add(DSCriterion aDSCriterion)
    {
        mDSCriterions.add(aDSCriterion);
    }

    /**
     * Returns the boolean operator for the criterion entry.
     *
     * @return Boolean operator.
     */
    public Field.Operator getBooleanOperator()
    {
        return mConditionalOperator;
    }

    /**
     * Assigns a boolean operator for the criterion entry.
     *
     * @param anOperator Boolean operator.
     */
    public void setBooleanOperator(Field.Operator anOperator)
    {
        mConditionalOperator = anOperator;
    }

    /**
     * Returns the count of values associated with this criterion entry.
     *
     * @return Count of values.
     */
    public int count()
    {
        return mDSCriterions.size();
    }

    /**
     * Returns <i>true</i> if the criterion entry is simple in
     * its nature.  A simple criteria is one where its boolean
     * operators is <i>AND</i> and its values is single.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isSimple()
    {
        return ((mConditionalOperator == Field.Operator.AND) && (mDSCriterions.size() == 1));
    }

    /**
     * Returns an array of criterions being managed by this
     * criterion entry.
     *
     * @return An array of criterions.
     */
    public ArrayList<DSCriterion> getCriterions()
    {
        return mDSCriterions;
    }

    /**
     * Returns the criterion identified by the offset parameter.
     *
     * @param anOffset Offset into the internal criterion array.
     *
     * @return Data source criterion.
     */
    public DSCriterion getCriterion(int anOffset)
    {
        return mDSCriterions.get(anOffset);
    }

    /**
     * Convenience method that returns the first criterion within
     * the internally managed criterion array.
     *
     * @return Data source criterion.
     */
    public DSCriterion getCriterion()
    {
        return mDSCriterions.get(0);
    }

    /**
     * Convenience method that returns the logical operator
     * from the first criterion within the internally managed
     * criterion array.
     *
     * @return Logical operator.
     */
    public Field.Operator getLogicalOperator()
    {
        return getCriterion().getLogicalOperator();
    }

    /**
     * Convenience method that returns the field name from the
     * first criterion within the internally managed criterion
     * array.
     *
     * @return Field name.
     */
    public String getName()
    {
        return getCriterion().getName();
    }

    /**
     * Convenience method that returns the field value from the
     * first criterion within the internally managed criterion
     * array.
     *
     * @return Field value.
     */
    public String getValue()
    {
        return getCriterion().getValue();
    }

    /**
     * Convenience method that returns the field value (formatted
     * as a Date) from the first criterion within the internally
     * managed criterion array.
     *
     * @return Field value.
     */
    public Date getValueAsDate()
    {
        return getCriterion().getValueAsDate();
    }

    /**
     * Returns the value associated with the criterion based on
     * the offset parameter.
     *
     * @param anOffset Offset into the internal criterion array.
     *
     * @return Field value.
     */
    public String getValue(int anOffset)
    {
        if (anOffset == 0)
            return getValue();
        else
        {
            DSCriterion dsCriterion = getCriterion();
            if (dsCriterion.isMultiValue())
            {
                ArrayList<String> valueList = dsCriterion.getValues();
                if (anOffset < valueList.size())
                    return valueList.get(anOffset);
            }
        }

        return StringUtils.EMPTY;
    }

    /**
     * Returns the value (formatted as a Date) associated with the
     * criterion based on the offset parameter.
     *
     * @param anOffset Offset into the internal criterion array.
     *
     * @return Field value.
     */
    public Date getValueAsDate(int anOffset)
    {
        if (anOffset == 0)
            return getValueAsDate();
        else
        {
            DSCriterion dsCriterion = getCriterion();
            if (dsCriterion.isMultiValue())
            {
                ArrayList<String> valueList = dsCriterion.getValues();
                if (anOffset < valueList.size())
                {
                    String dateValue = valueList.get(anOffset);
                    return Field.createDate(dateValue, Field.FORMAT_DATETIME_DEFAULT);
                }
            }
        }

        return new Date();
    }

    /**
     * Convenience method that returns the {@link DataField}
     * from the first criterion within the internally managed
     * criterion array.
     *
     * @return Simple field.
     */
    public DataField getField()
    {
        return getCriterion().getField();
    }

    /**
     * Convenience method that returns <i>true</i> if the
     * criterion entry is case sensitive.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isCaseInsensitive()
    {
        return getCriterion().isCaseInsensitive();
    }
}
