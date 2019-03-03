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

package com.nridge.core.base.ds;

import com.nridge.core.base.field.Field;
import com.nridge.core.base.field.data.DataDateTimeField;
import com.nridge.core.base.field.data.DataField;
import com.nridge.core.base.field.data.DataIntegerField;
import com.nridge.core.base.field.data.DataTextField;
import org.apache.commons.lang3.StringUtils;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

/**
 * A DSCriterion is responsible for encapsulating a query field, its operator
 * and one or more values.  It is exclusively referenced from within a
 * {@link DSCriteria} object.
 *
 * @since 1.0
 * @author Al Cole
 */
public class DSCriterion
{
    private DataField mField;
    private boolean mIsCaseInsensitive;
    private Field.Operator mOperator = Field.Operator.UNDEFINED;

    /**
     * Constructor that accepts a {@link DataField} and field
     * operator and initializes the DSCriterion accordingly.
     *
     * @param aField Data field.
     * @param anOperator Field operator.
     */
    public DSCriterion(DataField aField, Field.Operator anOperator)
    {
        mOperator = anOperator;
        mIsCaseInsensitive = false;
        mField = new DataField(aField);
    }

    /**
     * Constructor accepts a field name, operator and a single value
     * and initializes the DSCriterion accordingly.
     *
     * @param aName Field name.
     * @param anOperator Field operator.
     * @param aValue Field value.
     */
    public DSCriterion(String aName, Field.Operator anOperator, String aValue)
    {
        mOperator = anOperator;
        mIsCaseInsensitive = false;
        mField = new DataTextField(aName, StringUtils.EMPTY, aValue);
    }

    /**
     * Constructor accepts a field name, type, operator and a single value
     * and initializes the DSCriterion accordingly.
     *
     * @param aType Field type.
     * @param aName Field name.
     * @param anOperator Field operator.
     * @param aValue Field value.
     */
    public DSCriterion(Field.Type aType, String aName, Field.Operator anOperator, String aValue)
    {
        mOperator = anOperator;
        mIsCaseInsensitive = false;
        mField = new DataField(aType, aName,  aValue);
    }

    /**
     * Constructor accepts a field name, operator and an array of values
     * and initializes the DSCriterion accordingly.
     *
     * @param aType Field type.
     * @param aName Field name.
     * @param anOperator Field operator.
     * @param aValues An array of values.
     */
    public DSCriterion(Field.Type aType, String aName, Field.Operator anOperator, String... aValues)
    {
        mOperator = anOperator;
        mIsCaseInsensitive = false;
        mField = new DataField(aType, aName);
        if ((aValues != null) && (aValues.length > 0))
        {
            mField = new DataField(aType, aName);
            mField.setValues(aValues);
        }
    }

    /**
     * Constructor accepts a field name, operator and an array of values
     * and initializes the DSCriterion accordingly.
     *
     * @param aName Field name.
     * @param anOperator Field operator.
     * @param aValues An array of values.
     */
    public DSCriterion(String aName, Field.Operator anOperator, String... aValues)
    {
        mOperator = anOperator;
        mIsCaseInsensitive = false;
        mField = new DataTextField(aName);
        if ((aValues != null) && (aValues.length > 0))
            mField = new DataTextField(aName, StringUtils.EMPTY, aValues);
    }

    /**
     * Constructor accepts a field name, operator and an array of values
     * and initializes the DSCriterion accordingly.
     *
     * @param aName Field name.
     * @param anOperator Field operator.
     * @param aValues An array of values.
     */
    public DSCriterion(String aName, Field.Operator anOperator, ArrayList<String> aValues)
    {
        mOperator = anOperator;
        mIsCaseInsensitive = false;
        mField = new DataTextField(aName);
        if ((aValues != null) && (aValues.size() > 0))
            mField = new DataTextField(aName, StringUtils.EMPTY, aValues);
    }

    /**
     * Constructor accepts a field name, operator and a single value
     * and initializes the DSCriterion accordingly.
     *
     * @param aName Field name.
     * @param anOperator Field operator.
     * @param aValue Field value.
     */
    public DSCriterion(String aName, Field.Operator anOperator, int aValue)
    {
        mOperator = anOperator;
        mIsCaseInsensitive = false;
        mField = new DataIntegerField(aName, StringUtils.EMPTY, aValue);
    }

    /**
     * Constructor accepts a field name, operator and an array of values
     * and initializes the DSCriterion accordingly.
     *
     * @param aName Field name.
     * @param anOperator Field operator.
     * @param aValues An array of values.
     */
    public DSCriterion(String aName, Field.Operator anOperator, int... aValues)
    {
        mOperator = anOperator;
        mIsCaseInsensitive = false;
        if ((aValues != null) && (aValues.length > 0))
        {
            mField = new DataIntegerField(aName);
            for (int paramValue : aValues)
                mField.addValue(Integer.toString(paramValue));
        }
        else
            mField = new DataIntegerField(aName);
    }

    /**
     * Constructor accepts a field name, operator and a single value
     * and initializes the DSCriterion accordingly.
     *
     * @param aName Field name.
     * @param anOperator Field operator.
     * @param aValue Field value.
     */
    public DSCriterion(String aName, Field.Operator anOperator, long aValue)
    {
        mOperator = anOperator;
        mIsCaseInsensitive = false;
        mField = new DataField(aName, StringUtils.EMPTY, aValue);
    }

    /**
     * Constructor accepts a field name, operator and a single value
     * and initializes the DSCriterion accordingly.
     *
     * @param aName Field name.
     * @param anOperator Field operator.
     * @param aValue Field value.
     */
    public DSCriterion(String aName, Field.Operator anOperator, float aValue)
    {
        mOperator = anOperator;
        mIsCaseInsensitive = false;
        mField = new DataField(aName, StringUtils.EMPTY, aValue);
    }

    /**
     * Constructor accepts a field name, operator and a single value
     * and initializes the DSCriterion accordingly.
     *
     * @param aName Field name.
     * @param anOperator Field operator.
     * @param aValue Field value.
     */
    public DSCriterion(String aName, Field.Operator anOperator, double aValue)
    {
        mOperator = anOperator;
        mIsCaseInsensitive = false;
        mField = new DataField(aName, StringUtils.EMPTY, aValue);
    }

    /**
     * Constructor accepts a field name, operator and a single value
     * and initializes the DSCriterion accordingly.
     *
     * @param aName Field name.
     * @param anOperator Field operator.
     * @param aValue Field value.
     */
    public DSCriterion(String aName, Field.Operator anOperator, Date aValue)
    {
        mOperator = anOperator;
        mIsCaseInsensitive = false;
        mField = new DataField(aName, StringUtils.EMPTY, aValue);
    }

    /**
     * Constructor accepts a field name, operator and a single value
     * and initializes the DSCriterion accordingly.
     *
     * @param aName      Field name.
     * @param anOperator Field operator.
     * @param aValue     Field value.
     */
    public DSCriterion(String aName, Field.Operator anOperator, boolean aValue)
    {
        mOperator = anOperator;
        mIsCaseInsensitive = false;
        mField = new DataField(aName, StringUtils.EMPTY, aValue);
    }

    /**
     * Constructor accepts a field name, operator and an array of values
     * and initializes the DSCriterion accordingly.
     *
     * @param aName Field name.
     * @param anOperator Field operator.
     * @param aValues An array of values.
     */
    public DSCriterion(String aName, Field.Operator anOperator, Date... aValues)
    {
        mOperator = anOperator;
        mIsCaseInsensitive = false;
        if ((aValues != null) && (aValues.length > 0))
        {
            mField = new DataDateTimeField(aName);
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(Field.FORMAT_DATETIME_DEFAULT);
            for (Date paramValue : aValues)
                mField.addValue(simpleDateFormat.format(paramValue.getTime()));
        }
        else
            mField = new DataDateTimeField(aName);
    }

    /**
     * Returns a string summary representation of a criterion.
     *
     * @return String summary representation of a criterion.
     */
    @Override
    public String toString()
    {
        return String.format("%s %s %s", mField.getName(), mOperator.name(), mField.getValue());
    }

    /**
     * Returns the logical (a.k.a. field) operator.
     *
     * @return Logical operator.
     */
    public Field.Operator getLogicalOperator()
    {
        return mOperator;
    }

    /**
     * Returns the field type.
     *
     * @return Field type.
     */
    public Field.Type getType()
    {
        return mField.getType();
    }

    /**
     * Returns the name of the field.
     *
     * @return Field name.
     */
    public String getName()
    {
        return mField.getName();
    }

    /**
     * Returns the value of the field.
     *
     * @return Field value.
     */
    public String getValue()
    {
        return mField.getValue();
    }

    /**
     * Returns the value of the field as a Date.
     *
     * @return Field value as a Date.
     */
    public Date getValueAsDate()
    {
        return mField.getValueAsDate();
    }

    /**
     * Returns the name and value of the criterion as a
     * {@link DataField}.
     *
     * @return Data field.
     */
    public DataField getField()
    {
        return mField;
    }

    /**
     * Returns <i>true</i> if the criterion is case sensitive.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isCaseInsensitive()
    {
        return mIsCaseInsensitive;
    }

    /**
     * Assigns a boolean flag indicating whether the criterion
     * should be case sensitive.
     *
     * @param aFlag Boolean flag.
     */
    public void setCaseInsensitiveFlag(boolean aFlag)
    {
        mIsCaseInsensitive = aFlag;
    }

    /**
     * Returns <i>true</i> if the criterion contains multiple values.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isMultiValue()
    {
        return mField.isMultiValue();
    }

    /**
     * Returns the array of values associated with this criterion.
     *
     * @return Array of values.
     */
    public final ArrayList<String> getValues()
    {
        return mField.getValues();
    }

    /**
     * Adds the value to the collection of criterion values.
     *
     * @param aValue Field value.
     */
    public void addValue(String aValue)
    {
        mField.addValue(aValue);
    }

    /**
     * Returns the count of values associated with criterion.
     *
     * @return Count of values.
     */
    public int count()
    {
        return mField.valueCount();
    }
}
