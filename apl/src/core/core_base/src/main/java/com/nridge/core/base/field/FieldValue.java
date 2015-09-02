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

package com.nridge.core.base.field;

import com.nridge.core.base.field.data.DataBag;

import com.nridge.core.base.std.StrUtl;
import org.apache.commons.lang3.StringUtils;

import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

/**
 * A FieldValue is responsible for managing one or more field
 * values used by the {@link DataBag} class.
 *
 * @author Al Cole
 * @since 1.0
 */
public class FieldValue
{
    private boolean mIsAssigned;
    private boolean mIsMultiValue;
    private ArrayList<String> mValues;
    private String mDefaultValue = StringUtils.EMPTY;

    /**
     * Default constructor.
     */
    public FieldValue()
    {
        mValues = new ArrayList<String>();
    }

    /**
     * Constructor that accepts an initial value.
     *
     * @param aValue Field value.
     */
    public FieldValue(String aValue)
    {
        mValues = new ArrayList<String>();
        setValue(aValue);
    }

    /**
     * Constructor that accepts an initial value.
     *
     * @param aValues Field values.
     */
    public FieldValue(String ... aValues)
    {
        mValues = new ArrayList<String>();
        setValues(aValues);
    }

    /**
     * Constructor that accepts an initial value.
     *
     * @param aValues Field values.
     */
    public FieldValue(ArrayList<String> aValues)
    {
        mValues = new ArrayList<String>();
        setValues(aValues);
    }

    /**
     * Constructor clones an existing FieldValue.
     *
     * @param aFieldValue Field value instance.
     */
    public FieldValue(final FieldValue aFieldValue)
    {
        mValues = new ArrayList<String>();
        if (aFieldValue != null)
        {
            if (aFieldValue.count() > 0)
            {
                for (String strValue : aFieldValue.getValues())
                    addValue(strValue);
            }
            this.setAssignedFlag(aFieldValue.isAssigned());
            this.setMultiValueFlag(aFieldValue.isMultiValue());
            this.setDefaultValue(aFieldValue.getDefaultValue());
        }
    }

    /**
     * Returns a string representation of a DataField.
     *
     * @return String summary representation of this DataField.
     */
    @Override
    public String toString()
    {
        if (mValues.size() > 1)
            return getFirstValue() + "...";
        else
            return getFirstValue();
    }

    /**
     * Returns <i>true</i> if the field represents a multi-value or
     * <i>false</i> otherwise.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isMultiValue()
    {
        return mIsMultiValue;
    }

    /**
     * Identifies if the field should manage multiple values.  If this
     * feature is enabled (<i>true</i>), then the parent application
     * must manage the delimiter and value separation when the value
     * is assigned or retrieved.
     *
     * @param aIsMultiValue <i>true</i> or <i>false</i>
     */
    public void setMultiValueFlag(boolean aIsMultiValue)
    {
        mIsMultiValue = aIsMultiValue;
    }

    /**
     * Marks the field as having been assigned a value.
     *
     * @param aFlag <i>true</i> or <i>false</i>
     */
    public void setAssignedFlag(boolean aFlag)
    {
        mIsAssigned = aFlag;
    }

    /**
     * Returns the default value property of the field.
     *
     * @return A value or empty string (if unassigned).
     */
    public String getDefaultValue()
    {
        return mDefaultValue;
    }

    /**
     * Returns the default value property of the field as a <i>Date</i>
     * object instance.  The date is derived from the value using the
     * format mask of <code>Field.FORMAT_DATETIME_DEFAULT</code>.
     *
     * @return A {@link Date} reflecting the default value.
     */
    public Date getDefaultValueAsDate()
    {
        if (StringUtils.isNotEmpty(mDefaultValue))
        {
            ParsePosition parsePosition = new ParsePosition(0);
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(Field.FORMAT_DATETIME_DEFAULT);
            return simpleDateFormat.parse(mDefaultValue, parsePosition);
        }
        else
            return new Date();
    }

    /**
     * Returns <i>true</i> if the field has a value assigned to it or
     * <i>false</i> otherwise.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isAssigned()
    {
        return mIsAssigned;
    }

    /**
     * Returns the count of values in the list.
     *
     * @return Count of values in the list.
     */
    public int count()
    {
        return mValues.size();
    }

    /**
     * Clear the field values from the list and marking it as unassigned.
     */
    public void clearValues()
    {
        mValues.clear();
        mIsAssigned = false;
    }

    /**
     * Returns <i>true</i> if the field is empty or <i>false</i> otherwise.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isValueEmpty()
    {
        return StringUtils.isEmpty(getValue());
    }

    /**
     * Returns <i>true</i> if the field is not empty or <i>false</i> otherwise.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isValueNotEmpty()
    {
        return StringUtils.isNotEmpty(getValue());
    }

    /**
     * Returns a <i>String</i> representation of the field value.
     *
     * @return <i>String</i> representation of the field value.
     */
    public String getValue()
    {
        return getFirstValue();
    }

    /**
     * Returns the value in the list identified at the offset.
     *
     * @param anOffset Offset of value in the list.
     *
     * @return <i>String</i> representation of the field value
     * or an empty string if the list is empty.
     */
    public String getValue(int anOffset)
    {
        if (anOffset < count())
            return mValues.get(anOffset);
        else
            return StringUtils.EMPTY;
    }

    /**
     * Returns a <i>String</i> representation of the field value.
     *
     * @return <i>String</i> representation of the field value.
     */
    public String getValueAsString()
    {
        return getValue();
    }

    /**
     * Returns the first value in the list.
     *
     * @return <i>String</i> representation of the field value
     * or an empty string if the list is empty.
     */
    public String getFirstValue()
    {
        return getValue(0);
    }

    /**
     * Returns a hidden (e.g. encrypted) <i>String</i> representation of
     * the field value.
     * <p>
     * <b>Note:</b> The method uses a simple rot13 algorithm to transform
     * the value.  It is meant only to obscure the field value.
     * </p>
     * @return <i>String</i> representation of the field value.
     */
    public String getValueAsHidden()
    {
        return Field.hideValue(getValue());
    }

    /**
     * Return a read-only version of the value list.
     *
     * @return List of values.
     */
    public final ArrayList<String> getValues()
    {
        return mValues;
    }

    /**
     * Assigns the value list parameter to the field.
     *
     * @param aValues A value list that is formatted appropriately for
     *                the data type it represents.
     */
    public void setValues(ArrayList<String> aValues)
    {
        if (mValues != null)
        {
            if (count() > 0)
                clearValues();
            for (String strValue : aValues)
                mValues.add(strValue);
            if (! mIsAssigned)
                mIsAssigned = true;
            if (! mIsMultiValue)
                mIsMultiValue = true;
        }
    }

    /**
     * Assigns the value parameter to the field.
     *
     * @param aValue A value that is formatted appropriately for
     *               the data type it represents.
     */
    public void setValue(String aValue)
    {
        if (aValue != null)
        {
            if (count() > 0)
                clearValues();
            mValues.add(aValue);
            if (! mIsAssigned)
                mIsAssigned = true;
        }
    }

    /**
     * Assigns the value parameter to the list at the offset specified.
     *
     * @param anOffset Offset of value in the list to replace.
     * @param aValue A value that is formatted appropriately for
     *               the data type it represents.
     */
    public void setValue(int anOffset, String aValue)
    {
        if (aValue != null)
        {
            if (anOffset < count())
            {
                mValues.set(anOffset, aValue);
                if (! mIsAssigned)
                    mIsAssigned = true;
                if (! mIsMultiValue)
                    mIsMultiValue = true;
            }
        }
    }

    /**
     * Adds the value parameter to the field if it is multi-value.
     * Otherwise, it will simply set the current value.
     *
     * @param aValue A value that is formatted appropriately for
     *               the data type it represents.
     */
    public void addValue(String aValue)
    {
        if (aValue != null)
        {
            mValues.add(aValue);
            if (! mIsAssigned)
                mIsAssigned = true;
            if (! mIsMultiValue)
                mIsMultiValue = true;
        }
    }

    /**
     * Adds the value parameter to the field if it is multi-value
     * and ensure that it is unique.
     *
     * @param aValue A value that is formatted appropriately for
     *               the data type it represents.
     */
    public void addValueUnique(String aValue)
    {
        if (StringUtils.isNotEmpty(aValue))
        {
            if (! mValues.contains(aValue))
            {
                mValues.add(aValue);
                if (! mIsAssigned)
                    mIsAssigned = true;
                if (! mIsMultiValue)
                    mIsMultiValue = true;
            }
        }
    }

    /**
     * Assigns the value parameter to the field.
     *
     * @param aValue Value to assign.
     */
    public void setValue(int aValue)
    {
        setValue(((Integer)aValue).toString());
    }

    /**
     * Returns the field value as an <i>int</i> type.
     *
     * @return Value of the field.
     */
    public int getValueAsInt()
    {
        return Field.createInt(getValue());
    }

    /**
     * Returns the field value as an <i>Integer</i> type.
     *
     * @return Value of the field.
     */
    public Integer getValueAsIntegerObject()
    {
        return Field.createIntegerObject(getValue());
    }

    /**
     * Assigns the value parameter to the field.
     *
     * @param aValue Value to assign.
     */
    public void setValue(long aValue)
    {
        setValue(((Long)aValue).toString());
    }

    /**
     * Returns the field value as a <i>long</i> type.
     *
     * @return Value of the field.
     */
    public long getValueAsLong()
    {
        return Field.createLong(getValue());
    }

    /**
     * Returns the field value as a <i>Long</i> type.
     *
     * @return Value of the field.
     */
    public Long getValueAsLongObject()
    {
        return Field.createLongObject(getValue());
    }

    /**
     * Assigns the value parameter to the field.
     *
     * @param aValue Value to assign.
     */
    public void setValue(float aValue)
    {
        setValue(((Float)aValue).toString());
    }

    /**
     * Returns the field value as a <i>float</i> type.
     *
     * @return Value of the field.
     */
    public float getValueAsFloat()
    {
        return Field.createFloat(getValue());
    }

    /**
     * Returns the field value as a <i>Float</i> type.
     *
     * @return Value of the field.
     */
    public Float getValueAsFloatObject()
    {
        return Field.createFloatObject(getValue());
    }

    /**
     * Assigns the value parameter to the field.
     *
     * @param aValue Value to assign.
     */
    public void setValue(double aValue)
    {
        setValue(((Double)aValue).toString());
    }

    /**
     * Returns the field value as a <i>double</i> type.
     *
     * @return Value of the field.
     */
    public double getValueAsDouble()
    {
        return Field.createDouble(getValue());
    }

    /**
     * Returns the field value as a <i>Double</i> type.
     *
     * @return Value of the field.
     */
    public Double getValueAsDoubleObject()
    {
        return Field.createDoubleObject(getValue());
    }

    /**
     * Assigns the value parameter to the field.
     *
     * @param aValue Value to assign.
     */
    public void setValue(boolean aValue)
    {
        if (aValue)
            setValue(StrUtl.STRING_TRUE);
        else
            setValue(StrUtl.STRING_FALSE);
    }

    /**
     * Returns <i>true</i> if the field value evaluates as true or
     * <i>false</i> otherwise.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isValueTrue()
    {
        return Field.isValueTrue(getValue());
    }

    /**
     * Returns <i>false</i> if the field value evaluates as false or
     * <i>true</i> otherwise.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isValueFalse()
    {
        return !Field.isValueTrue(getValue());
    }

    /**
     * Returns the field value as a <i>Boolean</i> type.
     *
     * @return Value of the field.
     */
    public Boolean getValueAsBooleanObject()
    {
        return Field.isValueTrue(getValue());
    }

    /**
     * Assigns the value parameter to the field.
     *
     * @param aValue Value to assign.
     */
    public void setValue(Date aValue)
    {
        if (aValue != null)
        {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(Field.FORMAT_DATETIME_DEFAULT);
            setValue(simpleDateFormat.format(aValue.getTime()));
        }
    }

    /**
     * Returns the field value as a <i>Date</i> type.
     *
     * @return Value of the field.
     */
    public Date getValueAsDate()
    {
        return Field.createDate(getValue());
    }

    /**
     * Returns the Date field value as a <i>long</i> type.
     *
     * @return Value of the field.
     */
    public long getValueAsDateLong()
    {
        return Field.createDateLong(getValue());
    }

    /**
     * Assigns a default value property to the field.
     *
     * @param aDefaultValue Default value to assign.
     */
    public void setDefaultValue(String aDefaultValue)
    {
        mDefaultValue = aDefaultValue;
    }

    /**
     * Assigns a default value property to the field.
     *
     * @param aValue Default value to assign.
     */
    public void setDefaultValue(int aValue)
    {
        setDefaultValue(((Integer) aValue).toString());
    }

    /**
     * Assigns a default value property to the field.
     *
     * @param aValue Default value to assign.
     */
    public void setDefaultValue(long aValue)
    {
        setDefaultValue(((Long) aValue).toString());
    }

    /**
     * Assigns a default value property to the field.
     *
     * @param aValue Default value to assign.
     */
    public void setDefaultValue(float aValue)
    {
        setDefaultValue(((Float) aValue).toString());
    }

    /**
     * Assigns a default value property to the field.
     *
     * @param aValue Default value to assign.
     */
    public void setDefaultValue(double aValue)
    {
        setDefaultValue(((Double) aValue).toString());
    }

    /**
     * Assigns a default value property to the field.
     *
     * @param aValue Default value to assign.
     */
    public void setDefaultValue(boolean aValue)
    {
        if (aValue)
            setDefaultValue(StrUtl.STRING_TRUE);
        else
            setDefaultValue(StrUtl.STRING_FALSE);
    }

    /**
     * Assigns a default value property to the field.
     *
     * @param aValue Default value to assign.
     */
    public void setDefaultValue(Date aValue)
    {
        if (aValue != null)
        {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(Field.FORMAT_DATETIME_DEFAULT);
            setDefaultValue(simpleDateFormat.format(aValue.getTime()));
        }
    }

    /**
     * Assigns the values parameter to the field.
     *
     * @param aValues An array of String values.
     */
    public void setValues(String... aValues)
    {
        if (aValues.length > 0)
        {
            clearValues();
            setMultiValueFlag(true);
            for (String strValue : aValues)
                addValue(strValue);
        }
    }

    /**
     * Copies the default value (if one was provided) to the field
     * value property.
     */
    public void assignValueFromDefault()
    {
        if (StringUtils.isNotEmpty(mDefaultValue))
        {
            if (mDefaultValue.equals(Field.VALUE_DATETIME_TODAY))
                setValue(new Date());
            else
                setValue(mDefaultValue);
        }
    }

    /**
     * Collapses the list of values down to a single string.
     * Each value is separated by a delimiter character.
     *
     * @param aDelimiterChar Delimiter character.
     *
     * @return A collapsed string.
     */
    public String collapse(char aDelimiterChar)
    {
        return StrUtl.collapseToSingle(mValues, aDelimiterChar);
    }

    /**
     * Collapses the list of values down to a single string.
     * Each value is separated by a default delimiter
     * character.
     *
     * @return A collapsed string.
     */
    public String collapse()
    {
        return collapse(StrUtl.CHAR_PIPE);
    }

    /**
     * Expand the value string into a list of individual values
     * using the delimiter character to identify each one.
     *
     * @param aValue One or more values separated by a
     *               delimiter character.
     * @param aDelimiterChar Delimiter character.
     */
    public void expand(String aValue, char aDelimiterChar)
    {
        if (StringUtils.isNotEmpty(aValue))
        {
            mValues = StrUtl.expandToList(aValue, aDelimiterChar);
            if (! mIsAssigned)
                mIsAssigned = true;
            if (! mIsMultiValue)
                mIsMultiValue = true;
        }
    }

    /**
     * Expand the value string into a list of individual values
     * using the default delimiter character to identify each one.
     *
     * @param aValue One or more values separated by a
     *               delimiter character.
     */
    public void expand(String aValue)
    {
        expand(aValue, StrUtl.CHAR_PIPE);
    }
}
