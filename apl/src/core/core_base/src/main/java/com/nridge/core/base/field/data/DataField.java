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
import com.nridge.core.base.field.FieldRange;
import com.nridge.core.base.field.FieldValue;
import com.nridge.core.base.std.StrUtl;
import org.apache.commons.lang3.StringUtils;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

/**
 * The DataField class models a simple meta data field.  This is the class
 * you should go to when you are looking for a basic data type, name, title
 * and value representation of a piece of information.
 * <p>
 * Alternative classes for modeling fields include SQLField where
 * where each offers progressively more properties to describe the meta data.
 * </p>
 *
 * @since 1.0
 * @author Al Cole
 */
public class DataField
{
    private int mDisplaySize;
    private FieldRange mRange;
    private FieldValue mFieldValue;
    private String mName = StringUtils.EMPTY;
    private HashMap<String, String> mFeatures;
    private String mTitle = StringUtils.EMPTY;
    private Field.Type mType = Field.Type.Text;
    private transient HashMap<String, Object> mProperties;
    private Field.Order mSortOrder = Field.Order.UNDEFINED;

    /**
     * Default constructor.
     */
    public DataField()
    {
        mFieldValue = new FieldValue();
        mFeatures = new HashMap<String, String>();
    }

    /**
     * Constructor clones an existing data field.
     *
     * @param aField Source field instance to clone.
     */
    public DataField(final DataField aField)
    {
        if (aField != null)
        {
            this.mFieldValue = new FieldValue(aField.getFieldValue());
            this.mFeatures = new HashMap<String, String>(aField.getFeatures());

            this.setName(aField.getName());
            this.setType(aField.getType());
            this.setTitle(aField.getTitle());
            this.setSortOrder(aField.getSortOrder());
            this.setDisplaySize(aField.getDisplaySize());
            this.setMultiValueFlag(aField.isMultiValue());
            this.setDefaultValue(aField.getDefaultValue());
            if (aField.isRangeAssigned())
                setRange(new FieldRange(aField.getRange()));
            // Ignoring mProperties
        }
    }

    /**
     * Constructor accepts a data type parameter and initializes the field
     * accordingly.
     *
     * @param aType Data type.
     */
    public DataField(Field.Type aType)
    {
        mFieldValue = new FieldValue();
        mFeatures = new HashMap<String, String>();
        setType(aType);
    }

    /**
     * Constructor accepts data type and name parameters to initialize the
     * field instance.
     *
     * @param aType Data type.
     * @param aName Name of the field.
     */
    public DataField(Field.Type aType, String aName)
    {
        mFieldValue = new FieldValue();
        mFeatures = new HashMap<String, String>();
        setType(aType);
        setName(aName);
    }

    /**
     * Constructor accepts data type, name and title parameters
     * to initialize the field instance.
     *
     * @param aType Data type.
     * @param aName Name of the field.
     * @param aTitle Title of the field.
     */
    public DataField(Field.Type aType, String aName, String aTitle)
    {
        mFieldValue = new FieldValue();
        mFeatures = new HashMap<String, String>();
        setType(aType);
        setName(aName);
        setTitle(aTitle);
    }

    /**
     * Constructor accepts data type, name, title and value parameters
     * to initialize the field instance.
     *
     * @param aType Data type.
     * @param aName Name of the field.
     * @param aTitle Title of the field.
     * @param aValue Value of the field.
     */
    public DataField(Field.Type aType, String aName, String aTitle, String aValue)
    {
        mFieldValue = new FieldValue();
        mFeatures = new HashMap<String, String>();
        setType(aType);
        setName(aName);
        setTitle(aTitle);
        setValue(aValue);
    }

    /**
     * Constructor accepts data type, name, title and values parameters
     * to initialize the field instance.
     *
     * @param aType Data type.
     * @param aName Name of the field.
     * @param aTitle Title of the field.
     * @param aValues Values of the field.
     */
    public DataField(Field.Type aType, String aName, String aTitle, String ... aValues)
    {
        mFieldValue = new FieldValue();
        mFeatures = new HashMap<String, String>();
        setType(aType);
        setName(aName);
        setTitle(aTitle);
        setValues(aValues);
    }

    /**
     * Constructor accepts data type, name, title and values parameters
     * to initialize the field instance.
     *
     * @param aType Data type.
     * @param aName Name of the field.
     * @param aTitle Title of the field.
     * @param aValues Values of the field.
     */
    public DataField(Field.Type aType, String aName, String aTitle, ArrayList<String> aValues)
    {
        mFieldValue = new FieldValue();
        mFeatures = new HashMap<String, String>();
        setType(aType);
        setName(aName);
        setTitle(aTitle);
        setValues(aValues);
    }

    /**
     * Constructor accepts field name and value parameters and initializes the
     * SimpleField as a <i>Integer</i> data type by default.
     *
     * @param aName Name of field.
     * @param aTitle Title of the field.
     * @param aValue Value of field.
     */
    public DataField(String aName, String aTitle, int aValue)
    {
        mFieldValue = new FieldValue();
        mFeatures = new HashMap<String, String>();
        setType(Field.Type.Integer);
        setName(aName);
        setTitle(aTitle);
        setValue(aValue);
    }

    /**
     * Constructor accepts field name and value parameters and initializes the
     * SimpleField as a <i>Long</i> data type by default.
     *
     * @param aName Name of field.
     * @param aTitle Title of the field.
     * @param aValue Value of field.
     */
    public DataField(String aName, String aTitle, long aValue)
    {
        mFieldValue = new FieldValue();
        mFeatures = new HashMap<String, String>();
        setType(Field.Type.Long);
        setName(aName);
        setTitle(aTitle);
        setValue(aValue);
    }

    /**
     * Constructor accepts field name and value parameters and initializes the
     * SimpleField as a <i>Float</i> data type by default.
     *
     * @param aName Name of field.
     * @param aTitle Title of the field.
     * @param aValue Value of field.
     */
    public DataField(String aName, String aTitle, float aValue)
    {
        mFieldValue = new FieldValue();
        mFeatures = new HashMap<String, String>();
        setType(Field.Type.Float);
        setName(aName);
        setTitle(aTitle);
        setValue(aValue);
    }

    /**
     * Constructor accepts field name and value parameters and initializes the
     * SimpleField as a <i>Double</i> data type by default.
     *
     * @param aName Name of field.
     * @param aTitle Title of the field.
     * @param aValue Value of field.
     */
    public DataField(String aName, String aTitle, double aValue)
    {
        mFieldValue = new FieldValue();
        mFeatures = new HashMap<String, String>();
        setType(Field.Type.Double);
        setName(aName);
        setTitle(aTitle);
        setValue(aValue);
    }

    /**
     * Constructor accepts field name and value parameters and initializes the
     * SimpleField as a <i>Boolean</i> data type by default.
     *
     * @param aName Name of field.
     * @param aTitle Title of the field.
     * @param aValue Value of field.
     */
    public DataField(String aName, String aTitle, boolean aValue)
    {
        mFieldValue = new FieldValue();
        mFeatures = new HashMap<String, String>();
        setType(Field.Type.Boolean);
        setName(aName);
        setTitle(aTitle);
        setValue(aValue);
    }

    /**
     * Constructor accepts field name and value parameters and initializes the
     * SimpleField as a <i>DateTime</i> data type by default.
     *
     * @param aName Name of field.
     * @param aTitle Title of the field.
     * @param aValue Value of field.
     */
    public DataField(String aName, String aTitle, Date aValue)
    {
        mFieldValue = new FieldValue();
        mFeatures = new HashMap<String, String>();
        setType(Field.Type.DateTime);
        setName(aName);
        setTitle(aTitle);
        setValue(aValue);
    }

    /**
     * Returns a string representation of a DataField.
     *
     * @return String summary representation of this DataField.
     */
    @Override
    public String toString()
    {
        if (mFieldValue.count() > 1)
            return String.format("%s: %s...", mName, mFieldValue.collapse());
        else
            return mName + ": " + mFieldValue.getFirstValue();
    }

    /**
     * Returns the field name.
     *
     * @return Field name.
     */
    public String getName()
    {
        return mName;
    }

    /**
     * Assigns the name of the field.
     *
     * @param aName Field name.
     */
    public void setName(String aName)
    {
        mName = aName;
    }

    /**
     * Returns the data type of the field.
     *
     * @return Data type.
     */
    public Field.Type getType()
    {
        return mType;
    }

    /**
     * Assigns the data type of the field.
     *
     * @param aType Data type.
     */
    public void setType(Field.Type aType)
    {
        mType = aType;
    }

    /**
     * Returns the title (or label) property of the field.
     *
     * @return A title or empty string (if unassigned).
     */
    public String getTitle()
    {
        return mTitle;
    }

    /**
     * Assigns a title (or label) property to the field.
     *
     * @param aTitle String identifying the title.
     */
    public void setTitle(String aTitle)
    {
        mTitle = aTitle;
    }

    /**
     * Returns the display size for the field.
     *
     * @return Size of display width.
     */
    public int getDisplaySize()
    {
        return mDisplaySize;
    }

    /**
     * Assigns the size parameter to the display size property.
     *
     * @param aDisplaySize Size of the display.
     */
    public void setDisplaySize(int aDisplaySize)
    {
        mDisplaySize = aDisplaySize;
    }

    /**
     * Convenience method determines if the field data type represents a
     * <i>Integer, Long, Float, Double</i> data type.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isTypeNumber()
    {
        return Field.isNumber(mType);
    }

    /**
     * Convenience method determines if the field data type represents a
     * <i>Boolean</i> data type.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isTypeBoolean()
    {
        return Field.isBoolean(mType);
    }

    /**
     * Convenience method determines if the field data type represents a
     * <i>Text</i> data type.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isTypeText()
    {
        return Field.isText(mType);
    }

    /**
     * Convenience method determines if the field data type represents a
     * <i>Date, Time, DateTime</i> data type.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isTypeDateOrTime()
    {
        return Field.isDateOrTime(mType);
    }

    /**
     * If a {@link FieldRange} instance was assigned to the
     * ComplexField, then this method will return a reference
     * to its instance.
     *
     * @return FieldRange if previously assigned or <i>null</i> otherwise.
     */
    public FieldRange getRange()
    {
        return mRange;
    }

    /**
     * Assign a {@link FieldRange} to the ComplexField.  A field range
     * is used by the validation methods to determine if a field value
     * falls within a min and max range or a enumerated list of options.
     *
     * @param aRange FieldRange instance reference.
     */
    public void setRange(FieldRange aRange)
    {
        mRange = aRange;
    }

    /**
     * Creates a {@link FieldRange} instance and assigns the <i>String</i>
     * array to it.
     *
     * @param aStrArgs Array of string values.
     */
    public void setRange(String... aStrArgs)
    {
        if (isTypeText())
            mRange = new FieldRange(aStrArgs);
    }

    /**
     * Creates a {@link FieldRange} instance and assigns the minimum
     * and maximum parameters to it.
     *
     * @param aMin Minimum value of the range.
     * @param aMax Maximum value of the range.
     */
    public void setRange(int aMin, int aMax)
    {
        if (isTypeNumber())
            mRange = new FieldRange(aMin, aMax);
    }

    /**
     * Creates a {@link FieldRange} instance and assigns the minimum
     * and maximum parameters to it.
     *
     * @param aMin Minimum value of the range.
     * @param aMax Maximum value of the range.
     */
    public void setRange(long aMin, long aMax)
    {
        if (isTypeNumber())
            mRange = new FieldRange(aMin, aMax);
    }

    /**
     * Creates a {@link FieldRange} instance and assigns the minimum
     * and maximum parameters to it.
     *
     * @param aMin Minimum value of the range.
     * @param aMax Maximum value of the range.
     */
    public void setRange(double aMin, double aMax)
    {
        if (isTypeNumber())
            mRange = new FieldRange(aMin, aMax);
    }

    /**
     * Creates a {@link FieldRange} instance and assigns the minimum
     * and maximum parameters to it.
     *
     * @param aMin Minimum value of the range.
     * @param aMax Maximum value of the range.
     */
    public void setRange(Date aMin, Date aMax)
    {
        if (isTypeDateOrTime())
            mRange = new FieldRange(aMin, aMax);
    }

    /**
     * Creates a {@link FieldRange} instance and assigns the minimum
     * and maximum parameters to it.
     *
     * @param aMin Minimum value of the range.
     * @param aMax Maximum value of the range.
     */
    public void setRange(Calendar aMin, Calendar aMax)
    {
        if (isTypeDateOrTime())
            mRange = new FieldRange(aMin, aMax);
    }

    /**
     * Removes the {@link FieldRange} assignment from the field.
     */
    public void clearRange()
    {
        mRange = null;
    }

    /**
     * Returns <i>true</i> if the field has a {@link FieldRange}
     * assigned to it or <i>false</i> otherwise.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isRangeAssigned()
    {
        return mRange != null;
    }

    /**
     * Returns an instance of a {@link FieldValue}.
     *
     * @return FieldValue instance.
     */
    public final FieldValue getFieldValue()
    {
        return mFieldValue;
    }

    /**
     * Assigns the value parameter to the field.
     *
     * @param aValue Value to assign.
     */
    public void setValue(long aValue)
    {
        mFieldValue.setValue(aValue);
    }

    /**
     * Returns the field value as a <i>Boolean</i> type.
     *
     * @return Value of the field.
     */
    public Boolean getValueAsBooleanObject()
    {
        return mFieldValue.getValueAsBooleanObject();
    }

    /**
     * Assigns the value parameter to the field.
     *
     * @param aValue Value to assign.
     */
    public void setValue(double aValue)
    {
        mFieldValue.setValue(aValue);
    }

    /**
     * Returns the first value in the list.
     *
     * @return <i>String</i> representation of the field value
     * or an empty string if the list is empty.
     */
    public String getFirstValue()
    {
        return mFieldValue.getFirstValue();
    }

    /**
     * Returns the field value as an <i>int</i> type.
     *
     * @return Value of the field.
     */
    public int getValueAsInt()
    {
        return mFieldValue.getValueAsInt();
    }

    /**
     * Return a read-only version of the value list.
     *
     * @return List of values.
     */
    public ArrayList<String> getValues()
    {
        return mFieldValue.getValues();
    }

    /**
     * Return a read-only version of value array.
     *
     * @return Array of values.
     */
    public String[] getValuesAsArray()
    {
        String[] valueArray;

        ArrayList<String> valueList = getValues();
        int valueCount = valueList.size();
        if (valueCount > 0)
        {
            valueArray = new String[valueCount];
            valueList.toArray(valueArray);
        }
        else
            valueArray = new String[0];

        return valueArray;
    }

    /**
     * Returns <i>true</i> if the field has a value assigned to it or
     * <i>false</i> otherwise.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isAssigned()
    {
        return mFieldValue.isAssigned();
    }

    /**
     * Assigns the value parameter to the field.
     *
     * @param aValue Value to assign.
     */
    public void setValue(float aValue)
    {
        mFieldValue.setValue(aValue);
    }

    /**
     * Identifies if the field should manage multiple values.  If this
     * feature is enabled (<i>true</i>), then the parent application
     * must manage the delimiter and value separation when the value
     * is assigned or retrieved.  The {@link com.nridge.core.base.field.FieldValue} class offers
     * some useful methods for this task.
     *
     * @param aIsMultiValue <i>true</i> or <i>false</i>
     */
    public void setMultiValueFlag(boolean aIsMultiValue)
    {
        mFieldValue.setMultiValueFlag(aIsMultiValue);
    }

    /**
     * Assigns the value parameter to the field.
     *
     * @param aValue Value to assign.
     */
    public void setValue(boolean aValue)
    {
        mFieldValue.setValue(aValue);
    }

    /**
     * Returns the field value as a <i>double</i> type.
     *
     * @return Value of the field.
     */
    public double getValueAsDouble()
    {
        return mFieldValue.getValueAsDouble();
    }

    /**
     * Marks the field as having been assigned a value.
     *
     * @param aFlag <i>true</i> or <i>false</i>
     */
    public void setAssignedFlag(boolean aFlag)
    {
        mFieldValue.setAssignedFlag(aFlag);
    }

    /**
     * Assigns the value parameter to the field.
     *
     * @param aValue Value to assign.
     */
    public void setValue(int aValue)
    {
        mFieldValue.setValue(aValue);
    }

    /**
     * Returns the field value as a <i>Long</i> type.
     *
     * @return Value of the field.
     */
    public Long getValueAsLongObject()
    {
        return mFieldValue.getValueAsLongObject();
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
        mFieldValue.addValue(aValue);
    }

    /**
     * Adds the value parameter to the field if it is multi-value.
     * Otherwise, it will simply set the current value.
     *
     * @param aValue A value that is formatted appropriately for
     *               the data type it represents.
     */
    public void addValueUnique(String aValue)
    {
        mFieldValue.addValueUnique(aValue);
    }

    /**
     * Adds the value parameter to the field if it is multi-value.
     * Otherwise, it will simply set the current value.
     *
     * @param aValues An array of String values.
     */
    public void addValues(ArrayList<String> aValues)
    {
        if (aValues != null)
        {
            for (String value : aValues)
                mFieldValue.addValue(value);
        }
    }

    /**
     * Adds the value parameter to the field if it is multi-value
     * and ensure that it is unique.
     *
     * @param aValues An array of String values.
     */
    public void addValuesUnique(ArrayList<String> aValues)
    {
        if (aValues != null)
        {
            for (String value : aValues)
                mFieldValue.addValueUnique(value);
        }
    }

    /**
     * Clear the field values from the list and marking it as unassigned.
     */
    public void clearValues()
    {
        mFieldValue.clearValues();
    }

    /**
     * Assigns the value parameter to the field.
     *
     * @param aValue A value that is formatted appropriately for
     *               the data type it represents.
     */
    public void setValue(String aValue)
    {
        mFieldValue.setValue(aValue);
    }

    /**
     * Returns the field value as a <i>Date</i> type.
     *
     * @return Value of the field.
     */
    public Date getValueAsDate()
    {
        return mFieldValue.getValueAsDate();
    }

    /**
     * Returns the field value as a <i>float</i> type.
     *
     * @return Value of the field.
     */
    public float getValueAsFloat()
    {
        return mFieldValue.getValueAsFloat();
    }

    /**
     * Returns the field value as a <i>Double</i> type.
     *
     * @return Value of the field.
     */
    public Double getValueAsDoubleObject()
    {
        return mFieldValue.getValueAsDoubleObject();
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
        return mFieldValue.getValue(anOffset);
    }

    /**
     * Returns the field value as an <i>Integer</i> type.
     *
     * @return Value of the field.
     */
    public Integer getValueAsIntegerObject()
    {
        return mFieldValue.getValueAsIntegerObject();
    }

    /**
     * Assigns the value parameter to the field.
     *
     * @param aValue Value to assign.
     */
    public void setValue(Date aValue)
    {
        mFieldValue.setValue(aValue);
    }

    /**
     * Returns <i>true</i> if the field represents a multi-value or
     * <i>false</i> otherwise.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isMultiValue()
    {
        return mFieldValue.isMultiValue();
    }

    /**
     * Returns the count of values in the list.
     *
     * @return Count of values in the list.
     */
    public int valueCount()
    {
        return mFieldValue.count();
    }

    /**
     * Returns <i>false</i> if the field value evaluates as false or
     * <i>true</i> otherwise.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isValueFalse()
    {
        return mFieldValue.isValueFalse();
    }

    /**
     * Returns the Date field value as a <i>long</i> type.
     *
     * @return Value of the field.
     */
    public long getValueAsDateLong()
    {
        return mFieldValue.getValueAsDateLong();
    }

    /**
     * Returns the field value as a <i>long</i> type.
     *
     * @return Value of the field.
     */
    public long getValueAsLong()
    {
        return mFieldValue.getValueAsLong();
    }

    /**
     * Returns the field value as a <i>Float</i> type.
     *
     * @return Value of the field.
     */
    public Float getValueAsFloatObject()
    {
        return mFieldValue.getValueAsFloatObject();
    }

    /**
     * Returns a hidden (e.g. encrypted) <i>String</i> representation of
     * the field value.
     * <p>
     * <b>Note:</b> The method uses a simple rot13 algorithm to transform
     * the value.  It is meant only to obscure the field value.
     * </p>
     *
     * @return <i>String</i> representation of the field value.
     */
    public String getValueAsHidden()
    {
        return mFieldValue.getValueAsHidden();
    }

    /**
     * Returns <i>true</i> if the field is empty or <i>false</i> otherwise.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isValueEmpty()
    {
        return mFieldValue.isValueEmpty();
    }

    /**
     * Returns a <i>String</i> representation of the field value.
     *
     * @return <i>String</i> representation of the field value.
     */
    public String getValueAsString()
    {
        return mFieldValue.getValueAsString();
    }

    /**
     * Returns <i>true</i> if the field is not empty or <i>false</i> otherwise.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isValueNotEmpty()
    {
        return mFieldValue.isValueNotEmpty();
    }

    /**
     * Assigns the values parameter to the field.
     *
     * @param aValues An array of String values.
     */
    public void setValues(ArrayList<String> aValues)
    {
        mFieldValue.setValues(aValues);
    }

    /**
     * Assigns the values parameter to the field.
     *
     * @param aValues An array of String values.
     */
    public void setValues(String... aValues)
    {
        mFieldValue.setValues(aValues);
    }

    /**
     * Returns a <i>String</i> representation of the field value.
     *
     * @return <i>String</i> representation of the field value.
     */
    public String getValue()
    {
        return mFieldValue.getValue();
    }

    /**
     * Returns <i>true</i> if the field value evaluates as true or
     * <i>false</i> otherwise.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isValueTrue()
    {
        return mFieldValue.isValueTrue();
    }

    /**
     * Assigns the value parameter to the list at the offset specified.
     *  @param anOffset Offset of value in the list to replace.
     * @param aValue A value that is formatted appropriately for
     */
    public void setValue(int anOffset, String aValue)
    {
        mFieldValue.setValue(anOffset, aValue);
    }

    /**
     * Returns a formatted <i>String</i> representation of the field value based
     * on the mask parameter.
     *
     * @param aFormatMask Format mask string. Refer to {@link Field} for examples.
     *
     * @return Formatted <i>String</i> representation of the field value.
     */
    public String getValueFormatted(String aFormatMask)
    {
        String dataValue = getValueAsString();
        if (StringUtils.isNotEmpty(dataValue))
        {
            if (StringUtils.isNotEmpty(aFormatMask))
            {
                if (isTypeNumber())
                {
                    double dblValue = getValueAsDouble();
                    DecimalFormat dblFormat = new DecimalFormat(aFormatMask);
                    return dblFormat.format(dblValue);
                }
                else if (isTypeDateOrTime())
                {
                    Date valueDate = getValueAsDate();
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat(aFormatMask);
                    return simpleDateFormat.format(valueDate.getTime());
                }
            }
        }
        return dataValue;
    }

    /**
     * Returns the sort order preference of the field.
     *
     * @return Sort order preference.
     */
    public Field.Order getSortOrder()
    {
        return mSortOrder;
    }

    /**
     * Assigns a sort order preference to the field.
     *
     * @param aSortOrder Sort order.
     */
    public void setSortOrder(Field.Order aSortOrder)
    {
        mSortOrder = aSortOrder;
    }

    /**
     * Returns <i>true</i> if the field is sorted or <i>false</i> otherwise.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isSorted()
    {
        return mSortOrder != Field.Order.UNDEFINED;
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
        mFieldValue.expand(aValue, aDelimiterChar);
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
        return mFieldValue.collapse(aDelimiterChar);
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
        return mFieldValue.collapse();
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
        mFieldValue.expand(aValue);
    }

    /**
     * Copies the default value (if one was provided) to the field
     * value property.
     */
    public void assignValueFromDefault()
    {
        mFieldValue.assignValueFromDefault();
    }

    /**
     * Returns the default value property of the field.
     *
     * @return A value or empty string (if unassigned).
     */
    public String getDefaultValue()
    {
        return mFieldValue.getDefaultValue();
    }

    /**
     * Returns the default value property of the field as a <i>Date</i>
     * object instance.  The date is derived from the value using the
     * format mask of <code>Field.FORMAT_DATETIME_DEFAULT</code>.
     *
     * @return A {@link java.util.Date} reflecting the default value.
     */
    public Date getDefaultValueAsDate()
    {
        return mFieldValue.getDefaultValueAsDate();
    }

    /**
     * Assigns a default value property to the field.
     *
     * @param aValue Default value to assign.
     */
    public void setDefaultValue(double aValue)
    {
        mFieldValue.setDefaultValue(aValue);
    }

    /**
     * Assigns a default value property to the field.
     *
     * @param aValue Default value to assign.
     */
    public void setDefaultValue(float aValue)
    {
        mFieldValue.setDefaultValue(aValue);
    }

    /**
     * Assigns a default value property to the field.
     *
     * @param aValue Default value to assign.
     */
    public void setDefaultValue(long aValue)
    {
        mFieldValue.setDefaultValue(aValue);
    }

    /**
     * Assigns a default value property to the field.
     *
     * @param aValue Default value to assign.
     */
    public void setDefaultValue(int aValue)
    {
        mFieldValue.setDefaultValue(aValue);
    }

    /**
     * Assigns a default value property to the field.
     *
     * @param aValue Default value to assign.
     */
    public void setDefaultValue(Date aValue)
    {
        mFieldValue.setDefaultValue(aValue);
    }

    /**
     * Assigns a default value property to the field.
     *
     * @param aDefaultValue Default value to assign.
     */
    public void setDefaultValue(String aDefaultValue)
    {
        mFieldValue.setDefaultValue(aDefaultValue);
    }

    /**
     * Assigns a default value property to the field.
     *
     * @param aValue Default value to assign.
     */
    public void setDefaultValue(boolean aValue)
    {
        mFieldValue.setDefaultValue(aValue);
    }

    /**
     * Convenience method that returns the field value as an <i>Object</i> type.
     * For example, if your field represents an <i>int</i> value, then the method
     * will return an object representing an <i>Integer</i>.
     *
     * @return Object instance representation of the field.
     */
    public Object getValueAsObject()
    {
        switch (mType)
        {
            case Integer:
                return mFieldValue.getValueAsIntegerObject();
            case Long:
                return mFieldValue.getValueAsLongObject();
            case Float:
                return mFieldValue.getValueAsFloatObject();
            case Double:
                return mFieldValue.getValueAsDoubleObject();
            case Boolean:
                return mFieldValue.getValueAsBooleanObject();
            case Date:
            case Time:
            case DateTime:
                return mFieldValue.getValueAsDate();
            default:
                if (mFieldValue.isMultiValue())
                    return mFieldValue.collapse();
                else
                    return mFieldValue.getValue();
        }
    }

    /**
     * Convenience method that returns the field value as an <i>Object</i> type.
     * For example, if your field represents an <i>int</i> value, then the method
     * will return an object representing an <i>Integer</i>.
     *
     * @param aDelimiterChar Delimiter character.
     *
     * @return Object instance representation of the field.
     */
    public Object getValueAsObject(char aDelimiterChar)
    {
        switch (mType)
        {
            case Integer:
                return mFieldValue.getValueAsIntegerObject();
            case Long:
                return mFieldValue.getValueAsLongObject();
            case Float:
                return mFieldValue.getValueAsFloatObject();
            case Double:
                return mFieldValue.getValueAsDoubleObject();
            case Boolean:
                return mFieldValue.getValueAsBooleanObject();
            case Date:
            case Time:
            case DateTime:
                return mFieldValue.getValueAsDate();
            default:
                if (mFieldValue.isMultiValue())
                    return mFieldValue.collapse(aDelimiterChar);
                else
                    return mFieldValue.getValue();
        }
    }

    /**
     * Returns <i>true</i> if the field value of the <i>aField</i>
     * parameter matches the current value of this field.  The
     * comparison is done via {@link StringUtils}.equals() method.
     * If the comparison fails, the it returns <i>false</i>.
     *
     * @param aField Data field to compare with.
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isValueEqual(DataField aField)
    {
        if (aField != null)
        {
            String value1 = collapse();
            String value2 = aField.collapse();
            if (StringUtils.equals(value1, value2))
                return true;
        }

        return false;
    }

    /**
     * Returns <i>true</i> if the name and value of the <i>aField</i>
     * parameter matches the current value of this field.  The
     * comparison is done via {@link StringUtils}.equals() method.
     * If the comparison fails, the it returns <i>false</i>.
     *
     * @param aField Complex field to compare with.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isEqual(DataField aField)
    {
        return StringUtils.equals(aField.getName(), getName()) &&
               isValueEqual(aField);
    }

    /**
     * Returns <i>true</i> if this field does not have FEATURE_IS_HIDDEN and
     * FEATURE_IS_VISIBLE assigned as true.  Otherwise, it will return false
     * to signify that this field should not be displayed to the user.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isDisplayable()
    {
        if ((isFeatureAssigned(Field.FEATURE_IS_VISIBLE)) && (isFeatureFalse(Field.FEATURE_IS_VISIBLE)))
            return false;
        if ((isFeatureAssigned(Field.FEATURE_IS_HIDDEN)) && (isFeatureFalse(Field.FEATURE_IS_HIDDEN)))
            return false;

        return true;
    }

    /**
     * Returns <i>true</i> if the field value is valid or <i>false</i> otherwise.
     * A validation check ensures values are assigned when required and do not
     * exceed range limits (if assigned).
     * <p>
     * <b>Note:</b> If a field fails the validation check, then a property called
     * <i>Field.VALIDATION_PROPERTY_NAME</i> will be assigned a relevant message.
     * </p>
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isValid()
    {
        if (! isFeatureTrue(Field.FEATURE_IS_HIDDEN))
        {
            if ((isFeatureTrue(Field.FEATURE_IS_REQUIRED)) && (StringUtils.isEmpty(getValue())))
            {
                addProperty(Field.VALIDATION_PROPERTY_NAME, Field.VALIDATION_MESSAGE_IS_REQUIRED);
                return false;
            }
            else if (isRangeAssigned())
            {
                if (! mRange.isValid(getValue()))
                {
                    addProperty(Field.VALIDATION_PROPERTY_NAME, Field.VALIDATION_MESSAGE_OUT_OF_RANGE);
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * Add a unique feature to this field.  A feature enhances the core
     * capability of the field.  Standard features are listed below.
     *
     * <ul>
     *     <li>Field.FEATURE_IS_PRIMARY_KEY</li>
     *     <li>Field.FEATURE_IS_VISIBLE</li>
     *     <li>Field.FEATURE_IS_REQUIRED</li>
     *     <li>Field.FEATURE_IS_UNIQUE</li>
     *     <li>Field.FEATURE_IS_INDEXED</li>
     *     <li>Field.FEATURE_IS_STORED</li>
     *     <li>Field.FEATURE_IS_SECRET</li>
     *     <li>Field.FEATURE_TYPE_ID</li>
     *     <li>Field.FEATURE_INDEX_TYPE</li>
     *     <li>Field.FEATURE_STORED_SIZE</li>
     *     <li>Field.FEATURE_INDEX_POLICY</li>
     *     <li>Field.FEATURE_FUNCTION_NAME</li>
     *     <li>Field.FEATURE_SEQUENCE_SEED</li>
     *     <li>Field.FEATURE_SEQUENCE_INCREMENT</li>
     *     <li>Field.FEATURE_SEQUENCE_MANAGEMENT</li>
     * </ul>
     *
     * @param aName Name of the feature.
     * @param aValue Value to associate with the feature.
     */
    public void addFeature(String aName, String aValue)
    {
        mFeatures.put(aName, aValue);
    }

    /**
     * Add a unique feature to this field.  A feature enhances the core
     * capability of the field.
     *
     * @param aName Name of the feature.
     * @param aValue Value to associate with the feature.
     */
    public void addFeature(String aName, int aValue)
    {
        addFeature(aName, Integer.toString(aValue));
    }

    /**
     * Enabling the feature will add the name and assign it a
     * value of <i>StrUtl.STRING_TRUE</i>.
     *
     * @param aName Name of the feature.
     */
    public void enableFeature(String aName)
    {
        mFeatures.put(aName, StrUtl.STRING_TRUE);
    }

    /**
     * Disabling a feature will remove its name and value
     * from the internal list.
     *
     * @param aName Name of feature.
     */
    public void disableFeature(String aName)
    {
        mFeatures.remove(aName);
    }

    /**
     * Returns <i>true</i> if the feature was previously
     * added and assigned a value.
     *
     * @param aName Name of feature.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isFeatureAssigned(String aName)
    {
        return (getFeature(aName) != null);
    }

    /**
     * Returns <i>true</i> if the feature was previously
     * added and assigned a value of <i>StrUtl.STRING_TRUE</i>.
     *
     * @param aName Name of feature.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isFeatureTrue(String aName)
    {
        return StrUtl.stringToBoolean(mFeatures.get(aName));
    }

    /**
     * Returns <i>true</i> if the feature was previously
     * added and not assigned a value of <i>StrUtl.STRING_TRUE</i>.
     *
     * @param aName Name of feature.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isFeatureFalse(String aName)
    {
        return !StrUtl.stringToBoolean(mFeatures.get(aName));
    }

    /**
     * Returns <i>true</i> if the feature was previously
     * added and its value matches the one provided as a
     * parameter.
     *
     * @param aName Feature name.
     * @param aValue Feature value to match.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isFeatureEqual(String aName, String aValue)
    {
        String featureValue = getFeature(aName);
        return StringUtils.equalsIgnoreCase(featureValue, aValue);
    }

    /**
     * Count of unique features assigned to this field.
     *
     * @return Feature count.
     */
    public int featureCount()
    {
        return mFeatures.size();
    }

    /**
     * Returns the String associated with the feature name or
     * <i>null</i> if the name could not be found.
     *
     * @param aName Feature name.
     *
     * @return Feature value or <i>null</i>
     */
    public String getFeature(String aName)
    {
        return mFeatures.get(aName);
    }

    /**
     * Returns the int associated with the feature name.
     *
     * @param aName Feature name.
     *
     * @return Feature value or <i>null</i>
     */
    public int getFeatureAsInt(String aName)
    {
        return Field.createInt(getFeature(aName));
    }

    /**
     * Removes all features assigned to this object instance.
     */
    public void clearFeatures()
    {
        mFeatures.clear();
    }

    /**
     * Returns a read-only copy of the internal map containing
     * feature list.
     *
     * @return Internal feature map instance.
     */
    public final HashMap<String, String> getFeatures()
    {
        return mFeatures;
    }

    /**
     * Add an application defined property to the field.
     * <p>
     * <b>Notes:</b>
     * The goal of the DataField is to strike a balance
     * between providing enough properties to adequately model content
     * meta data without overloading it with too many members.  This method
     * offers a mechanism to capture additional ones that may be needed.
     * </p>
     * Properties added with this method are transient and will not be
     * persisted when saved.
     *
     * @param aName Property name (duplicates are not supported).
     * @param anObject Instance of an object.
     */
    public void addProperty(String aName, Object anObject)
    {
        if (mProperties == null)
            mProperties = new HashMap<String, Object>();
        mProperties.put(aName, anObject);
    }

    /**
     * Returns the object associated with the property name or
     * <i>null</i> if the name could not be matched.
     *
     * @param aName Name of the property.
     * @return Instance of an object.
     */
    public Object getProperty(String aName)
    {
        if (mProperties == null)
            return null;
        else
            return mProperties.get(aName);
    }

    /**
     * Removes all application defined properties assigned to this field.
     */
    public void clearProperties()
    {
        if (mProperties != null)
            mProperties.clear();
    }

    @Override
    public boolean equals(Object anObject)
    {
        if (this == anObject)
            return true;
        if (anObject == null || getClass() != anObject.getClass())
            return false;

        DataField dataField = (DataField) anObject;

        return isEqual(dataField);

    }

    /**
     * Returns a hash code value for the object. This method is
     * supported for the benefit of hash tables such as those provided by
     * {@link java.util.HashMap}.
     *
     * @return A hash code value for this object.
     */
    @Override
    public int hashCode()
    {
        int result = mDisplaySize;
        result = 31 * result + (mFieldValue != null ? mFieldValue.hashCode() : 0);
        result = 31 * result + mName.hashCode();
        result = 31 * result + (mFeatures != null ? mFeatures.hashCode() : 0);
        result = 31 * result + mTitle.hashCode();
        result = 31 * result + (mType != null ? mType.hashCode() : 0);
        return result;
    }
}
