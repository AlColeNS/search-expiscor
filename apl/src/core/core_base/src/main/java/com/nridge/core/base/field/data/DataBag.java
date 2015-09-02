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
import com.nridge.core.base.std.DigitalHash;
import com.nridge.core.base.std.StrUtl;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.*;
import java.util.zip.CRC32;

/**
 * A DataBag is an unordered collection of {@link DataField}s.  Use this
 * class when you need to model rich meta data information. In addition,
 * you can specify also specify a sort order for data sources to utilize
 * during fetch operations.
 * <p>
 * This framework provides a number of helper classes that accept a DataBag
 * for IO operations.
 * </p>
 * You can also use SQLBag to model information that needs to be
 * stored in an RDBMS.
 *
 * @author Al Cole
 * @since 1.0
 */
public class DataBag
{
    private long mTypeId;
    private ArrayList<DataField> mFields;
    private String mName = StringUtils.EMPTY;
    private String mTitle = StringUtils.EMPTY;
    private HashMap<String, String> mFeatures;
    private transient HashMap<String, Object> mProperties;

    /**
     * Default constructor.
     */
    public DataBag()
    {
        mFields = new ArrayList<DataField>();
        mFeatures = new HashMap<String, String>();
    }

    /**
     * Constructor accepts a bag name parameter and initializes the DataBag.
     *
     * @param aName Name of bag.
     */
    public DataBag(String aName)
    {
        mFields = new ArrayList<DataField>();
        mFeatures = new HashMap<String, String>();
        setName(aName);
    }

    /**
     * Constructor accepts a bag name and title parameter and initializes
     * the DataBag.
     *
     * @param aName Name of the bag.
     * @param aTitle Title of the bag.
     */
    public DataBag(String aName, String aTitle)
    {
        mFields = new ArrayList<DataField>();
        mFeatures = new HashMap<String, String>();
        setName(aName);
        setTitle(aTitle);
    }

    /**
     * Constructor clones an existing DataBag (including all of its fields).
     *
     * @param aBag Source bag instance to clone.
     */
    public DataBag(final DataBag aBag)
    {
        DataField newField;

        if (aBag != null)
        {
            setName(aBag.getName());
            setTitle(aBag.getTitle());
            setTypeId(aBag.getTypeId());
            mFields = new ArrayList<DataField>();

            for (DataField curField : aBag.getFields())
            {
                newField = new DataField(curField);
                add(newField);
            }
            this.mFeatures = new HashMap<String, String>(aBag.getFeatures());
            // Ignoring mProperties
        }
    }

    /**
     * Creates a new instance of the data bag based on the
     * one currently populated.  The new instance will contain
     * only those fields that have been populated from previous
     * assignments.  This can be helpful when you start with a
     * schema bag containing many possible fields that never
     * end up getting populated in a load operation.
     *
     * @return Data bag instance.
     */
    public DataBag collapseUnusedFields()
    {
        DataBag newBag;
        DataField newField;

        if (StringUtils.isEmpty(mName))
            newBag = new DataBag();
        else
            newBag = new DataBag(mName);
        newBag.setFeatures(getFeatures());
        for (DataField curField : getFields())
        {
            if (curField.isAssigned())
            {
                newField = new DataField(curField);
                newBag.add(newField);
            }
        }

        return newBag;
    }

    /**
     * Returns a string summary representation of a DataBag.
     *
     * @return String summary representation of this DataBag.
     */
    @Override
    public String toString()
    {
        String idName;

        if (StringUtils.isNotEmpty(mTitle))
            idName = mTitle;
        else
            idName = mName;
        return String.format("%s [%d fields]", idName, count());
    }

    /**
     * Returns an {@link ArrayList} of fields to aid in the
     * process of iterating through the bag of fields.
     *
     * <code>
     *     for (DataField DataField : DataBag.getFields())
     *          // Process field
     * </code>
     *
     * @return An array of fields.
     */
    public ArrayList<DataField> getFields()
    {
        return mFields;
    }

    /**
     * Returns the name of the bag.
     *
     * @return Bag name.
     */
    public String getName()
    {
        return mName;
    }

    /**
     * Assigns the name of the bag.
     *
     * @param aName Bag name.
     */
    public void setName(String aName)
    {
        mName = aName;
    }

    /**
     * Returns the title (or label) of the bag.
     *
     * @return A title or empty string (if unassigned).
     */
    public String getTitle()
    {
        return mTitle;
    }

    /**
     * Assigns a title (or label) property to the bag.
     *
     * @param aTitle Bag title.
     */
    public void setTitle(String aTitle)
    {
        mTitle = aTitle;
    }

    /**
     * Returns the type identification value of the bag.
     *
     * @return Type id value.
     */
    public long getTypeId()
    {
        return mTypeId;
    }

    /**
     * Assigns the id parameter to the type id property.  A type id can
     * be used to group a collection of fields (e.g. RDBMS table).
     *
     * @param aTypeId Type id value.
     */
    public void setTypeId(long aTypeId)
    {
        mTypeId = aTypeId;
    }

    /**
     * Convenience method will calculate a unique type id property for
     * the bag based on each field name using a CRC32 algorithm.
     */
    public void setTypeIdByNames()
    {
        CRC32 crc32 = new CRC32();
        crc32.reset();
        if (StringUtils.isNotEmpty(mName))
            crc32.update(mName.getBytes());
        else
        {
            for (DataField dataField : mFields)
                crc32.update(dataField.getName().getBytes());
        }
        setTypeId(crc32.getValue());
    }

    /**
     * Adds the DataField parameter to the bag.
     *
     * @param aField A field.
     */
    public void add(DataField aField)
    {
        mFields.add(aField);
    }

    /**
     * Convenience method that creates a DataField based on the
     * parameters and adds it to the bag.
     *
     * @param aType Type of field.
     * @param aName Name of field.
     * @param aTitle Title of the field.
     */
    public void add(Field.Type aType, String aName, String aTitle)
    {
        mFields.add(new DataField(aType, aName, aTitle));
    }

    /**
     * Convenience method that creates a DataField based on the
     * parameters and adds it to the bag.
     *
     * @param aType Type of field.
     * @param aName Name of field.
     * @param aTitle Title of the field.
     * @param aValue Value of the field.
     */
    public void add(Field.Type aType, String aName, String aTitle, String aValue)
    {
        mFields.add(new DataField(aType, aName, aTitle, aValue));
    }

    /**
     * Convenience method that creates a DataField based on the
     * parameters and adds it to the bag.  By default, the field
     * will be assigned a data type of <i>Text</i>.
     *
     * @param aName Name of field.
     */
    public void add(String aName)
    {
        mFields.add(new DataTextField(aName));
    }

    /**
     * Convenience method that creates a DataField based on the
     * parameters and adds it to the bag. By default, the field
     * will be assigned a data type of <i>Text</i>.
     *
     * @param aName Name of field.
     * @param aTitle Title of the field.
     */
    public void add(String aName, String aTitle)
    {
        mFields.add(new DataTextField(aName, aTitle));
    }

    /**
     * Convenience method that creates a DataField based on the
     * parameters and adds it to the bag. By default, the field
     * will be assigned a data type of <i>Text</i>.
     *
     * @param aName Name of field.
     * @param aTitle Title of the field.
     * @param aValue Value of the field.
     */
    public void add(String aName, String aTitle, String aValue)
    {
        mFields.add(new DataTextField(aName, aTitle, aValue));
    }

    /**
     * Convenience method that creates a DataField based on the
     * parameters and adds it to the bag. By default, the field
     * will be assigned a data type of <i>Integer</i>.
     *
     * @param aName Name of field.
     * @param aTitle Title of the field.
     * @param aValue Value of the field.
     */
    public void add(String aName, String aTitle, int aValue)
    {
        mFields.add(new DataField(aName, aTitle, aValue));
    }

    /**
     * Convenience method that creates a DataField based on the
     * parameters and adds it to the bag. By default, the field
     * will be assigned a data type of <i>Long</i>.
     *
     * @param aName Name of field.
     * @param aTitle Title of the field.
     * @param aValue Value of the field.
     */
    public void add(String aName, String aTitle, long aValue)
    {
        mFields.add(new DataField(aName, aTitle, aValue));
    }

    /**
     * Convenience method that creates a DataField based on the
     * parameters and adds it to the bag. By default, the field
     * will be assigned a data type of <i>Float</i>.
     *
     * @param aName Name of field.
     * @param aTitle Title of the field.
     * @param aValue Value of the field.
     */
    public void add(String aName, String aTitle, float aValue)
    {
        mFields.add(new DataField(aName, aTitle, aValue));
    }

    /**
     * Convenience method that creates a DataField based on the
     * parameters and adds it to the bag. By default, the field
     * will be assigned a data type of <i>Double</i>.
     *
     * @param aName Name of field.
     * @param aTitle Title of the field.
     * @param aValue Value of the field.
     */
    public void add(String aName, String aTitle, double aValue)
    {
        mFields.add(new DataField(aName, aTitle, aValue));
    }

    /**
     * Convenience method that creates a DataField based on the
     * parameters and adds it to the bag. By default, the field
     * will be assigned a data type of <i>Boolean</i>.
     *
     * @param aName Name of field.
     * @param aTitle Title of the field.
     * @param aValue Value of the field.
     */
    public void add(String aName, String aTitle, boolean aValue)
    {
        mFields.add(new DataField(aName, aTitle, aValue));
    }

    /**
     * Convenience method that creates a DataField based on the
     * parameters and adds it to the bag. By default, the field
     * will be assigned a data type of <i>Date</i>.
     *
     * @param aName Name of field.
     * @param aTitle Title of the field.
     * @param aValue Value of the field.
     */
    public void add(String aName, String aTitle, Date aValue)
    {
        mFields.add(new DataField(aName, aTitle, aValue));
    }

    /**
     * Returns the count of fields in this bag.
     *
     * @return Count of fields.
     */
    public int count()
    {
        if (mFields == null)
            return 0;
        else
            return mFields.size();
    }

    /**
     * Returns a field based on the offset into the bag.
     *
     * @param anOffset Offset into bag collection (starts at zero)
     * @return Field at the offset or null if it is out of bounds.
     */
    public DataField getByOffset(int anOffset)
    {
        return mFields.get(anOffset);
    }

    /**
     * Returns the offset of the field in the bag collection that
     * matches the field name parameter.
     *
     * @param aName Field name.
     * @return Offset value or -1 if not found.
     */
    public int getOffsetByName(String aName)
    {
        int offset = 0;
        for (DataField dataField : mFields)
        {
            if (dataField.getName().equals(aName))
                return offset;
            else
                offset++;
        }

        return -1;
    }

    /**
     * Returns the offset of the field in the bag collection that
     * matches the field name parameter (case insensitive).
     *
     * @param aName Field name.
     *
     * @return Offset value or -1 if not found.
     */
    public int getOffsetByNameIgnoreCase(String aName)
    {
        int offset = 0;
        for (DataField dataField : mFields)
        {
            if (StringUtils.equalsIgnoreCase(dataField.getName(), aName))
                return offset;
            else
                offset++;
        }

        return -1;
    }

    /**
     * Convenience method will identify a list of fields that has a feature
     * matching the parameter name.  If non are found, then list will be
     * empty.
     *
     * @param aName Feature name.
     *
     * @return List of data fields.
     */
    public ArrayList<DataField> getFieldByFeatureName(String aName)
    {
        ArrayList<DataField> dataFieldList = new ArrayList<>();

        for (DataField dataField : mFields)
        {
            if (StringUtils.isNotEmpty(dataField.getFeature(aName)))
                dataFieldList.add(dataField);
        }

        return dataFieldList;
    }

    /**
     * Convenience method will identify the first field that has a feature
     * matching the parameter name.  If non are found, then <i>null</i> is
     * returned.
     *
     * @param aName Feature name.
     *
     * @return The first data field or <i>null</i>.
     */
    public DataField getFirstFieldByFeatureName(String aName)
    {
        for (DataField dataField : mFields)
        {
            if (StringUtils.isNotEmpty(dataField.getFeature(aName)))
                return dataField;
        }
        return null;
    }

    /**
     * Convenience method will identify which field represents the primary
     * key in the bag collection.  If non are found, then <i>null</i> is
     * returned.
     *
     * @return The primary key field or <i>null</i>.
     */
    public DataField getPrimaryKeyField()
    {
        for (DataField dataField : mFields)
        {
            if (dataField.isFeatureTrue(Field.FEATURE_IS_PRIMARY_KEY))
                return dataField;
        }
        return null;
    }

    /**
     * Convenience method that constructs a {@link HashMap} based on the
     * field name (key) and DataField (value).
     *
     * @return HashMap of DataFields.
     */
    public HashMap<String, DataField> getHashMapNameKey()
    {
        HashMap<String, DataField> hmFields = new HashMap<String, DataField>();

        for (DataField dataField : mFields)
            hmFields.put(dataField.getName(), dataField);

        return hmFields;
    }

    /**
     * Returns the field matching the name parameter or <i>null</i>
     * otherwise.
     *
     * @param aName Field name.
     * @return DataField or <i>null</i> if unmatched.
     */
    public DataField getFieldByName(String aName)
    {
        for (DataField dataField : mFields)
        {
            if (dataField.getName().equals(aName))
                return dataField;
        }

        return null;
    }

    /**
     * Returns the field matching the name parameter (case insensitive)
     * or <i>null</i> otherwise.
     *
     * @param aName Field name.
     * @return DataField or <i>null</i> if unmatched.
     */
    public DataField getByNameIgnoreCase(String aName)
    {
        for (DataField dataField : mFields)
        {
            if (dataField.getName().equalsIgnoreCase(aName))
                return dataField;
        }

        return null;
    }

    /**
     * Returns the field matching the name parameter or <i>null</i>
     * otherwise.
     *
     * @param aName Field title.
     * @return DataField or <i>null</i> if unmatched.
     */
    public DataField getFieldByTitle(String aName)
    {
        for (DataField dataField : mFields)
        {
            if (dataField.getTitle().equals(aName))
                return dataField;
        }

        return null;
    }

    /**
     * Returns the field matching the name parameter (case insensitive)
     * or <i>null</i> otherwise.
     *
     * @param aName Field title.
     * @return DataField or <i>null</i> if unmatched.
     */
    public DataField getByTitleIgnoreCase(String aName)
    {
        for (DataField dataField : mFields)
        {
            if (dataField.getTitle().equalsIgnoreCase(aName))
                return dataField;
        }

        return null;
    }

    /**
     * Removes the field from the bag collection that matches the
     * name parameter.
     *
     * @param aName Field name.
     */
    public void remove(String aName)
    {
        DataField dataField = getFieldByName(aName);
        if (dataField != null)
            mFields.remove(dataField);
    }

    /**
     * Assigns the value parameter to the field matching the
     * name parameter.
     *
     * @param aName Field name.
     * @param aValue Field value to assign.
     */
    public void setValueByName(String aName, String aValue)
    {
        DataField dataField = getFieldByName(aName);
        if (dataField != null)
            dataField.setValue(aValue);
    }

    /**
     * Assigns the value parameter to the field matching the
     * name parameter.
     *
     * @param aName Field name.
     * @param aValue Field value to assign.
     */
    public void setValueByName(String aName, int aValue)
    {
        DataField dataField = getFieldByName(aName);
        if (dataField != null)
            dataField.setValue(aValue);
    }

    /**
     * Assigns the value parameter to the field matching the
     * name parameter.
     *
     * @param aName Field name.
     * @param aValue Field value to assign.
     */
    public void setValueByName(String aName, long aValue)
    {
        DataField dataField = getFieldByName(aName);
        if (dataField != null)
            dataField.setValue(aValue);
    }

    /**
     * Assigns the value parameter to the field matching the
     * name parameter.
     *
     * @param aName Field name.
     * @param aValue Field value to assign.
     */
    public void setValueByName(String aName, float aValue)
    {
        DataField dataField = getFieldByName(aName);
        if (dataField != null)
            dataField.setValue(aValue);
    }

    /**
     * Assigns the value parameter to the field matching the
     * name parameter.
     *
     * @param aName Field name.
     * @param aValue Field value to assign.
     */
    public void setValueByName(String aName, double aValue)
    {
        DataField dataField = getFieldByName(aName);
        if (dataField != null)
            dataField.setValue(aValue);
    }

    /**
     * Assigns the value parameter to the field matching the
     * name parameter.
     *
     * @param aName Field name.
     * @param aValue Field value to assign.
     */
    public void setValueByName(String aName, boolean aValue)
    {
        DataField dataField = getFieldByName(aName);
        if (dataField != null)
            dataField.setValue(aValue);
    }

    /**
     * Assigns the value parameter to the field matching the
     * name parameter.
     *
     * @param aName Field name.
     * @param aValue Field value to assign.
     */
    public void setValueByName(String aName, Date aValue)
    {
        DataField dataField = getFieldByName(aName);
        if (dataField != null)
            dataField.setValue(aValue);
    }



    /**
     * Returns the value of the field matching the name parameter.
     *
     * @param aName Field name.
     * @return The value of the matching field or an empty string.
     */
    public String getValueAsString(String aName)
    {
        DataField dataField = getFieldByName(aName);
        if (dataField == null)
            return StringUtils.EMPTY;
        else
            return dataField.getValueAsString();
    }

    /**
     * Returns value of the field matching the name parameter in a format
     * matching the mask parameter.
     *
     * @param aName Field name.
     * @param aFormatMask Format mask string. Refer to {@link Field} for examples.
     * @return Formatted <i>String</i> representation of the field value.
     */
    public String getValueFormatted(String aName, String aFormatMask)
    {
        DataField dataField = getFieldByName(aName);
        if (dataField == null)
            return StringUtils.EMPTY;
        else
            return dataField.getValueFormatted(aFormatMask);
    }

    /**
     * Returns the value of the field matching the name parameter
     * as an <i>int</i> type.
     *
     * @param aName Field name.
     * @return The value of the matching field or -1.
     */
    public int getValueAsInt(String aName)
    {
        DataField dataField = getFieldByName(aName);
        if (dataField == null)
            return -1;
        else
            return dataField.getValueAsInt();
    }

    /**
     * Returns the value of the field matching the name parameter
     * as an <i>Integer</i> type.
     *
     * @param aName Field name.
     * @return The value of the matching field or <i>null</i>.
     */
    public Integer getValueAsIntegerObject(String aName)
    {
        DataField dataField = getFieldByName(aName);
        if (dataField == null)
            return null;
        else
            return dataField.getValueAsIntegerObject();
    }

    /**
     * Returns the value of the field matching the name parameter
     * as a <i>long</i> type.
     *
     * @param aName Field name.
     * @return The value of the matching field or -1.
     */
    public long getValueAsLong(String aName)
    {
        DataField dataField = getFieldByName(aName);
        if (dataField == null)
            return -1L;
        else
            return dataField.getValueAsLong();
    }

    /**
     * Returns the value of the field matching the name parameter
     * as a <i>Long</i> type.
     *
     * @param aName Field name.
     * @return The value of the matching field or <i>null</i>.
     */
    public Long getValueAsLongObject(String aName)
    {
        DataField dataField = getFieldByName(aName);
        if (dataField == null)
            return null;
        else
            return dataField.getValueAsLongObject();
    }

    /**
     * Returns the value of the field matching the name parameter
     * as a <i>float</i> type.
     *
     * @param aName Field name.
     * @return The value of the matching field or -1.
     */
    public float getValueAsFloat(String aName)
    {
        DataField dataField = getFieldByName(aName);
        if (dataField == null)
            return -1.0F;
        else
            return dataField.getValueAsFloat();
    }

    /**
     * Returns the value of the field matching the name parameter
     * as a <i>Float</i> type.
     *
     * @param aName Field name.
     * @return The value of the matching field or <i>null</i>.
     */
    public Float getValueAsFloatObject(String aName)
    {
        DataField dataField = getFieldByName(aName);
        if (dataField == null)
            return null;
        else
            return dataField.getValueAsFloatObject();
    }

    /**
     * Returns the value of the field matching the name parameter
     * as a <i>double</i> type.
     *
     * @param aName Field name.
     * @return The value of the matching field or -1.
     */
    public double getValueAsDouble(String aName)
    {
        DataField dataField = getFieldByName(aName);
        if (dataField == null)
            return -1.0;
        else
            return dataField.getValueAsDouble();
    }

    /**
     * Returns the value of the field matching the name parameter
     * as a <i>Double</i> type.
     *
     * @param aName Field name.
     * @return The value of the matching field or <i>null</i>.
     */
    public Double getValueAsDoubleObject(String aName)
    {
        DataField dataField = getFieldByName(aName);
        if (dataField == null)
            return null;
        else
            return dataField.getValueAsDoubleObject();
    }

    /**
     * Returns <i>true</i> if the value of the field matching the
     * name parameter matches a string representation of True or
     * False.
     *
     * @param aName Field name.
     *
     * @return The <i>true</i> or <i>false</i>.
     *
     */
    public boolean isValueTrue(String aName)
    {
        DataField dataField = getFieldByName(aName);
        if (dataField == null)
            return false;
        else
            return dataField.isValueTrue();
    }

    /**
     * Returns the value of the field matching the name parameter
     * as a <i>Boolean</i> type.
     *
     * @param aName Field name.
     * @return The value of the matching field or <i>null</i>.
     */
    public Boolean getValueAsBooleanObject(String aName)
    {
        DataField dataField = getFieldByName(aName);
        if (dataField == null)
            return null;
        else
            return dataField.getValueAsBooleanObject();
    }

    /**
     * Returns the value of the field matching the name parameter
     * as a <i>Date</i> type.
     *
     * @param aName Field name.
     * @return The value of the matching field or <i>null</i>.
     */
    public Date getValueAsDate(String aName)
    {
        DataField dataField = getFieldByName(aName);
        if (dataField == null)
            return null;
        else
            return dataField.getValueAsDate();
    }

    /**
     * Returns the value of the field matching the name parameter
     * as a <i>Date</i> type.  The format mask is used to convert
     * the string representation of the date.
     *
     * @param aName       Field name.
     * @param aFormatMask SimpleDateFormat mask.
     *
     * @return The value of the matching field or <i>null</i>.
     */
    public Date getValueAsDate(String aName, String aFormatMask)
    {
        DataField dataField = getFieldByName(aName);
        if (dataField == null)
            return null;
        else
            return Field.createDate(dataField.getValueAsString(), aFormatMask);
    }

    /**
     * Returns the value of the field matching the name parameter
     * as a <i>long</i> type.
     *
     * @param aName Field name.
     *
     * @return The value of the matching field or -1.
     */
    public long getValueAsDateLong(String aName)
    {
        DataField dataField = getFieldByName(aName);
        if (dataField == null)
            return -1L;
        else
            return dataField.getValueAsDateLong();
    }

    /**
     * Convenience method that locates a field by its name and returns
     * its display size value.
     *
     * @param aName Field name.
     *
     * @return Display size value or zero if unmatched.
     */
    public int getDisplaySize(String aName)
    {
        DataField dataField = getFieldByName(aName);
        if (dataField == null)
            return 0;
        else
            return dataField.getDisplaySize();
    }

    /**
     * Convenience method that locates a field by its name and returns
     * <i>true</i> if it is required or <i>false</i> otherwise.  Note
     * that an unmatched field will return <i>false</i>.
     *
     * @param aName Field name.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isRequired(String aName)
    {
        DataField dataField = getFieldByName(aName);
        if (dataField == null)
            return false;
        else
            return dataField.isFeatureTrue(Field.FEATURE_IS_REQUIRED);
    }

    /**
     * Convenience method that locates a field by its name and returns
     * <i>true</i> if it is visible or <i>false</i> otherwise.  Note
     * that an unmatched field will return <i>true</i>.
     *
     *  @param aName Field name.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isVisible(String aName)
    {
        return !isHidden(aName);
    }

    /**
     * Convenience method that locates a field by its name and returns
     * <i>true</i> if it is hidden or <i>false</i> otherwise.  Note
     * that an unmatched field will return <i>false</i>.
     *
     * @param aName Field name.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isHidden(String aName)
    {
        DataField dataField = getFieldByName(aName);
        if (dataField == null)
            return false;
        else
            return dataField.isFeatureTrue(Field.FEATURE_IS_HIDDEN);
    }

    /**
     * Convenience method that locates a field by its name and returns
     * its title or an empty string otherwise.
     *
     * @param aName Field name.
     *
     * @return Title of field or an empty string.
     */
    public String getTitle(String aName)
    {
        DataField dataField = getFieldByName(aName);
        if (dataField == null)
            return StringUtils.EMPTY;
        else
            return dataField.getTitle();
    }

    /**
     * Convenience method that locates a field by its name and returns
     * its default value or an empty string otherwise.
     *
     *  @param aName Field name.
     *
     * @return Default value of field or an empty string.
     */
    public String getDefaultValue(String aName)
    {
        DataField dataField = getFieldByName(aName);
        if (dataField == null)
            return StringUtils.EMPTY;
        else
            return dataField.getDefaultValue();
    }

    /**
     * Convenience method that assigns all fields within the bag to
     * the assignment flag parameter value.
     *
     * @param aFlag Assignment flag value.
     */
    public void setAssignedFlagAll(boolean aFlag)
    {
        for (DataField dataField : mFields)
            dataField.setAssignedFlag(aFlag);
    }

    /**
     * Convenience method that examines all of the fields in the bag
     * to determine if any of them are required to have a value
     * assigned to them.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isRequired()
    {
        for (DataField dataField : mFields)
        {
            if (dataField.isFeatureTrue(Field.FEATURE_IS_REQUIRED))
                return true;
        }

        return false;
    }

    /**
     * Returns the number of fields that are visible in the bag.
     *
     * @return Count of visible fields.
     */
    public int visibleCount()
    {
        int visibleCount = 0;

        for (DataField dataField : mFields)
        {
            if (dataField.isFeatureFalse(Field.FEATURE_IS_HIDDEN))
                visibleCount++;
        }

        return visibleCount;
    }

    /**
     * Returns the number of fields that are assigned in the bag.
     *
     * @return Count of assigned fields.
     */
    public int assignedCount()
    {
        int assignedCount = 0;

        for (DataField dataField : mFields)
        {
            if (dataField.isAssigned())
                assignedCount++;
        }

        return assignedCount;
    }

    /**
     * Convenience method that calculates the maximum length of field names
     * in the bag.
     *
     * @return Maximum length of all field names.
     */
    public int maxNameLength()
    {
        int maxFieldLength = 0;

        for (DataField dataField : mFields)
        {
            if (dataField.isFeatureFalse(Field.FEATURE_IS_HIDDEN))
                maxFieldLength = Math.max(maxFieldLength, dataField.getName().length());
        }

        return maxFieldLength;
    }

    /**
     * Convenience method that calculates the maximum length of field titles
     * in the bag.
     *
     * @return Maximum length of all field titles.
     */
    public int maxTitleLength()
    {
        int maxTitleLength = 0;

        for (DataField dataField : mFields)
        {
            if (dataField.isFeatureFalse(Field.FEATURE_IS_HIDDEN))
                maxTitleLength = Math.max(maxTitleLength, dataField.getTitle().length());
        }

        return maxTitleLength;
    }

    /**
     * Convenience method that clears all fields of sort order assignments.
     */
    public void clearSort()
    {
        for (DataField dataField : mFields)
            dataField.setSortOrder(Field.Order.UNDEFINED);
    }

    /**
     * Convenience method that resets the value of all fields to an empty string.
     */
    public void resetValues()
    {
        setAssignedFlagAll(false);
        for (DataField dataField : mFields)
            dataField.setValue(StringUtils.EMPTY);
    }

    /**
     * Convenience method that resets the value of all fields to either an
     * empty string or a default value.
     */
    public void resetValuesWithDefaults()
    {
        for (DataField dataField : mFields)
        {
            dataField.setValue(StringUtils.EMPTY);
            dataField.setAssignedFlag(false);
            dataField.assignValueFromDefault();
        }
    }

    /**
     * Convenience method examines all of the fields in the bag to determine if
     * they are valid. A validation check ensures values are assigned when required
     * and do not exceed range limits (if assigned).
     * <p>
     * <b>Note:</b> If a field fails the validation check, then a property called
     * <i>Field.VALIDATION_PROPERTY_NAME</i> will be assigned a relevant message.
     * </p>
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isValid()
    {
        boolean isValid = true;

        clearFieldProperties();
        for (DataField dataField : mFields)
        {
            if (! dataField.isValid())
                isValid = false;
        }

        return isValid;
    }

    /**
     * Creates a list of validation messages for fields that failed
     * the <code>isValid()</code> check.
     *
     * @return List of failed validation messages.
     */
    public ArrayList<String> getValidationMessages()
    {
        String propertyMessage;

        ArrayList<String> messageList = new ArrayList<String>();
        for (DataField dataField : mFields)
        {
            propertyMessage = (String) dataField.getProperty(Field.VALIDATION_PROPERTY_NAME);
            if (StringUtils.isNotEmpty(propertyMessage))
                messageList.add(String.format("%s: %s", dataField.getName(), propertyMessage));
        }

        return messageList;
    }

    /**
     * Will compare each field within the current bag against the fields within the
     * bag provided as a parameter.
     * <p>
     * <b>Note:</b> If a field is found to differ, then a property called
     * <i>Field.VALIDATION_FIELD_CHANGED</i> will be assigned a relevant message.
     * </p>
     *
     * @param aBag Bag of fields to compare.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isFieldValuesEqual(DataBag aBag)
    {
        DataField dataField;

        clearFieldProperties();
        boolean isEqual = true;
        for (DataField bagField : mFields)
        {
            dataField = aBag.getFieldByName(bagField.getName());
            if (dataField != null)
            {
                if (! dataField.isEqual(bagField))
                {
                    isEqual = false;
                    addProperty(Field.VALIDATION_FIELD_CHANGED, Field.VALIDATION_MESSAGE_FIELD_CHANGED);
                }
            }
        }

        return isEqual;
    }

    /**
     * Process the bag information (name, fields, features) through
     * the digital hash algorithm.
     *
     * @param aHash Digital hash instance.
     * @param anIsFeatureIncluded Should features be included.
     *
     * @throws IOException Triggered by hash algorithm.
     */
    public void processHash(DigitalHash aHash, boolean anIsFeatureIncluded)
        throws IOException
    {
        for (DataField bagField : mFields)
        {
            aHash.processBuffer(bagField.getName());
            aHash.processBuffer(Field.typeToString(bagField.getType()));
            aHash.processBuffer(bagField.getTitle());
            if (anIsFeatureIncluded)
            {
                for (Map.Entry<String, String> featureEntry : getFeatures().entrySet())
                {
                    aHash.processBuffer(featureEntry.getKey());
                    aHash.processBuffer(featureEntry.getValue());
                }
            }
            if (bagField.isMultiValue())
                aHash.processBuffer(bagField.collapse());
            else
                aHash.processBuffer(bagField.getValue());
        }
    }

    /**
     * Generates a unique hash string using the MD5 algorithm using
     * the bag field information.
     *
     * @param anIsFeatureIncluded Should feature name/values be included?
     *
     * @return Unique hash string.
     */
    public String generateUniqueHash(boolean anIsFeatureIncluded)
    {
        String hashId;

        DigitalHash digitalHash = new DigitalHash();
        try
        {
            processHash(digitalHash, anIsFeatureIncluded);
            hashId = digitalHash.getHashSequence();
        }
        catch (IOException e)
        {
            UUID uniqueId = UUID.randomUUID();
            hashId = uniqueId.toString();
        }

        return hashId;
    }

    /**
     * Add a unique feature to this bag.  A feature enhances the core
     * capability of the bag.  Standard features are listed below.
     * <ul>
     *     <li>Field.FEATURE_OPERATION_NAME</li>
     *     <li>Field.FEATURE_TABLESPACE_NAME</li>
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
     * Add a unique feature to this bag.  A feature enhances the core
     * capability of the bag.
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
     * Count of unique features assigned to this bag.
     *
     * @return Feature count.
     */
    public int featureCount()
    {
        return mFeatures.size();
    }

    /**
     * Returns the number of fields that match the feature name
     * parameter.
     *
     * @param aName Feature name.
     *
     * @return Matching count.
     */
    public int featureNameCount(String aName)
    {
        int nameCount = 0;

        for (DataField dataField : mFields)
        {
            if (StringUtils.isNotEmpty(dataField.getFeature(aName)))
                nameCount++;
        }

        return nameCount;
    }

    /**
     * Returns the number of fields that match the feature name
     * and value parameters.  The value is matched in a case
     * insensitive manner.
     *
     * @param aName Feature name.
     * @param aValue Feature value.
     *
     * @return Matching count.
     */
    public int featureNameValueCount(String aName, String aValue)
    {
        String featureValue;
        int nameValueCount = 0;

        for (DataField dataField : mFields)
        {
            featureValue = dataField.getFeature(aName);
            if (StringUtils.equalsIgnoreCase(featureValue, aValue))
                nameValueCount++;
        }

        return nameValueCount;
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
     * Assigns the hash map of features to the list.
     *
     * @param aFeatures Feature list.
     */
    public void setFeatures(HashMap<String, String> aFeatures)
    {
        if (aFeatures != null)
            mFeatures = new HashMap<String, String>(aFeatures);
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
     * Add an application defined property to the bag.
     * <p>
     * <b>Notes:</b>
     * </p>
     * <ul>
     *     <li>The goal of the DataBag is to strike a balance between
     *     providing enough properties to adequately model application
     *     related data without overloading it.</li>
     *     <li>This method offers a mechanism to capture additional
     *     (application specific) properties that may be needed.</li>
     *     <li>Properties added with this method are transient and
     *     will not be stored when saved.</li>
     * </ul>
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
     * Removes all application defined properties assigned to this bag.
     */
    public void clearBagProperties()
    {
        if (mProperties != null)
            mProperties.clear();
    }

    /**
     * Convenience method that removes all properties for each field
     * in the bag.
     */
    public void clearFieldProperties()
    {
        for (DataField dataField : mFields)
            dataField.clearProperties();
    }

    /**
     * Returns the property map instance managed by the bag or <i>null</i>
     * if empty.
     *
     * @return Hash map instance.
     */
    public HashMap<String, Object> getProperties()
    {
        return mProperties;
    }
}
