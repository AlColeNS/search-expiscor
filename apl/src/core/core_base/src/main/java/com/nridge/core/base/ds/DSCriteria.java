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
import com.nridge.core.base.field.data.DataField;
import com.nridge.core.base.field.data.DataTable;
import com.nridge.core.base.field.data.DataTextField;
import com.nridge.core.base.std.StrUtl;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * A DSCriteria can be used to express a search criteria for a data source.
 * In its simplest form, it can capture one or more field names, logical operators
 * (equal, less-than, greater-than) and a field values.  In addition, it also
 * supports grouping these expressions with boolean operators such as AND and OR.
 *
 * @since 1.0
 * @author Al Cole
 */
public class DSCriteria
{
    private String mName = StringUtils.EMPTY;
    private HashMap<String, String> mFeatures;
    private ArrayList<DSCriterionEntry> mCriterionEntries;
    private transient HashMap<String, Object> mProperties;

    /**
     * Default constructor.
     */
    public DSCriteria()
    {
        mFeatures = new HashMap<String, String>();
        mCriterionEntries = new ArrayList<DSCriterionEntry>();
    }

    /**
     * Constructor accepts a name parameter and initializes the
     * Criteria accordingly.
     *
     * @param aName Name of the criteria.
     */
    public DSCriteria(String aName)
    {
        setName(aName);
        mFeatures = new HashMap<String, String>();
        mCriterionEntries = new ArrayList<DSCriterionEntry>();
    }

    /**
     * Constructor accepts a criterion parameter and initializes the
     * Criteria accordingly.
     *
     * @param aDSCriterion Criterion instance.
     */
    public DSCriteria(DSCriterion aDSCriterion)
    {
        mFeatures = new HashMap<String, String>();
        mCriterionEntries = new ArrayList<DSCriterionEntry>();
        add(aDSCriterion);
    }

    /**
     * Constructor that accepts a name and field map and initializes the
     * Criteria accordingly.
     *
     * @param aName Name of the criteria.
     * @param aCriteriaMap Map of fields to construct a criteria from.
     */
    @SuppressWarnings("unchecked")
    public DSCriteria(String aName, Map<String, Object> aCriteriaMap)
    {
        setName(aName);
        mFeatures = new HashMap<String, String>();
        mCriterionEntries = new ArrayList<DSCriterionEntry>();

        if (aCriteriaMap != null)
        {
            Object mapValue;
            String mapName, mapOperator;
            for (Map.Entry<String, Object> mapEntry : aCriteriaMap.entrySet())
            {
                String entryName = mapEntry.getKey();
                if (! entryName.equals("criteria"))
                    continue;

                Object entryObject = mapEntry.getValue();
                if (entryObject instanceof ArrayList)
                {
                    ArrayList<Object> arrayList = (ArrayList<Object>) entryObject;
                    for (Object arrayObject : arrayList)
                    {
                        HashMap<String,String> hashMap = (HashMap<String,String>) arrayObject;
                        mapValue = hashMap.get("value");
                        mapName = hashMap.get("fieldName");
                        mapOperator = hashMap.get("operator");
                        addSpecialEntry(mapName, mapOperator, mapValue);
                    }
                }
            }
        }
    }

    /**
     * Returns a list of criterion entries currently being managed
     * by the criteria.
     *
     * @return An array of criterion entries.
     */
    public ArrayList<DSCriterionEntry> getCriterionEntries()
    {
        return mCriterionEntries;
    }

    /**
     * Returns a string summary representation of a criteria.
     *
     * @return String summary representation of a criteria.
     */
    @Override
    public String toString()
    {
        String idName;

        if (StringUtils.isEmpty(mName))
            idName = "Criteria";
        else
            idName = mName;

        return String.format("%s [%d criterion entries]", idName, count());
    }

    private Field.Operator convertSpecialOperator(String anOperator)
    {
        if (anOperator.equals("greaterThan"))
            return Field.Operator.GREATER_THAN;
        else if (anOperator.equals("greaterOrEqual"))
            return Field.Operator.GREATER_THAN_EQUAL;
        else if (anOperator.equals("lessThan"))
            return Field.Operator.LESS_THAN;
        else if (anOperator.equals("lessOrEqual"))
            return Field.Operator.LESS_THAN_EQUAL;
        else if (anOperator.equals("between"))
            return Field.Operator.BETWEEN;
        else if (anOperator.equals("betweenInclusive"))
            return Field.Operator.BETWEEN_INCLUSIVE;
        else if ((anOperator.equals("contains")) || (anOperator.equals("iContains")))
            return Field.Operator.CONTAINS;
        else if ((anOperator.equals("startsWith")) || (anOperator.equals("iStartsWith")))
            return Field.Operator.STARTS_WITH;
        else if ((anOperator.equals("notStartsWith")) || (anOperator.equals("iNotStartsWith")))
            return Field.Operator.NOT_STARTS_WITH;
        else if ((anOperator.equals("endsWith")) || (anOperator.equals("iEndsWith")))
            return Field.Operator.ENDS_WITH;
        else if ((anOperator.equals("notEndsWith")) || (anOperator.equals("iNotEndsWith")))
            return Field.Operator.NOT_ENDS_WITH;
        else if ((anOperator.equals("notContains")) || (anOperator.equals("iNotContains")))
            return Field.Operator.ENDS_WITH;
        else if (anOperator.equals("notEqual"))
            return Field.Operator.NOT_EQUAL;
        else if (anOperator.equals("inSet"))
            return Field.Operator.IN;
        else
            return Field.Operator.EQUAL;
    }

    /**
     * Adds a Smart GWT representation of a criterion object to this
     * criteria.
     *
     * @param aFieldName Field name.
     * @param anOperator Logical field operator.
     * @param anObject Generic representation of the value.
     */
    @SuppressWarnings("unchecked")
    public void addSpecialEntry(String aFieldName, String anOperator, Object anObject)
    {
        if ((StringUtils.isNotEmpty(aFieldName)) && (StringUtils.isNotEmpty(anOperator)) &&
            (anObject != null))
        {
            String textValue;
            DSCriterion dsCriterion;

            int offset = aFieldName.indexOf(StrUtl.CHAR_COLON);
            if (offset > 0)
            {
                textValue = anObject.toString();
                String fieldName = aFieldName.substring(0, offset);
                Field.Operator fieldOperator = Field.Operator.valueOf(aFieldName.substring(offset + 1));
                switch (fieldOperator)
                {
                    case IN:
                    case NOT_IN:
                    case BETWEEN:
                    case NOT_BETWEEN:
                    case BETWEEN_INCLUSIVE:
                        ArrayList<String> fieldValues = StrUtl.expandToList(textValue, StrUtl.CHAR_PIPE);
                        dsCriterion = new DSCriterion(fieldName, fieldOperator, fieldValues);
                        break;
                    default:
                        dsCriterion = new DSCriterion(fieldName, fieldOperator, textValue);
                        break;
                }
            }
            else
            {
                if (anObject instanceof Integer)
                {
                    Integer integerValue = (Integer) anObject;
                    dsCriterion = new DSCriterion(aFieldName, convertSpecialOperator(anOperator), integerValue);
                }
                else if (anObject instanceof Long)
                {
                    Long longValue = (Long) anObject;
                    dsCriterion = new DSCriterion(aFieldName, convertSpecialOperator(anOperator), longValue);
                }
                else if (anObject instanceof Float)
                {
                    Float floatValue = (Float) anObject;
                    dsCriterion = new DSCriterion(aFieldName, convertSpecialOperator(anOperator), floatValue);
                }
                else if (anObject instanceof Double)
                {
                    Double doubleValue = (Double) anObject;
                    dsCriterion = new DSCriterion(aFieldName, convertSpecialOperator(anOperator), doubleValue);
                }
                else if (anObject instanceof Date)
                {
                    Date dateValue = (Date) anObject;
                    dsCriterion = new DSCriterion(aFieldName, convertSpecialOperator(anOperator), dateValue);
                }
                else if (anObject instanceof ArrayList)
                {
                    ArrayList<String> arrayList = (ArrayList<String>) anObject;
                    String[] fieldValues = StrUtl.convertToMulti(arrayList);
                    dsCriterion = new DSCriterion(aFieldName, convertSpecialOperator(anOperator), fieldValues);
                }
                else
                {
                    textValue = anObject.toString();
                    dsCriterion = new DSCriterion(aFieldName, convertSpecialOperator(anOperator), textValue);
                }
            }

            add(dsCriterion);
        }
    }

    private String criterionFieldValueName(int aValue)
    {
        return String.format("%s_%d", DS.CRITERIA_VALUE_FIELD_NAME, aValue);
    }

    /**
     * Returns a {@link DataTable} representation of this criteria.
     * Use this method as a convenient way to flatten the hierarchy
     * of criterion objects.
     *
     * @return Simple table representation of the criteria.
     */
    public DataTable toTable()
    {
        DSCriterion dsCriterion;
        int maxValueCount = maxCountOfValues();

        DataTable dataTable = new DataTable(mName);
        dataTable.add(new DataTextField(DS.CRITERIA_BOOLEAN_FIELD_NAME));
        dataTable.add(new DataTextField(DS.CRITERIA_ENTRY_TYPE_NAME));
        dataTable.add(new DataTextField(DS.CRITERIA_ENTRY_FIELD_NAME));
        dataTable.add(new DataTextField(DS.CRITERIA_OPERATOR_FIELD_NAME));
        for (int val = 0; val < maxValueCount; val++)
            dataTable.add(new DataTextField(criterionFieldValueName(val+1)));

        for (DSCriterionEntry ce : mCriterionEntries)
        {
            dsCriterion = ce.getCriterion();

            dataTable.newRow();
            dataTable.setValueByName(DS.CRITERIA_BOOLEAN_FIELD_NAME, ce.getBooleanOperator().name());
            dataTable.setValueByName(DS.CRITERIA_ENTRY_TYPE_NAME, Field.typeToString(dsCriterion.getType()));
            dataTable.setValueByName(DS.CRITERIA_ENTRY_FIELD_NAME, dsCriterion.getName());
            dataTable.setValueByName(DS.CRITERIA_OPERATOR_FIELD_NAME, dsCriterion.getLogicalOperator().name());
            int valueNumber = 1;
            if (dsCriterion.isMultiValue())
            {
                for (String fieldValue : dsCriterion.getValues())
                    dataTable.setValueByName(criterionFieldValueName(valueNumber++), fieldValue);
            }
            else
                dataTable.setValueByName(criterionFieldValueName(valueNumber), dsCriterion.getValue());
            dataTable.addRow();
        }

        return dataTable;
    }

    /**
     * Returns the name of the criteria.
     *
     * @return Criteria name.
     */
    public String getName()
    {
        return mName;
    }

    /**
     * Assigns the name of the criteria.
     *
     * @param aName Criteria name.
     */
    public void setName(String aName)
    {
        mName = aName;
    }

    /**
     * Adds a criterion entry to the criteria.
     *
     * @param aDSCriterion Criterion instance.
     */
    public void add(DSCriterion aDSCriterion)
    {
        mCriterionEntries.add(new DSCriterionEntry(aDSCriterion));
    }

    /**
     * Adds the criterion instance to the criteria using the logical
     * operator parameter.
     *
     * @param anOperator Logical operator.
     * @param aDSCriterion Criterion instance.
     */
    public void add(Field.Operator anOperator, DSCriterion aDSCriterion)
    {
        mCriterionEntries.add(new DSCriterionEntry(anOperator, aDSCriterion));
    }

    /**
     * Convenience method that first constructs a criterion from
     * the {@link DataField} parameter and then adds it to the
     * criteria.
     *
     * <b>Note:</b>The default field operator for the criterion
     * entry will be <i>EQUAL</i>.
     *
     * @param aField Simple field.
     */
    public void add(DataField aField)
    {
        mCriterionEntries.add(new DSCriterionEntry(Field.Operator.AND, new DSCriterion(aField, Field.Operator.EQUAL)));
    }

    /**
     * Convenience method that first constructs a criterion from
     * the parameters and then adds it to the criteria.
     *
     * <b>Note:</b>The default field operator for the criterion
     * entry will be <i>EQUAL</i>.
     *
     * @param aName Field name.
     * @param aValue Field value.
     */
    public void add(String aName, String aValue)
    {
        mCriterionEntries.add(new DSCriterionEntry(Field.Operator.AND, new DSCriterion(aName, Field.Operator.EQUAL, aValue)));
    }

    /**
     * Convenience method that first constructs a criterion from
     * the {@link DataField} parameter and then adds it to the
     * criteria.
     *
     * @param aField Simple field.
     * @param anOperator Field operator.
     */
    public void add(DataField aField, Field.Operator anOperator)
    {
        mCriterionEntries.add(new DSCriterionEntry(Field.Operator.AND, new DSCriterion(aField, anOperator)));
    }

    /**
     * Convenience method that first constructs a criterion from
     * the parameters and then adds it to the criteria.
     *
     * @param aName Field name.
     * @param anOperator Field operator.
     * @param aValue Field value.
     */
    public void add(String aName, Field.Operator anOperator, String aValue)
    {
        mCriterionEntries.add(new DSCriterionEntry(Field.Operator.AND, new DSCriterion(aName, anOperator, aValue)));
    }

    /**
     * Convenience method that first constructs a criterion from
     * the parameters and then adds it to the criteria.
     *
     * @param aName Field name.
     * @param anOperator Field operator.
     * @param aValues An array of values.
     */
    public void add(String aName, Field.Operator anOperator, String... aValues)
    {
        mCriterionEntries.add(new DSCriterionEntry(Field.Operator.AND, new DSCriterion(aName, anOperator, aValues)));
    }

    /**
     * Convenience method that first constructs a criterion from
     * the parameters and then adds it to the criteria.
     *
     * @param aName Field name.
     * @param anOperator Field operator.
     * @param aValue Field value.
     */
    public void add(String aName, Field.Operator anOperator, int aValue)
    {
        mCriterionEntries.add(new DSCriterionEntry(Field.Operator.AND, new DSCriterion(aName, anOperator, aValue)));
    }

    /**
     * Convenience method that first constructs a criterion from
     * the parameters and then adds it to the criteria.  This
     * method is useful for defining an array of numbers with
     * the <i>IN</i> operator.
     *
     * @param aName Field name.
     * @param anOperator Field operator.
     * @param aValues Field values.
     */
    public void add(String aName, Field.Operator anOperator, int... aValues)
    {
        mCriterionEntries.add(new DSCriterionEntry(Field.Operator.AND, new DSCriterion(aName, anOperator, aValues)));
    }

    /**
     * Convenience method that first constructs a criterion from
     * the parameters and then adds it to the criteria.
     *
     * @param aName Field name.
     * @param anOperator Field operator.
     * @param aValue Field value.
     */
    public void add(String aName, Field.Operator anOperator, long aValue)
    {
        mCriterionEntries.add(new DSCriterionEntry(Field.Operator.AND, new DSCriterion(aName, anOperator, aValue)));
    }

    /**
     * Convenience method that first constructs a criterion from
     * the parameters and then adds it to the criteria.
     *
     * @param aName Field name.
     * @param anOperator Field operator.
     * @param aValue Field value.
     */
    public void add(String aName, Field.Operator anOperator, float aValue)
    {
        mCriterionEntries.add(new DSCriterionEntry(Field.Operator.AND, new DSCriterion(aName, anOperator, aValue)));
    }

    /**
     * Convenience method that first constructs a criterion from
     * the parameters and then adds it to the criteria.
     *
     * @param aName Field name.
     * @param anOperator Field operator.
     * @param aValue Field value.
     */
    public void add(String aName, Field.Operator anOperator, double aValue)
    {
        mCriterionEntries.add(new DSCriterionEntry(Field.Operator.AND, new DSCriterion(aName, anOperator, aValue)));
    }

    /**
     * Convenience method that first constructs a criterion from
     * the parameters and then adds it to the criteria.
     *
     * @param aName Field name.
     * @param anOperator Field operator.
     * @param aValue Field value.
     */
    public void add(String aName, Field.Operator anOperator, Date aValue)
    {
        mCriterionEntries.add(new DSCriterionEntry(Field.Operator.AND, new DSCriterion(aName, anOperator, aValue)));
    }

    /**
     * Convenience method that first constructs a criterion from
     * the parameters and then adds it to the criteria. This
     * method is useful for defining an array of numbers with
     * the <i>BETWEEN</i> operator.
     *
     * @param aName Field name.
     * @param anOperator Field operator.
     * @param aValue1 First Date value.
     * @param aValue2 Second Date value.
     */
    public void add(String aName, Field.Operator anOperator, Date aValue1, Date aValue2)
    {
        mCriterionEntries.add(new DSCriterionEntry(Field.Operator.AND,
            new DSCriterion(aName, anOperator, aValue1, aValue2)));
    }

    /**
     * Convenience method that first constructs a criterion from
     * the parameters and then adds it to the criteria.
     *
     * @param aName      Field name.
     * @param anOperator Field operator.
     * @param aValue     Field value.
     */
    public void add(String aName, Field.Operator anOperator, boolean aValue)
    {
        mCriterionEntries.add(new DSCriterionEntry(Field.Operator.AND, new DSCriterion(aName, anOperator, aValue)));
    }

    /**
     * Adds the criterion entry to the criteria using the boolean
     * operator parameter.
     *
     * @param anOperator Boolean operator.
     * @param aCriterionEntry Criterion entry.
     */
    public void add(Field.Operator anOperator, DSCriterionEntry aCriterionEntry)
    {
        aCriterionEntry.setBooleanOperator(anOperator);
        mCriterionEntries.add(aCriterionEntry);
    }

    /**
     * Adds the criterion entry to the criteria using the boolean
     * operator parameter.
     *
     * <b>Note:</b>The default boolean operator for the criterion
     * entry will be <i>AND</i>.
     *
     * @param aCriterionEntry Criterion entry.
     */
    public void add(DSCriterionEntry aCriterionEntry)
    {
        add(Field.Operator.AND, aCriterionEntry);
    }

    /**
     * Scans the criteria for the first criterion entry that
     * matches the field name.  If matched, then the field
     * instance is returned.
     *
     * @param aName Field name to match.
     *
     * @return Field instance or <i>null</i>.
     */
    public DataField getFirstFieldByName(String aName)
    {
        if (StringUtils.isNotEmpty(aName))
        {
            for (DSCriterionEntry ce : mCriterionEntries)
            {
                if (ce.getName().equals(aName))
                    return ce.getField();
            }
        }

        return null;
    }

    /**
     * Returns <i>true</i> if the complete criteria is simple in
     * its nature.  A simple criteria is one where all of the
     * boolean operators are <i>AND</i> and all values are
     * single.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isSimple()
    {
        boolean isSimple = true;

        for (DSCriterionEntry ce : mCriterionEntries)
        {
            if (! ce.isSimple())
            {
                isSimple = false;
                break;
            }
        }
        return isSimple;
    }

    /**
     * Returns the maximum number of values any criterion might
     * have in the criteria.  If the criterion does not represent
     * a multi-value, then the count for that entry would be one.
     *
     * @return Maximum count of values.
     */
    public int maxCountOfValues()
    {
        int maxCount = 0;
        DSCriterion dsCriterion;

        for (DSCriterionEntry ce : mCriterionEntries)
        {
            dsCriterion = ce.getCriterion();
            maxCount = Math.max(maxCount, dsCriterion.count());
        }

        return maxCount;
    }

    /**
     * Returns <i>true</i> if the complete criteria is advanced
     * in its nature.  An advanced criteria is one where any of the
     * boolean operators are <i>OR</i> or that there are entries
     * with multi-values.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isAdvanced()
    {
        return !isSimple();
    }

    /**
     * Clears the collection of criterion entries.
     */
    public void reset()
    {
        mCriterionEntries.clear();
        mFeatures.clear();
    }

    /**
     * Returns the count of criterion entries in this criteria.
     *
     * @return Total criterion entries in this criteria.
     */
    public int count()
    {
        return mCriterionEntries.size();
    }

    /**
     * Add a unique feature to this criteria.  A feature enhances the core
     * capability of the criteria.  Standard features are listed below.
     * <ul>
     *     <li>Field.FEATURE_OPERATION_NAME</li>
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
     * Add a unique feature to this criteria.  A feature enhances the core
     * capability of the criteria.
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
     * Count of unique features assigned to this criteria.
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
     * Add an application defined property to the criteria.
     *
     * <b>Notes:</b>
     *
     * <ul>
     *     <li>The goal of the DSCriteria is to strike a balance between
     *     providing enough properties to adequately model application
     *     related data without overloading it.</li>
     *     <li>This method offers a mechanism to capture additional
     *     (application specific) properties that may be needed.</li>
     *     <li>Properties added with this method are transient and
     *     will not be persisted when saved.</li>
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
    public void clearProperties()
    {
        mProperties.clear();
    }

    /**
     * Returns the property map instance managed by the criteria or <i>null</i>
     * if empty.
     *
     * @return Hash map instance.
     */
    public HashMap<String, Object> getProperties()
    {
        return mProperties;
    }
}
