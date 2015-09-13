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

import com.nridge.core.base.field.data.DataTable;

import com.nridge.core.base.std.StrUtl;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

/**
 * A FieldRow is used by {@link DataTable} to model a row of
 * cells within their tables.
 *
 * @author Al Cole
 * @since 1.0
 */
public class FieldRow
{
    private CellValue[] mCells;
    private transient HashMap<String, Object> mProperties;

    /**
     * Constructor accepts a count of columns it must manage for
     * the newly created row.
     *
     * @param aColumnCount Number of columns in the row.
     */
    public FieldRow(int aColumnCount)
    {
        mCells = new CellValue[aColumnCount];
        for (int col = 0; col < aColumnCount; col++)
            mCells[col] = new CellValue();
    }

    /**
     * Constructor clones an existing FieldRow.
     *
     * @param aRow Source field row instance to clone.
     */
    public FieldRow(final FieldRow aRow)
    {
        if (aRow != null)
        {
            int colCount = aRow.count();
            mCells = new CellValue[colCount];
            for (int col = 0; col < colCount; col++)
                mCells[col] = new CellValue(aRow.getCellValue(col));
        }
    }

    /**
     * Returns a string representation of a CellValue.
     *
     * @return String summary representation of this CellValue.
     */
    @Override
    public String toString()
    {
        return String.format("Row [%d cols]", mCells.length);
    }

    /**
     * Returns the count of cells in the row.
     *
     * @return Count of cells in the row.
     */
    public int count()
    {
        return mCells.length;
    }

    public final CellValue getCellValue(int aColOffset)
    {
        if ((aColOffset >= 0) && (aColOffset < mCells.length))
            return mCells[aColOffset];
        else
            return null;
    }

    /**
     * Return a the cell value identified by the column offset.
     *
     * @param aColOffset Column offset in the row.
     *
     * @return Cell value.
     */
    public String getValue(int aColOffset)
    {
        if ((aColOffset >= 0) && (aColOffset < mCells.length))
            return mCells[aColOffset].getValue();
        else
            return StringUtils.EMPTY;
    }

    /**
     * Assigns the parameter value to the cell identified by the
     * column offset parameter.
     *
     * @param aColOffset Column offset in the row.
     * @param aValue A cell value that is formatted appropriately for
     *                the data type it represents.
     */
    public void setValue(int aColOffset, String aValue)
    {
        if ((aColOffset >= 0) && (aColOffset < mCells.length))
            mCells[aColOffset].setValue(aValue);
    }

    /**
     * Returns the count of cell values identified by the
     * column offset parameter.
     *
     * @param aColOffset Column offset in the row.
     *
     * @return List of cell values.
     */
    public int cellValueCount(int aColOffset)
    {
        if ((aColOffset >= 0) && (aColOffset < mCells.length))
            return mCells[aColOffset].count();
        else
            return 0;
    }

    /**
     * Return a read-only version of the cell value list.
     *
     * @param aColOffset Column offset in the row.
     *
     * @return List of cell values.
     */
    public final ArrayList<String> getValues(int aColOffset)
    {
        if ((aColOffset >= 0) && (aColOffset < mCells.length))
            return mCells[aColOffset].getValues();
        else
            return new ArrayList<String>();
    }

    /**
     * Assigns the cell value list parameter to the cell.
     *
     * @param aColOffset Column offset in the row.
     * @param aValues A value list that is formatted appropriately for
     *                the data type it represents.
     */
    public void setValues(int aColOffset, ArrayList<String> aValues)
    {
        if ((aColOffset >= 0) && (aColOffset < mCells.length))
            mCells[aColOffset].setValues(aValues);
    }

    /**
     * Collapses the list of values down to a single string.
     * Each value is separated by a delimiter character.
     *
     * @param aColOffset Column offset in the row.
     * @param aDelimiterChar Delimiter character.
     *
     * @return A collapsed string.
     */
    public String collapse(int aColOffset, char aDelimiterChar)
    {
        return StrUtl.collapseToSingle(getValues(aColOffset), aDelimiterChar);
    }

    /**
     * Collapses the list of values down to a single string.
     * Each value is separated by a default delimiter
     * character.
     *
     * @param aColOffset Column offset in the row.
     *
     * @return A collapsed string.
     */
    public String collapse(int aColOffset)
    {
        return collapse(aColOffset, StrUtl.CHAR_PIPE);
    }

    /**
     * Expand the value string into a list of individual values
     * using the delimiter character to identify each one.
     *
     * @param aColOffset Column offset in the row.
     * @param aValue One or more values separated by a
     *               delimiter character.
     * @param aDelimiterChar Delimiter character.
     */
    public void expand(int aColOffset, String aValue, char aDelimiterChar)
    {
        if (StringUtils.isNotEmpty(aValue))
            setValues(aColOffset, StrUtl.expandToList(aValue, aDelimiterChar));
    }

    /**
     * Expand the value string into a list of individual values
     * using the default delimiter character to identify each one.
     *
     * @param aColOffset Column offset in the row.
     * @param aValue One or more values separated by a
     *               delimiter character.
     */
    public void expand(int aColOffset, String aValue)
    {
        expand(aColOffset, aValue, StrUtl.CHAR_PIPE);
    }

    /**
     * Returns <i>true</i> if the field row parameter matches the
     * cell values of this field row.
     *
     * @param aRow Field row instance to base comparison on.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isEqual(FieldRow aRow)
    {
        if (aRow != null)
        {
            if (mCells.length == aRow.count())
            {
                int colCount = aRow.count();
                for (int col = 0; col < colCount; col++)
                {
                    if (! collapse(col).equals(aRow.collapse(col)))
                        return false;
                }
            }
            else
                return false;
        }
        else
            return false;

        return true;
    }

    /**
     * Indicates whether some other object is "equal to" this one.
     *
     * @param anObject Reference object with which to compare.
     * @return  {@code true} if this object is the same as the anObject
     *          argument; {@code false} otherwise.
     */
    @Override
    public boolean equals(Object anObject)
    {
        if (this == anObject)
            return true;

        if (anObject == null || getClass() != anObject.getClass())
            return false;

        FieldRow fieldRow = (FieldRow) anObject;

        return isEqual(fieldRow);
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
        return mCells != null ? Arrays.hashCode(mCells) : 0;
    }

    /**
     * Add an application defined property to the row.
     * <b>Notes:</b>
     * <ul>
     * <li>The goal of the Field is to strike a balance between
     * providing enough properties to adequately model application
     * related data without overloading it.</li>
     * <li>This method offers a mechanism to capture additional
     * (application specific) properties that may be needed.</li>
     * <li>Properties added with this method are transient and
     * will not be persisted when saved.</li>
     * </ul>
     *
     * @param aName    Property name (duplicates are not supported).
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
     *
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
        mProperties.clear();
    }
}
