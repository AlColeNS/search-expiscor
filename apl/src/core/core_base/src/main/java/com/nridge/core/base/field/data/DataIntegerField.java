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

/**
 * The DataIntegerField extends <i>DataField</i> and
 * should be used when modeling an Integer object.
 * <p>
 * This field type should be used when interacting with DataSource
 * content sources.
 * </p>
 *
 * @author Al Cole
 * @since 1.0
 */
public class DataIntegerField extends DataField
{
    /**
     * Default constructor.
     */
    public DataIntegerField()
    {
        super(Field.Type.Integer);
    }

    /**
     * Constructor clones an existing field.
     *
     * @param aField Field instance.
     */
    public DataIntegerField(final DataIntegerField aField)
    {
        super(aField);
    }

    /**
     * Constructor accepts a name parameter to initialize the
     * field instance.
     *
     * @param aName Name of the field.
     */
    public DataIntegerField(String aName)
    {
        super(Field.Type.Integer, aName);
    }

    /**
     * Constructor accepts a name and title parameters
     * to initialize the field instance.
     *
     * @param aName Name of the field.
     * @param aTitle Title of the field.
     */
    public DataIntegerField(String aName, String aTitle)
    {
        super(Field.Type.Integer, aName, aTitle);
    }

    /**
     * Constructor accepts data type, name, title and value parameters
     * to initialize the field instance.
     *
     * @param aName Name of the field.
     * @param aTitle Title of the field.
     * @param aValue Value of the field.
     */
    public DataIntegerField(String aName, String aTitle, String aValue)
    {
        super(Field.Type.Integer, aName, aTitle, aValue);
    }

    /**
     * Constructor accepts data type, name, title and value parameters
     * to initialize the field instance.
     *
     * @param aName Name of the field.
     * @param aTitle Title of the field.
     * @param aValue Value of the field.
     */
    public DataIntegerField(String aName, String aTitle, int aValue)
    {
        super(aName, aTitle, aValue);
    }
}
