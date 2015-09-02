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

/**
 * A CellValue is responsible for managing one or more cell
 * values used by the {@link DataTable} class.
 *
 * @author Al Cole
 * @since 1.0
 */
public class CellValue extends FieldValue
{
    /**
     * Default constructor.
     */
    public CellValue()
    {
        super();
    }

    /**
     * Constructor clones an existing CellValue.
     *
     * @param aCellValue Cell value instance.
     */
    public CellValue(final CellValue aCellValue)
    {
        super(aCellValue);
    }
}
