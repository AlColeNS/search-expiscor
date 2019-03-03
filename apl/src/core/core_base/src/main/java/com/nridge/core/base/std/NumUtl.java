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

package com.nridge.core.base.std;

/**
 * The NumUtl class provides static convenience methods for determining the
 * type of value the number represents.
 * <p>
 * The Apache Commons has a number of good utility methods for numeric values.
 * http://commons.apache.org/lang/api-release/org/apache/commons/lang/math/package-summary.html
 * </p>
 *
 * @author Al Cole
 * @version 1.0 Jan 2, 2014
 * @since 1.0
 */
public class NumUtl
{
    /**
     * Determines if the numeric value is odd.
     * @param aValue Numeric value to evaluate.
     * @return <i>true</i> if the value is odd and <i>false</i> otherwise.
     */
    public static boolean isOdd(int aValue)
    {
        return ((aValue %2) == 1);
    }

    /**
     * Determines if the numeric value is even.
     * @param aValue  Numeric value to evaluate.
     * @return <i>true</i> if the value is even and <i>false</i> otherwise.
     */
    public static boolean isEven(int aValue)
    {
        return ((aValue %2) == 0);
    }
}

