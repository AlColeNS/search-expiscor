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
 * The BitUtl class provides utility methods for bitwise mask
 * operations.  The goal of the class was to improve code readability.
 * An EnumSet is a better way to model bits.  If you decide to serialize
 * the enum, then you look at the ordinal() method.
 *
 * @author Al Cole
 * @version 1.0 Jan 4, 2014
 * @since 1.0
 */
public class BitUtl
{
    /**
     * The <code>set</code> method uses the bitwise OR operator to assign
     * the individual bits of the bit mask parameter.
     * @param aValue Base value to apply bit mask to.
     * @param aBitMask Mask value that should be applied to the base.
     * @return The result from the OR operator.
     */
    public static int set(int aValue, int aBitMask)
    {
        return aValue |= aBitMask;
    }

    /**
     * The <code>isSet</code> method will return <i>true</i> if the bits
     * associated with the bit mask has been set.
     * @param aValue Base value to apply bit mask to.
     * @param aBitMask aBitMask Mask value that should be applied to the base.
     * @return <i>true</i> if the bits has been true, <i>false</i> otherwise.
     */
    public static boolean isSet(int aValue, int aBitMask)
    {
        return (aValue & aBitMask) == aBitMask;
    }

    /**
     * The <code>reset</code> method uses the bitwise XOR operator to assign
     * the individual bits of the bit mask parameter.
     * @param aValue Base value to apply bit mask to.
     * @param aBitMask Mask value that should be applied to the base.
     * @return The result value from the XOR operator.
     */
    public static int reset(int aValue, int aBitMask)
    {
        return aValue &= ~aBitMask;
    }
}
