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

import org.apache.commons.lang3.time.DateUtils;

/**
 * The Sleep class provides utility methods for pausing program
 * execution.
 * <p>
 * The Apache Commons has a number of good utility methods for this.
 *
 * http://commons.apache.org/proper/commons-lang/javadocs/api-release/index.html
 * </p>
 * @author Al Cole
 * @version 1.0 Jan 2, 2014
 * @since 1.0
 */
public class Sleep
{
    public static void forMilliseconds(long aMilliseconds)
    {
        try { Thread.sleep(aMilliseconds); } catch (InterruptedException ignored) {}
    }

    public static void forSeconds(int aSeconds)
    {
        forMilliseconds(aSeconds * DateUtils.MILLIS_PER_SECOND);
    }

    public static void forMinutes(int aMinutes)
    {
        forMilliseconds(aMinutes * DateUtils.MILLIS_PER_MINUTE);
    }

    public static void forHours(int aHours)
    {
        forMilliseconds(aHours * DateUtils.MILLIS_PER_HOUR);
    }

    public static void forDays(int aDays)
    {
        forMilliseconds(aDays * DateUtils.MILLIS_PER_DAY);
    }
}
