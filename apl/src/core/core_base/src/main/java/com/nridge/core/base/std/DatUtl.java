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

package com.nridge.core.base.std;

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

/**
 * The DatUtl class provides static convenience methods for calculating date/time
 * conversions.
 * <p>
 * The Apache Commons has a number of good utility methods for date/time calculations.
 * http://commons.apache.org/lang/api-2.5/org/apache/commons/lang/time/DateUtils.html
 * </p>
 *
 * @author Al Cole
 * @version 1.0 Nov 27, 2014
 * @since 1.0
 */
public class DatUtl
{
// http://www.coderanch.com/t/410264/java/java/Julian-Gregorian-date-conversion
// http://users.zoominternet.net/~matto/Java/Julian%20Date%20Converter.htm

// Gregorian Calendar adopted Oct. 15, 1582 (2299161)

    public static int GREGORIANSTARTDATE = 15 + 31 * (10 + 12 * 1582);
    public static double HALFSECOND = 0.5;

    private DatUtl()
    {
    }

    /**
     * Returns the Julian day number that begins at noon of this day.
     * Positive year signifies A.D., negative year B.C.
     * Remember that the year after 1 B.C. was 1 A.D.
     * <code>
     * System.out.println("Julian date for May 23, 1968 : " + toJulian(new int[]{1968, 5, 23}));
     * </code>
     * <p>
     * Reference: Numerical Recipes in C, 2nd ed., Cambridge University Press 1992
     * </p>
     * @param aYmd Three value integer array (Year, Month, Day).
     * @return The calculated Julian Day value.
     */
    public static double toJulian(int[] aYmd)
    {
        int year = aYmd[0];
        int month = aYmd[1]; // jan=1, feb=2, ...
        int day = aYmd[2];
        int julianYear = year;
        if (year < 0) julianYear++;
        int julianMonth = month;
        if (month > 2)
        {
            julianMonth++;
        }
        else
        {
            julianYear--;
            julianMonth += 13;
        }

        double julian = (java.lang.Math.floor(365.25 * julianYear)
                + java.lang.Math.floor(30.6001 * julianMonth) + day + 1720995.0);
        if (day + 31 * (month + 12 * year) >= GREGORIANSTARTDATE)
        {
            // change over to Gregorian calendar
            int ja = (int) (0.01 * julianYear);
            julian += 2 - ja + (0.25 * ja);
        }
        return java.lang.Math.floor(julian);
    }

    /**
     * Returns the Julian day number that begins at noon of this day.
     * Positive year signifies A.D., negative year B.C.
     * Remember that the year after 1 B.C. was 1 A.D.
     * <code>
     * System.out.println("Julian date for May 23, 1968 : " + toJulian(new Date()));
     * </code>
     * <p>
     * Reference: Numerical Recipes in C, 2nd ed., Cambridge University Press 1992
     * </p>
     * @param aDate The date to convert.
     * @return The calculated Julian Day value.
     */
    public static double toJulian(Date aDate)
    {
        int[] yearMonthDay = new int[3];
        TimeZone timeZone = TimeZone.getDefault();
        Calendar calendarInstance = Calendar.getInstance(timeZone);
        calendarInstance.setTime(aDate);

        yearMonthDay[0] = calendarInstance.get(Calendar.YEAR);
        yearMonthDay[1] = calendarInstance.get(Calendar.MONTH) + 1;
        yearMonthDay[2] = calendarInstance.get(Calendar.DAY_OF_MONTH);

        return toJulian(yearMonthDay);
    }

    /**
     * Converts a Julian day to a calendar date.
     * <p>
     * Reference: Numerical Recipes in C, 2nd ed., Cambridge University Press 1992
     * </p>
     * @param aJulianDay a Julian Day value.
     * @return A three value integer array {Y, M, D}
     */
    public static int[] fromJulian(double aJulianDay)
    {
        int jalpha, ja, jb, jc, jd, je, year, month, day;
        ja = (int) aJulianDay;
        if (ja >= GREGORIANSTARTDATE)
        {
            jalpha = (int) (((ja - 1867216) - 0.25) / 36524.25);
            ja = ja + 1 + jalpha - jalpha / 4;
        }

        jb = ja + 1524;
        jc = (int) (6680.0 + ((jb - 2439870) - 122.1) / 365.25);
        jd = 365 * jc + jc / 4;
        je = (int) ((jb - jd) / 30.6001);
        day = jb - jd - (int) (30.6001 * je);
        month = je - 1;
        if (month > 12) month = month - 12;
        year = jc - 4715;
        if (month > 2) year--;
        if (year <= 0) year--;

        return new int[]{year, month, day};
    }
}
