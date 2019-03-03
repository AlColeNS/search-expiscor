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

package com.nridge.core.app.mail;

import com.nridge.core.app.mgr.AppMgr;
import com.nridge.core.base.std.NSException;
import com.nridge.core.base.std.StrUtl;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;

/**
 * The Mail class captures the constants, enumerated types
 * and utility methods for the Mail Manager package.
 *
 * @author Al Cole
 * @since 1.0
 */
public class Mail
{
    public static final String CFG_PROPERTY_PREFIX = "app.mail";

    public static final String STATUS_SUCCESS = "Success";
    public static final String STATUS_FAILURE = "Failure";

    public static final String MESSAGE_NONE = "None.";

    private Mail()
    {
    }

    /**
     * Convenience method that extracts a first name from an email address
     * formatted as 'first.last@company.com'.  The first name will have its
     * first letter capitalized.
     *
     * @param anEmailAddress Email address.
     *
     * @return Proper first name.
     */
    public static String extractFirstName(String anEmailAddress)
    {
        String firstName = StringUtils.EMPTY;

        if (StringUtils.isNotEmpty(anEmailAddress))
        {
            int offset = anEmailAddress.indexOf(StrUtl.CHAR_DOT);
            if (offset > 0)
                firstName = StrUtl.firstCharToUpper(anEmailAddress.substring(0, offset));
            else
            {
                offset = anEmailAddress.indexOf(StrUtl.CHAR_AT);
                if (offset > 0)
                    firstName = StrUtl.firstCharToUpper(anEmailAddress.substring(0, offset));
            }
        }

        return firstName;
    }

    /**
     * Convenience method that extracts a last name from an email address
     * formatted as 'first.last@company.com'.  The last name will have its
     * first letter capitalized.
     *
     * @param anEmailAddress Email address.
     *
     * @return Proper last name.
     */
    public static String extractLastName(String anEmailAddress)
    {
        String lastName = StringUtils.EMPTY;

        if (StringUtils.isNotEmpty(anEmailAddress))
        {
            int offset2 = anEmailAddress.indexOf(StrUtl.CHAR_AT);
            if (offset2 > 0)
            {
                int offset1 = anEmailAddress.indexOf(StrUtl.CHAR_DOT);
                if (offset1 > 0)
                    lastName = StrUtl.firstCharToUpper(anEmailAddress.substring(offset1+1, offset2));
                else
                    lastName = StrUtl.firstCharToUpper(anEmailAddress.substring(0, offset2));
            }
        }

        return lastName;
    }
}
