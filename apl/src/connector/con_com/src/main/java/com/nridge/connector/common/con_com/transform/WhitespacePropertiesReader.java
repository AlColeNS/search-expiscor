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

package com.nridge.connector.common.con_com.transform;

import com.nridge.core.base.std.StrUtl;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang3.StringUtils;

import java.io.Reader;

/**
 * Utility class to handle property names with spaces.
 */
public class WhitespacePropertiesReader extends PropertiesConfiguration.PropertiesReader
{
    public WhitespacePropertiesReader(Reader aReader, char aDelimiter)
    {
        super(aReader, aDelimiter);
    }

    /**
     * Special algorithm for parsing properties keys with whitespace. This
     * method is called for each non-comment line read from the properties
     * file.
     */
    @Override
    protected void parseProperty(String aLine)
    {

// Simply split the line at the first '=' character.

        if (StringUtils.contains(aLine, StrUtl.CHAR_EQUAL))
        {
            int offset = aLine.indexOf('=');
            if (offset > 0)
            {
                String propertyKey = aLine.substring(0, offset).trim();
                String propertyValue = aLine.substring(offset + 1).trim();

// Now store the key and the value of the property.

                initPropertyName(propertyKey);
                initPropertyValue(propertyValue);
            }
            else
                super.parseProperty(aLine);
        }
        else
            super.parseProperty(aLine);


    }
}
