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

import org.apache.commons.lang3.StringUtils;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;

/**
 * The WebUtl class provides utility features to applications that need
 * to construct web URIs.
 *
 * @author Al Cole
 * @version 1.0 Jan 4, 2014
 * @since 1.0
 */
public class WebUtl
{
    public static String encodeValue(String aValue)
    {
        if (StringUtils.isEmpty(aValue))
            return StringUtils.EMPTY;

        int offset = aValue.indexOf('%');
        if (offset != -1)
            aValue = aValue.replace("%", "%25");
        offset = aValue.indexOf('|');
        if (offset != -1)
            aValue = aValue.replace("|", "%7C");
        offset = aValue.indexOf('~');
        if (offset != -1)
            aValue = aValue.replace("~", "%7E");
        offset = aValue.indexOf(';');
        if (offset != -1)
            aValue = aValue.replace(";", "%3B");
        offset = aValue.indexOf('/');
        if (offset != -1)
            aValue = aValue.replace("/", "%2F");
        offset = aValue.indexOf('?');
        if (offset != -1)
            aValue = aValue.replace("?", "%3F");
        offset = aValue.indexOf(':');
        if (offset != -1)
            aValue = aValue.replace(":", "%3A");
        offset = aValue.indexOf('&');
        if (offset != -1)
            aValue = aValue.replace("&", "%26");
        offset = aValue.indexOf('=');
        if (offset != -1)
            aValue = aValue.replace("=", "%3D");
        offset = aValue.indexOf('+');
        if (offset != -1)
            aValue = aValue.replace("+", "%2B");
        offset = aValue.indexOf('$');
        if (offset != -1)
            aValue = aValue.replace("$", "%24");
        offset = aValue.indexOf(',');
        if (offset != -1)
            aValue = aValue.replace(",", "%2C");
        offset = aValue.indexOf('#');
        if (offset != -1)
            aValue = aValue.replace("#", "%23");
        offset = aValue.indexOf('^');
        if (offset != -1)
            aValue = aValue.replace("^", "%5E");
        offset = aValue.indexOf('[');
        if (offset != -1)
            aValue = aValue.replace("[", "%5B");
        offset = aValue.indexOf(']');
        if (offset != -1)
            aValue = aValue.replace("]", "%5D");
        offset = aValue.indexOf('\"');
        if (offset != -1)
            aValue = aValue.replace("\"", "%22");
        offset = aValue.indexOf('\\');
        if (offset != -1)
            aValue = aValue.replace("\\", "%5C");
        offset = aValue.indexOf(' ');
        if (offset != -1)
            aValue = aValue.replace(" ", "+");

        return aValue;
    }

    /*
     * This class perform application/x-www-form-urlencoded-type encoding rather than
     * percent encoding, therefore replacing with + is a correct behaviour.
     */
    public static String urlEncodeValue(String aValue)
    {
        String encodedValue;

        try
        {
            encodedValue = URLEncoder.encode(aValue, StrUtl.CHARSET_UTF_8);
            int offset = encodedValue.indexOf(StrUtl.CHAR_PLUS);
            if (offset != -1)
                encodedValue = StringUtils.replace(encodedValue, "+", "%20");
        }
        catch (UnsupportedEncodingException e)
        {
            encodedValue = aValue;
        }

        return encodedValue;
    }

    public static String urlDecodeValue(String aValue)
    {
        String decodedValue;

        try
        {
            decodedValue = URLDecoder.decode(aValue, StrUtl.CHARSET_UTF_8);
        }
        catch (UnsupportedEncodingException e)
        {
            decodedValue = aValue;
        }

        return decodedValue;
    }

    public static String urlExtractFileName(String aURL)
    {
        if (StringUtils.isNotEmpty(aURL))
        {
            try
            {
                URL webURL = new URL(aURL);
                String fileName = webURL.getFile();
                if (fileName.indexOf('/') == -1)
                    return fileName;
            }
            catch (MalformedURLException ignored)
            {
            }

            int offset = aURL.lastIndexOf('/');
            if (offset != -1)
                return aURL.substring(offset+1);
        }

        return aURL;
    }
}
