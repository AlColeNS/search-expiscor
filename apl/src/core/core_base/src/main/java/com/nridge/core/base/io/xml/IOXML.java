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

package com.nridge.core.base.io.xml;

import com.nridge.core.base.std.StrUtl;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.PrintWriter;

/**
 * The IOXML class provides a collection of utility methods that aid
 * in the generation and consumption of an XML DOM hierarchy.
 *
 * @since 1.0
 * @author Al Cole
 */
public class IOXML
{
    /**
     * Generates one or more space characters for indentation.
     *
     * @param aPW Print writer output stream.
     * @param aSpaceCount Count of spaces to indent.
     */
    public static void indentLine(PrintWriter aPW, int aSpaceCount)
    {
        for (int i = 0; i < aSpaceCount; i++)
            aPW.append(StrUtl.CHAR_SPACE);
    }

    /**
     * Writes an XML tag attribute (name/value) while ensuring the characters
     * are properly escaped.
     *
     * @param aPW Print writer output stream.
     * @param aName Attribute name.
     * @param aValue Attribute value.
     *
     * @throws IOException I/O related exception.
     */
    public static void writeAttrNameValue(PrintWriter aPW, String aName, String aValue)
        throws IOException
    {
        if (StringUtils.isNotEmpty(aValue))
            aPW.printf(" %s=\"%s\"", StringEscapeUtils.escapeXml10(aName),
                    StringEscapeUtils.escapeXml10(aValue));
    }

    /**
     * Writes an XML tag attribute (name/value) while ensuring the characters
     * are properly escaped.
     *
     * @param aPW Print writer output stream.
     * @param aName Attribute name.
     * @param aValue Attribute value.
     *
     * @throws IOException I/O related exception.
     */
    public static void writeAttrNameValue(PrintWriter aPW, String aName, char aValue)
        throws IOException
    {
        aPW.printf(" %s=\"%c\"", StringEscapeUtils.escapeXml10(aName), aValue);
    }

    /**
     * Writes an XML tag attribute (name/value) while ensuring the characters
     * are properly escaped.
     *
     * @param aPW Print writer output stream.
     * @param aName Attribute name.
     * @param aValue Attribute value.
     *
     * @throws IOException I/O related exception.
     */
    public static void writeAttrNameValue(PrintWriter aPW, String aName, int aValue)
            throws IOException
    {
        aPW.printf(" %s=\"%d\"", StringEscapeUtils.escapeXml10(aName), aValue);
    }

    /**
     * Writes an XML tag attribute (name/value) while ensuring the characters
     * are properly escaped.
     *
     * @param aPW Print writer output stream.
     * @param aName Attribute name.
     * @param aValue Attribute value.
     *
     * @throws IOException I/O related exception.
     */
    public static void writeAttrNameValue(PrintWriter aPW, String aName, long aValue)
            throws IOException
    {
        aPW.printf(" %s=\"%d\"", StringEscapeUtils.escapeXml10(aName), aValue);
    }

    /**
     * Writes an XML tag attribute (name/value) while ensuring the characters
     * are properly escaped.
     *
     * @param aPW Print writer output stream.
     * @param aName Attribute name.
     * @param aValue Attribute value.
     *
     * @throws IOException I/O related exception.
     */
    public static void writeAttrNameValue(PrintWriter aPW, String aName, boolean aValue)
            throws IOException
    {
        aPW.printf(" %s=\"%s\"", StringEscapeUtils.escapeXml10(aName),
                StrUtl.booleanToString(aValue));
    }

    /**
     * Writes an XML node (name/value) while ensuring the characters are properly
     * escaped.
     *
     * @param aPW Print writer output stream.
     * @param anIndentAmount Count of spaces to indent.
     * @param aName Node name.
     * @param aValue Node value.
     * @throws IOException I/O related exception.
     */
    public static void writeNodeNameValue(PrintWriter aPW, int anIndentAmount, String aName, String aValue)
            throws IOException
    {
        if (StringUtils.isNotEmpty(aValue))
        {
            indentLine(aPW, anIndentAmount);
            aPW.printf("<%s>%s</%s>%n", aName, StringEscapeUtils.escapeXml10(aValue), aName);
        }
    }

    /**
     * Writes an XML node (name/value) while ensuring the characters are properly
     * escaped.
     *
     * @param aPW Print writer output stream.
     * @param anIndentAmount Count of spaces to indent.
     * @param aName Node name.
     * @param aValue Node value.
     * @throws IOException I/O related exception.
     */
    public static void writeNodeNameValue(PrintWriter aPW, int anIndentAmount, String aName, int aValue)
            throws IOException
    {
        if (aValue != 0)
        {
            indentLine(aPW, anIndentAmount);
            aPW.printf("<%s>%d</%s>%n", aName, aValue, aName);
        }
    }

    /**
     * Writes an XML node (name/value) while ensuring the characters are properly
     * escaped.
     *
     * @param aPW Print writer output stream.
     * @param anIndentAmount Count of spaces to indent.
     * @param aName Node name.
     * @param aValue Node value.
     * @throws IOException I/O related exception.
     */
    public static void writeNodeNameValue(PrintWriter aPW, int anIndentAmount, String aName, long aValue)
            throws IOException
    {
        if (aValue != 0L)
        {
            indentLine(aPW, anIndentAmount);
            aPW.printf("<%s>%d</%s>%n", aName, aValue, aName);
        }
    }

    /**
     * Writes an XML node (name/value) while ensuring the characters are properly
     * escaped.
     *
     * @param aPW Print writer output stream.
     * @param anIndentAmount Count of spaces to indent.
     * @param aName Node name.
     * @param aValue Node value.
     * @throws IOException I/O related exception.
     */
    public static void writeNodeNameValue(PrintWriter aPW, int anIndentAmount, String aName, boolean aValue)
            throws IOException
    {
        indentLine(aPW, anIndentAmount);
        aPW.printf("<%s>%s</%s>%n", aName, StrUtl.booleanToString(aValue), aName);
    }
}
