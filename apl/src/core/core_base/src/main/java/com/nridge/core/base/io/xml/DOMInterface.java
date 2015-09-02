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

import org.w3c.dom.Element;
import org.xml.sax.SAXException;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;

/**
 * The DOMInterface provides a collection of methods that can save/load
 * an XML representation of a bag/table object.
 *
 * @since 1.0
 * @author Al Cole
 */
public interface DOMInterface
{
    /**
     * Saves the previous assigned bag/table (e.g. via constructor or set method)
     * to the print writer stream wrapped in a tag name specified in the parameter.
     *
     * @param aPW PrintWriter stream instance.
     * @param aTagName Tag name.
     * @param anIndentAmount Indentation count.
     *
     * @throws IOException I/O related exception.
     */
    public void save(PrintWriter aPW, String aTagName, int anIndentAmount)
        throws IOException;

    /**
     * Saves the previous assigned bag/table (e.g. via constructor or set method)
     * to the print writer stream specified as a parameter.
     *
     * @param aPW PrintWriter stream instance.
     *
     * @throws IOException I/O related exception.
     */
    public void save(PrintWriter aPW) throws IOException;

    /**
     * Saves the previous assigned bag/table (e.g. via constructor or set method)
     * to the path/file name specified as a parameter.
     *
     * @param aPathFileName Absolute file name.
     *
     * @throws IOException I/O related exception.
     */
    public void save(String aPathFileName) throws IOException;

    /**
     * Parses an XML DOM element and loads it into a bag/table.
     *
     * @param anElement DOM element.
     *
     * @throws IOException I/O related exception.
     */
    public void load(Element anElement) throws IOException;

    /**
     * Parses an XML DOM element and loads it into a bag/table.
     *
     * @param anIS Input stream.
     *
     * @throws IOException I/O related exception.
     * @throws javax.xml.parsers.ParserConfigurationException XML parser related exception.
     * @throws org.xml.sax.SAXException                       XML parser related exception.
     * @throws TransformerException                           XML parser related exception.
     */
    public void load(InputStream anIS)
        throws ParserConfigurationException, IOException, SAXException, TransformerException;

    /**
     * Parses an XML file identified by the path/file name parameter
     * and loads it into a bag/table.
     *
     * @param aPathFileName Absolute file name.
     *
     * @throws IOException I/O related exception.
     * @throws ParserConfigurationException XML parser related exception.
     * @throws SAXException XML parser related exception.
     */
    public void load(String aPathFileName)
        throws IOException, ParserConfigurationException, SAXException;
}
