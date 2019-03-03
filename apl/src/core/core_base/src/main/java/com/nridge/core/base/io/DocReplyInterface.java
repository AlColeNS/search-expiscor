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

package com.nridge.core.base.io;

import com.nridge.core.base.doc.Document;
import com.nridge.core.base.field.data.DataField;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

/**
 * The DocReplyInterface provides a collection of methods that can save/load
 * document reply payloads.
 *
 * @since 1.0
 * @author Al Cole
 */
public interface DocReplyInterface
{
    /**
     * Assigns a reply field containing status and other desired features.
     *
     * @param aField Operation field.
     */
    public void setField(DataField aField);

    /**
     * Returns a reply field.
     *
     * @return Data field instance.
     */
    public DataField getField();

    /**
     * Assigns standard message features to the reply.
     *
     * @param aMessage Reply message.
     * @param aDetail Reply message detail.
     */
    public void setFeatures(String aMessage, String aDetail);

    /**
     * Assigns standard account features to the reply.
     *
     * @param aSessionId Session identifier.
     */
    public void setFeatures(String aSessionId);

    /**
     * Assigns the document list parameter to the internally managed list instance.
     *
     * @param aDocumentList A list of document instances.
     */
    public void setDocumentList(ArrayList<Document> aDocumentList);

    /**
     * Returns a reference to the internally managed list of document instances.
     *
     * @return List of document instances.
     */
    public ArrayList<Document> getDocumentList();

    /**
     * Saves the previous assigned document list (e.g. via constructor or set method)
     * to a string and returns it.
     *
     * @return String representation of the document.
     *
     * @throws java.io.IOException I/O related exception.
     */
    public String saveAsString() throws IOException;

    /**
     * Parses an input stream and loads it into an internally managed
     * document operation instance.
     *
     * @param anIS Input stream instance.
     *
     * @throws java.io.IOException I/O related exception.
     * @throws ParserConfigurationException Parsing exception.
     * @throws SAXException SAX exception.
     * @throws TransformerException Transformer exception.
     */
    public void load(InputStream anIS) throws ParserConfigurationException, IOException, SAXException, TransformerException;
}
