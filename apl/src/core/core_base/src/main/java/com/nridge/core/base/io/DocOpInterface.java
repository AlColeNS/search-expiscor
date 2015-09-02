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

package com.nridge.core.base.io;

import com.nridge.core.base.doc.Document;
import com.nridge.core.base.ds.DSCriteria;
import com.nridge.core.base.field.data.DataField;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

/**
 * The DocOpInterface provides a collection of methods that can save/load
 * document and data source criteria.
 *
 * @since 1.0
 * @author Al Cole
 */
public interface DocOpInterface
{
    /**
     * Assigns an operation field containing a name and other desired features.
     *
     * @param aField Operation field.
     */
    public void setField(DataField aField);

    /**
     * Returns an operation field.
     *
     * @return Data field instance.
     */
    public DataField getField();

    /**
     * Assigns standard account features to the operation. Please
     * note that the parent application is responsible for encrypting
     * the account password prior to assigning it in this method.
     *
     * @param anAccountName Account name.
     * @param anAccountPassword Account password.
     */
    public void setAccountNamePassword(String anAccountName, String anAccountPassword);

    /**
     * Assigns standard account features to the operation. Please
     * note that the parent application is responsible for hashing
     * the account password prior to assigning it in this method.
     *
     * @param anAccountName Account name.
     * @param anAccountPasshash Account password.
     */
    public void setAccountNamePasshash(String anAccountName, String anAccountPasshash);

    /**
     * Assigns standard account features to the operation.
     *
     * @param aSessionId Session identifier.
     */
    public void setSessionId(String aSessionId);

    /**
     * Adds the document to the internally managed document list.
     *
     * @param aDocument Document instance.
     */
    public void addDocument(Document aDocument);

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
     * Assigns the data source criteria instance.  A criteria instance
     * is only assigned for <code>Doc.OPERATION_FETCH</code> names.
     *
     * @param aCriteria Data source criteria instance.
     */
    public void setCriteria(DSCriteria aCriteria);

    /**
     * Returns a reference to an internally managed criteria instance.
     *
     * @return Data source criteria instance if the operation was
     * <code>Doc.OPERATION_FETCH</code> or <i>null</i> otherwise.
     */
    public DSCriteria getCriteria();

    /**
     * Saves the previous assigned document list (e.g. via constructor or set method)
     * to a string and returns it.
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
     */
    public void load(InputStream anIS) throws ParserConfigurationException, IOException, SAXException, TransformerException;
}
