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

package com.nridge.ds.ds_common;

import com.nridge.core.app.mgr.AppMgr;
import com.nridge.core.base.doc.Document;
import com.nridge.core.base.ds.DSCriteria;
import com.nridge.core.base.ds.DSException;
import com.nridge.core.base.field.data.DataBag;
import com.nridge.core.base.io.xml.DataBagXML;
import com.nridge.core.base.std.StrUtl;
import org.apache.commons.lang3.StringUtils;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * A data source is an abstract type designed to offer standard methods
 * for accessing content sources. It is modeled after methods that support
 * Create/Read/Update/Delete (CRUD) operations.  A content source might be
 * a SolrDS or a ElasticSearchDS.
 * Operations supported include:
 * <ul>
 *     <li>Adding {@link com.nridge.core.base.doc.Document}</li>
 *     <li>Updating {@link com.nridge.core.base.doc.Document}</li>
 *     <li>Deleting {@link com.nridge.core.base.doc.Document}</li>
 *     <li>Fetching via {@link com.nridge.core.base.ds.DSCriteria}</li>
 * </ul>
 * <p>
 * Typically, a data source fetches values from an external content
 * source and stores the result set in a data table. This result
 * set is captured within the hierarchical document and returned
 * after fetching operations and made available via accessor methods.
 * Criteria objects can be used for fetch, update and delete operations.
 * Finally, the schema definition of the content can be externalized into
 * JSON and XML files.
 * </p>
 *
 * @since 1.0
 * @author Al Cole
 */
public abstract class DSDocument
{
    private final String DS_TYPE_NAME = "DSDocument";
    private final String DS_TITLE_DEFAULT = "Unknown Data Source Document";

    protected AppMgr mAppMgr;
    private boolean mIsDefined;
    protected Document mDocument;
    private String mCfgPropertyPrefix = StringUtils.EMPTY;
    protected transient HashMap<String, Object> mProperties;

    /**
     * Constructor accepts an application manager parameter and initializes
     * the data source accordingly.
     *
     * @param anAppMgr Application manager.
     */
    public DSDocument(AppMgr anAppMgr)
    {
        mAppMgr = anAppMgr;
        mDocument = new Document(DS_TYPE_NAME);
        mDocument.setTitle(DS_TITLE_DEFAULT);
        mProperties = new HashMap<String, Object>();
    }

    /**
     * Constructor accepts an application manager and name parameter and
     * initializes the daa source accordingly.
     *
     * @param anAppMgr Application manager.
     * @param aName Name of the data source.
     */
    public DSDocument(AppMgr anAppMgr, String aName)
    {
        mAppMgr = anAppMgr;
        mDocument = new Document(aName);
        mDocument.setTitle(DS_TITLE_DEFAULT);
        mProperties = new HashMap<String, Object>();
    }

    /**
     * Constructor accepts an application manager, name and title parameters
     * and initializes the data source accordingly.
     *
     * @param anAppMgr Application manager.
     * @param aName Name of the data source.
     * @param aTitle Title of the data source.
     */
    public DSDocument(AppMgr anAppMgr, String aName, String aTitle)
    {
        mAppMgr = anAppMgr;
        mDocument = new Document(aName);
        mDocument.setTitle(aTitle);
        mProperties = new HashMap<String, Object>();
    }

    /**
     * Constructor accepts a <i>DSTable</i> as an initialization parameter.
     * The data source will be used to assign the schema definition to the new
     * data source.
     *
     * @param anAppMgr Application manager.
     * @param aSrcDS Data source to clone.
     */
    public DSDocument(AppMgr anAppMgr, DSDocument aSrcDS)
    {
        if (aSrcDS != null)
        {
            mAppMgr = anAppMgr;
            setName(aSrcDS.getName());
            setTitle(aSrcDS.getTitle());
            mProperties = new HashMap<String, Object>();
            mDocument = new Document(aSrcDS.getDocument());
        }
    }

    /**
     * Returns a string summary representation of a data source.
     *
     * @return String summary representation of this data source.
     */
    @Override
    public String toString()
    {
        String idString;
        if (StringUtils.isNotEmpty(getTitle()))
            idString = DS_TYPE_NAME + " - " + getTitle();
        else
            idString = DS_TYPE_NAME + " - " + getName();
        if (mDocument != null)
            idString += " + " + mDocument.toString();

        return idString;
    }

    /**
     * Returns the name of the data source.
     *
     * @return Data source name.
     */
    public String getName()
    {
        return mDocument.getName();
    }

    /**
     * Assigns the name of the data source.
     *
     * @param aName Data source name.
     */
    public void setName(String aName)
    {
        mDocument.setName(aName);
    }

    /**
     * Returns the title of the data source.
     *
     * @return Data source title.
     */
    public String getTitle()
    {
        return mDocument.getTitle();
    }

    /**
     * Assigns the title of the data source.
     *
     * @param aTitle Data source title.
     */
    public void setTitle(String aTitle)
    {
        mDocument.setTitle(aTitle);
    }

    /**
     * Returns a {@link com.nridge.core.base.doc.Document} representation of the underlying
     * data source content.
     *
     * @return Document hierarchy of data source content.
     */
    public Document getDocument()
    {
        return mDocument;
    }

    /**
     * Returns the configuration property prefix string.
     *
     * @return Property prefix string.
     */
    public String getCfgPropertyPrefix()
    {
        return mCfgPropertyPrefix;
    }

    /**
     * Assigns the configuration property prefix to the document data source.
     *
     * @param aPropertyPrefix Property prefix.
     */
    public void setCfgPropertyPrefix(String aPropertyPrefix)
    {
        mCfgPropertyPrefix = aPropertyPrefix;
    }

    /**
     * Convenience method that returns the value of an application
     * manager configuration property using the concatenation of
     * the property prefix and suffix values.
     *
     * @param aSuffix Property name suffix.
     *
     * @return Matching property value.
     */
    public String getCfgString(String aSuffix)
    {
        String propertyName;

        if (StringUtils.startsWith(aSuffix, "."))
            propertyName = mCfgPropertyPrefix + aSuffix;
        else
            propertyName = mCfgPropertyPrefix + "." + aSuffix;

        return mAppMgr.getString(propertyName);
    }

    /**
     * Convenience method that returns the value of an application
     * manager configuration property using the concatenation of
     * the property prefix and suffix values.  If the property is
     * not found, then the default value parameter will be returned.
     *
     * @param aSuffix Property name suffix.
     * @param aDefaultValue Default value.
     *
     * @return Matching property value or the default value.
     */
    public String getCfgString(String aSuffix, String aDefaultValue)
    {
        String propertyName;

        if (StringUtils.startsWith(aSuffix, "."))
            propertyName = mCfgPropertyPrefix + aSuffix;
        else
            propertyName = mCfgPropertyPrefix + "." + aSuffix;

        return mAppMgr.getString(propertyName, aDefaultValue);
    }

    /**
     * Returns a typed value for the property name identified
     * or the default value (if unmatched).
     *
     * @param aSuffix Property name suffix.
     * @param aDefaultValue Default value to return if property
     *                      name is not matched.
     *
     * @return Value of the property.
     */
    public int getCfgInteger(String aSuffix, int aDefaultValue)
    {
        String propertyName;

        if (org.apache.commons.lang.StringUtils.startsWith(aSuffix, "."))
            propertyName = mCfgPropertyPrefix + aSuffix;
        else
            propertyName = mCfgPropertyPrefix + "." + aSuffix;

        return mAppMgr.getInt(propertyName, aDefaultValue);
    }

    /**
     * Returns <i>true</i> if the application manager configuration
     * property value evaluates to <i>true</i>.
     *
     * @param aSuffix Property name suffix.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isCfgStringTrue(String aSuffix)
    {
        String propertyValue = getCfgString(aSuffix);
        return StrUtl.stringToBoolean(propertyValue);
    }

    /**
     * Returns the file name (derived from the internal data source name).
     *
     * @return Data source schema definition file name.
     */
    public String createSchemaFileName()
    {
        return String.format("ds_schema_%s.xml", getName());
    }

    /**
     * Returns the path/file name derived from the base path
     * and the internal data source name.
     *
     * @param aPathName Path name where file should be read/written.
     *
     * @return Data source values path/file name.
     */
    public String createSchemaPathFileName(String aPathName)
    {
        return String.format("%s%c%s", aPathName, File.separatorChar,
                             createSchemaFileName());
    }

    /**
     * Returns the file name derived from the internal data source name.
     *
     * @return Data source values file name.
     */
    public String createSnapshotFileName()
    {
        return String.format("ds_snapshot_%s.xml", getName());
    }

    /**
     * Returns the path/file name derived from the path and the internal data source name.
     *
     * @param aPathName Path name where file should be read/written.
     *
     * @return Data source values path/file name.
     */
    public String createSnapshotPathFileName(String aPathName)
    {
        return String.format("%s%c%s", aPathName, File.separatorChar, createSnapshotFileName());
    }

    /**
     * Returns <i>true</i> if the data source has a definition or
     * <i>false</i> otherwise.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isDefined()
    {
        return mIsDefined;
    }

    /**
     * Identifies if the data source has been defined (e.g. a schema is
     * established).
     *
     * @param anIsDefined <i>true</i> or <i>false</i>
     */
    public void setDefinedFlag(boolean anIsDefined)
    {
        mIsDefined = anIsDefined;
    }

    /**
     * Returns the type name of the data source.
     *
     * @return Data source type.
     */
    public String getTypeName()
    {
        return DS_TYPE_NAME;
    }

    /**
     * Stores the schema definition of the underlying data source
     * (formatted in XML) to the file system. The name of the file
     * is derived from the data of the data source and the location
     * of the file is specified as a parameter.
     *
     * @param aPathName Path name where the file should be written.
     *
     * @throws java.io.IOException I/O related exception.
     */
    public void saveSchema(String aPathName)
        throws IOException
    {
        String pathFileName = createSchemaPathFileName(aPathName);
        DataBagXML dataBagXML = new DataBagXML(mDocument.getBag());
        dataBagXML.save(pathFileName);
    }

    /**
     * Loads the (XML formatted) schema file of a (formally saved)
     * data source definition into memory. The name of the file is
     * derived from the data of the data source and the location of
     * the file is specified as a parameter.
     *
     * @param aPathName Path name where the XML file should be
     *                  loaded from.
     *
     * @throws java.io.IOException                            I/O related exception.
     * @throws javax.xml.parsers.ParserConfigurationException XML parser related exception.
     * @throws org.xml.sax.SAXException                       XML parser related exception.
     */
    public void loadSchema(String aPathName)
        throws IOException, ParserConfigurationException, SAXException
    {
        String pathFileName = createSchemaPathFileName(aPathName);
        DataBagXML dataBagXML = new DataBagXML();
        dataBagXML.load(pathFileName);
        mDocument = new Document(mDocument.getType(), dataBagXML.getBag());
        setDefinedFlag(true);
    }

    /**
     * Assigns the data bag instance as the schema for the
     * document data source.
     *
     * @param aBag Data bag instance.
     */
    public void setSchema(DataBag aBag)
    {
        if (aBag != null)
        {
            DataBag schemaBag = new DataBag(aBag);
            schemaBag.setAssignedFlagAll(false);
            mDocument = new Document(mDocument.getType(), schemaBag);
            setDefinedFlag(true);
        }
    }

    /**
     * Return a data bag instance representing the schema for
     * the data source.
     *
     * @return Data bag instance.
     */
    public DataBag getSchema()
    {
        return mDocument.getBag();
    }

    /**
     * Stores the snapshot definition/values of the underlying data source
     * (formatted in XML) to the file system. The name of the file
     * is derived from the data of the data source and the location
     * of the file is specified as a parameter.
     *
     * @param aPathName Path name where the file should be written.
     *
     * @throws java.io.IOException I/O related exception.
     */
    public void saveSnapshot(String aPathName)
        throws IOException
    {
        String pathFileName = createSnapshotPathFileName(aPathName);
        DataBagXML dataBagXML = new DataBagXML(mDocument.getBag());
        dataBagXML.save(pathFileName);
    }

    /**
     * Loads the (XML formatted) snapshot file of a (formally saved)
     * data source snapshot definition/values into memory. The name of
     * the file is derived from the data of the data source and the
     * location of the file is specified as a parameter.
     *
     * @param aPathName Path name where the XML file should be
     *                  loaded from.
     *
     * @throws java.io.IOException                            I/O related exception.
     * @throws javax.xml.parsers.ParserConfigurationException XML parser related exception.
     * @throws org.xml.sax.SAXException                       XML parser related exception.
     */
    public void loadSnapshot(String aPathName)
        throws IOException, ParserConfigurationException, SAXException
    {
        String pathFileName = createSchemaPathFileName(aPathName);
        DataBagXML dataBagXML = new DataBagXML();
        dataBagXML.load(pathFileName);
        mDocument = new Document(mDocument.getType(), dataBagXML.getBag());
    }

    /**
     * Add an application defined property to the data source.
     * <p>
     * <b>Notes:</b>
     * </p>
     * <ul>
     *     <li>The goal of the data source is to strike a balance between
     *     providing enough properties to adequately model application
     *     related data without overloading it.</li>
     *     <li>This method offers a mechanism to capture additional
     *     (application specific) properties that may be needed.</li>
     *     <li>Properties added with this method are transient and
     *     will not be persisted when saved.</li>
     * </ul>
     *
     * @param aName Property name (duplicates are not supported).
     * @param anObject Instance of an object.
     */
    public void addProperty(String aName, Object anObject)
    {
        if ((aName != null) && (anObject != null))
            mProperties.put(aName, anObject);
    }

    /**
     * Returns the object associated with the property name or
     * <i>null</i> if the name could not be matched.
     *
     * @param aName Name of the property.
     * @return Instance of an object.
     */
    public Object getProperty(String aName)
    {
        return mProperties.get(aName);
    }

    /**
     * Removes all application defined properties assigned to
     * this data source.
     */
    public void clearProperties()
    {
        mProperties.clear();
    }

    /**
     * Calculates a count (using a wildcard criteria) of all the
     * rows stored in the content source and returns that value.
     *
     * @return Count of all rows in the content source.
     *
     * @throws com.nridge.core.base.ds.DSException Data source related exception.
     */
    public abstract int count() throws DSException;

    /**
     * Returns a count of rows that match the <i>DSCriteria</i> specified
     * in the parameter.
     *
     * @param aDSCriteria Data source criteria.
     *
     * @return Count of rows matching the data source criteria.
     *
     * @throws com.nridge.core.base.ds.DSException Data source related exception.
     */
    public abstract int count(DSCriteria aDSCriteria) throws DSException;

    /**
     * Returns a <i>Document</i> representation of all documents
     * fetched from the underlying content source (using a wildcard
     * criteria).
     * <p>
     * <b>Note:</b> Depending on the size of the content source
     * behind this data source, this method could consume large
     * amounts of heap memory.  Therefore, it should only be
     * used when the number of column and rows is known to be
     * small in size.
     * </p>
     *
     * @return Document hierarchy representing all documents in
     * the content source.
     *
     * @throws com.nridge.core.base.ds.DSException Data source related exception.
     */
    public abstract Document fetch()  throws DSException;

    /**
     * Returns a <i>Document</i> representation of the documents
     * that match the <i>DSCriteria</i> specified in the parameter.
     * <p>
     * <b>Note:</b> Depending on the size of the content source
     * behind this data source and the criteria specified, this
     * method could consume large amounts of heap memory.
     * Therefore, the developer is encouraged to use the alternative
     * method for fetch where an offset and limit parameter can be
     * specified.
     * </p>
     *
     * @param aDSCriteria Data source criteria.
     *
     * @return Document hierarchy representing all documents that
     * match the criteria in the content source.
     *
     * @throws com.nridge.core.base.ds.DSException Data source related exception.
     */
    public abstract Document fetch(DSCriteria aDSCriteria) throws DSException;

    /**
     * Returns a <i>Document</i> representation of the documents
     * that match the <i>DSCriteria</i> specified in the parameter.
     * In addition, this method offers a paging mechanism where the
     * starting offset and a fetch limit can be applied to each
     * content fetch query.
     *
     * @param aDSCriteria Data source criteria.
     * @param anOffset Starting offset into the matching content rows.
     * @param aLimit Limit on the total number of rows to extract from
     *               the content source during this fetch operation.
     *
     * @return Document hierarchy representing all documents that
     * match the criteria in the content source. (based on the offset
     * and limit values).
     *
     * @throws com.nridge.core.base.ds.DSException Data source related exception.
     */
    public abstract Document fetch(DSCriteria aDSCriteria, int anOffset, int aLimit) throws DSException;

    /**
     * Adds the field values captured in the <i>Document</i> to
     * the content source.  The fields must be derived from the
     * same collection defined in the schema definition.
     * <p>
     * <b>Note:</b> Depending on the data source and its ability
     * to support transactions, you may need to apply
     * <code>commit()</code> and <code>rollback()</code>
     * logic around this method.
     * </p>
     *
     * @param aDocument Document to store.
     *
     * @throws com.nridge.core.base.ds.DSException Data source related exception.
     */
    public abstract void add(Document aDocument) throws DSException;

    /**
     * Adds the field values captured in the array of <i>Document</i>
     * to the content source.  The fields must be derived from the
     * same collection defined in the schema definition.
     * <p>
     * <b>Note:</b> Depending on the data source and its ability
     * to support transactions, you may need to apply
     * <code>commit()</code> and <code>rollback()</code>
     * logic around this method.
     * </p>
     *
     * @param aDocuments An array of Documents to store.
     *
     * @throws com.nridge.core.base.ds.DSException Data source related exception.
     */
    public abstract void add(ArrayList<Document> aDocuments) throws DSException;

    /**
     * Updates the field values captured in the <i>Document</i>
     * within the content source.  The fields must be derived from the
     * same collection defined in the schema definition.
     * <p>
     * <b>Note:</b> The Document must designate a field as a primary
     * key and that value must be assigned prior to using this
     * method.  Also, depending on the data source and its ability
     * to support transactions, you may need to apply
     * <code>commit()</code> and <code>rollback()</code>
     * logic around this method.
     * </p>
     *
     * @param aDocument Document to update.
     *
     * @throws com.nridge.core.base.ds.DSException Data source related exception.
     */
    public abstract void update(Document aDocument) throws DSException;

    /**
     * Updates the field values captured in the array of
     * <i>Document</i> within the content source.  The fields must
     * be derived from the same collection defined in the schema
     * definition.
     * <p>
     * <b>Note:</b> The Document must designate a field as a primary
     * key and that value must be assigned prior to using this
     * method.  Also, depending on the data source and its ability
     * to support transactions, you may need to apply
     * <code>commit()</code> and <code>rollback()</code>
     * logic around this method.
     * </p>
     *
     * @param aDocuments An array of Documents to update.
     *
     * @throws com.nridge.core.base.ds.DSException Data source related exception.
     */
    public abstract void update(ArrayList<Document> aDocuments) throws DSException;

    /**
     * Deletes the document identified by the <i>Document</i> from
     * the content source.  The fields must be derived from the
     * same collection defined in the schema definition.
     * <p>
     * <b>Note:</b> The document must designate a field as a primary
     * key and that value must be assigned prior to using this
     * method. Also, depending on the data source and its ability
     * to support transactions, you may need to apply
     * <code>commit()</code> and <code>rollback()</code>
     * logic around this method.
     * </p>
     *
     * @param aDocument Document where the primary key field value is
     *                  assigned.
     *
     * @throws com.nridge.core.base.ds.DSException Data source related exception.
     */
    public abstract void delete(Document aDocument) throws DSException;

    /**
     * Deletes the documents identified by the array of <i>Document</i>
     * from the content source.  The fields must be derived from the
     * same collection defined in the schema definition.
     * <p>
     * <b>Note:</b> The bag must designate a field as a primary
     * key and that value must be assigned prior to using this
     * method.
     * </p>
     *
     * @param aDocuments An array of documents where the primary key
     *                   field value is assigned.
     *
     * @throws com.nridge.core.base.ds.DSException Data source related exception.
     */
    public abstract void delete(ArrayList<Document> aDocuments) throws DSException;

    /**
     * Commits any staged add/update/delete transactions to the content
     * source.
     * <p>
     * <b>Note:</b> This method does not perform any commit operation
     * (it is a no-op).  The concrete class must override this method
     * if the underlying content source supports transactions.
     * </p>
     *
     * @throws com.nridge.core.base.ds.DSException Data source related exception.
     */
    public void commit()
        throws DSException
    {
        throw new DSException("Commit is an unsupported method.");
    }

    /**
     * Commits any staged add/update/delete transactions to the content
     * source.
     * <p>
     * <b>Note:</b> This method does not perform any commit operation
     * (it is a no-op).  The concrete class must override this method
     * if the underlying content source supports transactions.
     * </p>
     *
     * @throws com.nridge.core.base.ds.DSException Data source related exception.
     */
    public void rollback()
        throws DSException
    {
        throw new DSException("Rollback is an unsupported method.");
    }

    /**
     * Shutdowns any resources associated with a session or connection
     * object related to the data source.
     *
     * @throws com.nridge.core.base.ds.DSException Data source related exception.
     */
    public void shutdown()
        throws DSException
    {
        throw new DSException("Shutdown is an unsupported method.");
    }

}
