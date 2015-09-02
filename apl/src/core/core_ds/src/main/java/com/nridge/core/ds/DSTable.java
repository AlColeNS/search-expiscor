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

package com.nridge.core.ds;

import com.nridge.core.app.mgr.AppMgr;
import com.nridge.core.base.ds.DSCriteria;
import com.nridge.core.base.ds.DSException;
import com.nridge.core.base.field.data.DataBag;
import com.nridge.core.base.field.data.DataTable;
import com.nridge.core.base.io.xml.DataBagXML;
import com.nridge.core.ds.io.xml.SmartGWTXML;
import com.nridge.core.io.csv.DataTableCSV;
import org.apache.commons.lang3.StringUtils;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.HashMap;
import java.io.File;

/**
 * A data source is an abstract type designed to offer standard methods
 * for accessing content sources. It is modeled after methods that support
 * Create/Read/Update/Delete (CRUD) operations.  A content source might be
 * a file system, RDBMSTable or a MemoryTable.
 * <p>
 * Operations supported include:
 * </p>
 * <ul>
 *     <li>Adding {@link DataBag}</li>
 *     <li>Updating {@link DataBag}</li>
 *     <li>Deleting {@link DataBag}</li>
 *     <li>Fetching via {@link DSCriteria}</li>
 * </ul>
 * <p>
 * Typically, a data source fetches values from an external content
 * source and stores the working set in a cache table. This cache
 * table is returned after fetching operations and made available
 * via accessor methods. Criteria objects can be used for fetch,
 * update and delete operations. Finally, the schema definition of the
 * content can be externalized into JSON and XML files.
 * </p>
 *
 * @since 1.0
 * @author Al Cole
 */
public abstract class DSTable
{
    private final String DS_TYPE_NAME = "DSTable";
    private final String DS_NAME_DEFAULT = "unknown";
    private final String DS_TITLE_DEFAULT = "Unknown Data Source Table";

    protected AppMgr mAppMgr;
    private boolean mIsDefined;
    protected DataTable mCacheTable;
    protected String mName = DS_NAME_DEFAULT;
    protected String mTitle = DS_TITLE_DEFAULT;
    protected transient HashMap<String, Object> mProperties;

    /**
     * Constructor accepts an application manager parameter and initializes
     * the data source accordingly.
     *
     * @param anAppMgr Application manager.
     */
    public DSTable(AppMgr anAppMgr)
    {
        mAppMgr = anAppMgr;
        mCacheTable = new DataTable();
        mCacheTable.setName("Cache");
    }

    /**
     * Constructor accepts an application manager and name parameter and
     * initializes the daa source accordingly.
     *
     * @param anAppMgr Application manager.
     * @param aName Name of the data source.
     */
    public DSTable(AppMgr anAppMgr, String aName)
    {
        mAppMgr = anAppMgr;
        mCacheTable = new DataTable(aName);
        setName(aName);
    }

    /**
     * Constructor accepts an application manager, name and title parameters
     * and initializes the data source accordingly.
     *
     * @param anAppMgr Application manager.
     * @param aName Name of the data source.
     * @param aTitle Title of the data source.
     */
    public DSTable(AppMgr anAppMgr, String aName, String aTitle)
    {
        mAppMgr = anAppMgr;
        mCacheTable = new DataTable(aName);
        setName(aName);
        setTitle(aTitle);
    }

    /**
     * Constructor accepts a <i>DSTable</i> as an initialization parameter.
     * The data source will be used to assign the schema definition to the new
     * data source.
     *
     * @param anAppMgr Application manager.
     * @param aSrcDS Data source to clone.
     */
    public DSTable(AppMgr anAppMgr, DSTable aSrcDS)
    {
        if (aSrcDS != null)
        {
            mAppMgr = anAppMgr;
            setName(aSrcDS.getName());
            setTitle(aSrcDS.getTitle());
            setCacheBag(new DataBag(aSrcDS.getCacheBag()));
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
        if (mCacheTable != null)
            idString += " + " + mCacheTable.toString();

        return idString;
    }

    /**
     * Returns the name of the data source.
     *
     * @return Data source name.
     */
    public String getName()
    {
        return mName;
    }

    /**
     * Assigns the name of the data source.
     *
     * @param aName Data source name.
     */
    public void setName(String aName)
    {
        mName = aName;
        mCacheTable.setName(aName);
    }

    /**
     * Returns the title of the data source.
     *
     * @return Data source title.
     */
    public String getTitle()
    {
        return mTitle;
    }

    /**
     * Assigns the title of the data source.
     *
     * @param aTitle Data source title.
     */
    public void setTitle(String aTitle)
    {
        mTitle = aTitle;
    }

    /**
     * Returns a <i>DataTable</i> representation of the cached rows
     * of previously fetched content.
     *
     * @return Table of cached content.
     */
    public DataTable getCacheTable()
    {
        return mCacheTable;
    }

    /**
     * Assigns a table of content to the internally managed cache.
     *
     * @param aCacheTable Cache table of content.
     */
    public void setCacheTable(DataTable aCacheTable)
    {
        if ((aCacheTable != null) && (aCacheTable.columnCount() > 0))
        {
            mCacheTable = aCacheTable;
            mIsDefined = true;
        }
    }

    /**
     * Assigns the fields contained within the bag to the columns of
     * the cache table.  The bag represents the internal schema of
     * the data source.
     *
     * @param aBag Data bag of fields.
     */
    public void setCacheBag(DataBag aBag)
    {
        if ((aBag != null) && (aBag.count() > 0))
        {
            mCacheTable = new DataTable(aBag);
            mIsDefined = true;
        }
    }

    /**
     * Returns the <i>DataBag</i> representation of the field columns
     * defined for the cache table.
     *
     * @return Data bag representation of the cache table columns.
     */
    public DataBag getCacheBag()
    {
        if (mCacheTable != null)
            return mCacheTable.getColumnBag();
        else
            return new DataBag(getName());
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
     * Returns the path/file name derived from the path name, value format type
     * (e.g. xml or csv) and the internal data source name.
     *
     * @param aPathName Path name where the file should be written.
     *
     * @return Data source values path/file name.
     */
    public String createSchemaPathFileName(String aPathName)
    {
        return String.format("%s%c%s", aPathName, File.separatorChar,
                             createSchemaFileName());
    }

    /**
     * Returns the Smart GWT path/file name (derived from the path name parameter
     * and the internal data source name).
     *
     * @param aPathName Path name where the file should be written.
     *
     * @return Smart GWT data source definition path/file name.
     */
    public String createSmartGWTPathFileName(String aPathName)
    {
        return String.format("%s%c%s.ds.xml", aPathName, File.separatorChar, getName());
    }

    /**
     * Returns the file name derived from the value format type (e.g. xml or csv) and
     * the internal data source name.
     *
     * @param aValueFormatType Value format type ("csv", "xml")
     *
     * @return Data source values file name.
     */
    public String createValuesFileName(String aValueFormatType)
    {
        return String.format("ds_values_%s.%s", getName(), aValueFormatType);
    }

    /**
     * Returns the path/file name derived from the path name, value format type
     * (e.g. xml or csv) and the internal data source name.
     *
     * @param aPathName Path name where the file should be written.
     * @param aValueFormatType Value format type ("csv", "xml")
     *
     * @return Data source values path/file name.
     */
    public String createValuePathFileName(String aPathName, String aValueFormatType)
    {
        return String.format("%s%c%s", aPathName, File.separatorChar,
                             createValuesFileName(aValueFormatType));
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
     * @throws IOException I/O related exception.
     * @throws DSException Data source related exception.
     */
    public void saveSchema(String aPathName)
        throws IOException, DSException
    {
        String pathFileName = createSchemaPathFileName(aPathName);
        DataBagXML dataBagXML = new DataBagXML(getCacheBag());
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
     * @throws IOException I/O related exception.
     * @throws DSException Data source related exception.
     * @throws ParserConfigurationException XML related exception.
     * @throws SAXException XML related exception.
     */
    public void loadSchema(String aPathName)
        throws IOException, DSException, ParserConfigurationException, SAXException
    {
        String pathFileName = createSchemaPathFileName(aPathName);
        DataBagXML dataBagXML = new DataBagXML();
        dataBagXML.load(pathFileName);
        setCacheBag(dataBagXML.getBag());
    }

    /**
     * Saves the content of the internally managed cache table
     * to an XML file.  The name of the file is derived from
     * the data of the data source and the location of the
     * file is specified as a parameter.
     *
     * @param aPathFileName Path/File name where the file should be written.
     *
     * @throws IOException I/O related exception.
     * @throws DSException Data source related exception.
     */
    public void saveCache(String aPathFileName)
        throws IOException, DSException
    {
        DataTableCSV dataTableCSV = new DataTableCSV(mCacheTable);
        dataTableCSV.save(aPathFileName, true);
    }

    /**
     * Loads the data source cache table (formatted in XML) into
     * memory. The name of the file is derived from the data of
     * the data source and the location of the file is specified
     * as a parameter.
     *
     * @param aPathFileName Path/File name where the XML file should be
     *                      loaded from.
     *
     * @throws IOException I/O related exception.
     * @throws DSException Data source related exception.
     */
    public void loadCache(String aPathFileName)
        throws IOException, DSException
    {
        DataTableCSV dataTableCSV = new DataTableCSV(mCacheTable);
        dataTableCSV.load(aPathFileName, true);
    }

// Override this if you have a specialized scenario

    /**
     * Saves the schema of the internally managed cache table
     * to a Smart GWT DS XML file.  The name of the file is
     * derived from the data of the data source and the location
     * of the file is specified as a parameter.
     * <p>
     * <b>Note:</b> Developers are encouraged to override this
     * method if you have a specialized storage scenario.
     * </p>
     *
     * @param aPathName Path name where the file should be written.
     *
     * @throws IOException I/O related exception.
     * @throws DSException Data source related exception.
     */
    public void saveSmartGWT(String aPathName)
        throws IOException, DSException
    {
        String pathFileName = createSmartGWTPathFileName(aPathName);
        SmartGWTXML smartGWTXML = new SmartGWTXML();
        smartGWTXML.save(pathFileName, this);
    }

    /**
     * Loads the (XML formatted) schema file of a Smart GWT DS into
     * this data source. The name of the file is derived from the
     * data of the data source and the location of the file is
     * specified as a parameter.
     * <p>
     * <b>Note:</b>&nbsp;Developers are encouraged to override this
     * method if you have a specialized loading scenario.
     * </p>
     *
     * @param aPathName Path name where the XML file should be
     *                  loaded from.
     *
     * @throws IOException I/O related exception.
     * @throws DSException Data source related exception.
     */
    public void loadSmartGWT(String aPathName)
        throws IOException, DSException
    {
        String pathFileName = createSmartGWTPathFileName(aPathName);
        SmartGWTXML smartGWTXML = new SmartGWTXML();
        smartGWTXML.save(pathFileName, this);
    }

    /**
     * Add an application defined property to the data source.
     * <p>
     * <b>Notes:</b>
     * </p>
     * <ul>
     *     <li>The goal of the DataSource is to strike a balance between
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
        if (mProperties == null)
            mProperties = new HashMap<String, Object>();
        mProperties.put(aName, anObject);
    }

    /**
     * Returns the object associated with the property name or
     * <i>null</i> if the name could not be matched.
     *
     * @param aName Name of the property.
     *
     * @return Instance of an object.
     */
    public Object getProperty(String aName)
    {
        if (mProperties == null)
            return null;
        else
            return mProperties.get(aName);
    }

    /**
     * Removes all application defined properties assigned to
     * this application manager.
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
     * @throws DSException Data source related exception.
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
     * @throws DSException Data source related exception.
     */
    public abstract int count(DSCriteria aDSCriteria) throws DSException;

    /**
     * Returns a <i>DataTable</i> representation of all rows
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
     * @return Table representing all rows in the content source.
     *
     * @throws DSException Data source related exception.
     */
    public abstract DataTable fetch() throws DSException;

    /**
     * Returns a <i>DataTable</i>  representation of the rows that
     * match the <i>DSCriteria</i> specified in the parameter.
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
     * @return Table representing all rows that match the criteria
     * in the content source.
     *
     * @throws DSException Data source related exception.
     */
    public abstract DataTable fetch(DSCriteria aDSCriteria) throws DSException;

    /**
     * Returns a <i>DataTable</i>  representation of the rows that
     * match the <i>DSCriteria</i> specified in the parameter.  In
     * addition, this method offers a paging mechanism where the
     * starting offset and a fetch limit can be applied to each
     * content fetch query.
     *
     * @param aDSCriteria Data source criteria.
     * @param anOffset Starting offset into the matching content rows.
     * @param aLimit Limit on the total number of rows to extract from
     *               the content source during this fetch operation.
     *
     * @return Table representing all rows that match the criteria
     * in the content source (based on the offset and limit values).
     *
     * @throws DSException Data source related exception.
     */
    public abstract DataTable fetch(DSCriteria aDSCriteria, int anOffset, int aLimit) throws DSException;

    /**
     * Adds the field values captured in the <i>DataBag</i> to
     * the content source.  The fields must be derived from the
     * same collection defined in the cache schema definition.
     *
     * @param aBag Bag of field values to store.
     *
     * @throws DSException Data source related exception.
     */
    public abstract void add(DataBag aBag) throws DSException;

    /**
     * Updates the field values captured in the <i>DataBag</i>
     * within the content source.  The fields must be derived from the
     * same collection defined in the cache schema definition.
     * <p>
     * <b>Note:</b> The bag must designate a field as a primary
     * key and that value must be assigned prior to using this
     * method.
     * </p>
     *
     * @param aBag Bag of field values to update.
     *
     * @throws DSException Data source related exception.
     */
    public abstract void update(DataBag aBag) throws DSException;

    /**
     * Deletes the record identified by the <i>DataBag</i> from
     * the content source.  The fields must be derived from the
     * same collection defined in the cache schema definition.
     * <p>
     * <b>Note:</b> The bag must designate a field as a primary
     * key and that value must be assigned prior to using this
     * method.
     * </p>
     *
     * @param aBag Bag where the primary key field value is
     *             assigned.
     *
     * @throws DSException Data source related exception.
     */
    public abstract void delete(DataBag aBag) throws DSException;
}
