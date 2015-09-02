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

package com.nridge.ds.content.ds_content;

import com.nridge.core.app.mgr.AppMgr;
import com.nridge.core.base.field.Field;
import com.nridge.core.base.field.FieldRow;
import com.nridge.core.base.field.data.DataBag;
import com.nridge.core.base.field.data.DataTable;
import com.nridge.core.base.field.data.DataTextField;
import com.nridge.core.base.std.NSException;
import com.nridge.core.base.std.StrUtl;
import com.nridge.core.io.csv.DataTableCSV;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;

/**
 * The ContentType class is responsible for detecting matching
 * document types based on an application provided CSV file.
 * Refer to the <i>doctypes_default.csv</i> file for a reference
 * listing of types.
 */
public class ContentType
{
    private DataTable mTable;
    private final AppMgr mAppMgr;
    private String mCfgPropertyPrefix = Content.CFG_PROPERTY_PREFIX;

    /**
     * Constructor accepts an application manager parameter and initializes
     * the content type accordingly.
     *
     * @param anAppMgr Application manager instance.
     */
    public ContentType(final AppMgr anAppMgr)
    {
        mAppMgr = anAppMgr;
        mTable = new DataTable(schemaBag());
    }

    /**
     * Creates a <i>DataBag</i> containing a list of fields
     * representing a schema for this object.
     *
     * @return DataBag instance.
     */
    public DataBag schemaBag()
    {
        DataBag dataBag = new DataBag("Document Type");

        dataBag.add(new DataTextField("type_name", "Type Name"));
        dataBag.add(new DataTextField("file_extension", "File Extension"));
        dataBag.add(new DataTextField("mime_type", "MIME Type"));
        dataBag.add(new DataTextField("url_pattern", "URL Pattern"));
        dataBag.add(new DataTextField("icon_name", "Icon Name"));

        return dataBag;
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

        if (StringUtils.startsWith(aSuffix, "."))
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
     * Given a file name, return the type name that matches its
     * extension or <code>Content.CONTENT_TYPE_UNKNOWN</code>.
     *
     * @param aFileName File name to examine.
     *
     * @return Type name.
     */
    public String nameByFileExtension(String aFileName)
    {
        if (StringUtils.isNotEmpty(aFileName))
        {
            int extOffset = aFileName.lastIndexOf(StrUtl.CHAR_DOT);
            if (extOffset != -1)
            {
                String fileExtension = aFileName.substring(extOffset);
                FieldRow fieldRow = Field.firstFieldRow(mTable.findValueInsensitive("file_extension", Field.Operator.EQUAL, fileExtension));
                if (fieldRow != null)
                    return mTable.getValueByName(fieldRow, "type_name");
            }
        }

        return Content.CONTENT_TYPE_UNKNOWN;
    }

    /**
     * Given a URL, return the type name that matches its pattern
     * or <code>Content.CONTENT_TYPE_UNKNOWN</code>.
     *
     * @param aURL URL to examine.
     *
     * @return Type name.
     */
    public String nameByURL(String aURL)
    {
        if (StringUtils.isNotEmpty(aURL))
        {
            DataBag dataBag;
            String urlPattern;

            int rowCount = mTable.rowCount();
            for (int row = 0; row < rowCount; row++)
            {
                dataBag = mTable.getRowAsBag(row);
                urlPattern = dataBag.getValueAsString("url_pattern");
                if (StringUtils.isNotEmpty(urlPattern))
                {
                    if (StringUtils.contains(aURL, urlPattern))
                        return dataBag.getValueAsString("type_type");
                }
            }
        }

        return Content.CONTENT_TYPE_UNKNOWN;
    }

    /**
     * Given a file name, return the icon name that matches its
     * extension or <code>Content.CONTENT_TYPE_UNKNOWN</code>.
     *
     * @param aFileName File name to examine.
     *
     * @return Icon name.
     */
    public String iconByFileExtension(String aFileName)
    {
        if (StringUtils.isNotEmpty(aFileName))
        {
            int extOffset = aFileName.lastIndexOf(StrUtl.CHAR_DOT);
            if (extOffset != -1)
            {
                String fileExtension = aFileName.substring(extOffset);
                FieldRow fieldRow = Field.firstFieldRow(mTable.findValueInsensitive("file_extension", Field.Operator.EQUAL, fileExtension));
                if (fieldRow != null)
                    return mTable.getValueByName(fieldRow, "icon_name");
            }
        }

        return Content.CONTENT_TYPE_UNKNOWN;
    }

    /**
     * Given a type name, return the icon name that matches it
     * or <code>Content.CONTENT_TYPE_UNKNOWN</code>.
     *
     * @param aTypeName Type name to examine.
     *
     * @return Icon name.
     */
    public String iconByTypeName(String aTypeName)
    {
        if (StringUtils.isNotEmpty(aTypeName))
        {
            FieldRow fieldRow = Field.firstFieldRow(mTable.findValue("type_name", Field.Operator.EQUAL, aTypeName));
            if (fieldRow != null)
                return mTable.getValueByName(fieldRow, "icon_name");
        }

        return Content.CONTENT_TYPE_UNKNOWN;
    }

    /**
     * Given a type name, return the MIME type that matches it
     * or <code>Content.CONTENT_TYPE_UNKNOWN</code>.
     *
     * @param aTypeName Type name to examine.
     *
     * @return MIME type.
     */
    public String mimeByTypeName(String aTypeName)
    {
        if (StringUtils.isNotEmpty(aTypeName))
        {
            FieldRow fieldRow = Field.firstFieldRow(mTable.findValue("type_name", Field.Operator.EQUAL, aTypeName));
            if (fieldRow != null)
                return mTable.getValueByName(fieldRow, "mime_type");
        }

        return Content.CONTENT_TYPE_UNKNOWN;
    }

    /**
     * Given a MIME type, return the type name that matches it
     * or <code>Content.CONTENT_TYPE_UNKNOWN</code>.
     *
     * @param aMIMEType MIME type to examine.
     *
     * @return Type name.
     */
    public String nameByMIMEType(String aMIMEType)
    {
        if (StringUtils.isNotEmpty(aMIMEType))
        {
            FieldRow fieldRow = Field.firstFieldRow(mTable.findValue("mime_type", Field.Operator.EQUAL, aMIMEType));
            if (fieldRow != null)
                return mTable.getValueByName(fieldRow, "type_name");
        }

        return Content.CONTENT_TYPE_UNKNOWN;
    }

    /**
     * Given a MIME type, return the icon name that matches it
     * or <code>Content.CONTENT_TYPE_UNKNOWN</code>.
     *
     * @param aMIMEType MIME type to examine.
     *
     * @return Icon name.
     */
    public String iconByMIMEType(String aMIMEType)
    {
        if (StringUtils.isNotEmpty(aMIMEType))
        {
            FieldRow fieldRow = Field.firstFieldRow(mTable.findValue("mime_type", Field.Operator.EQUAL, aMIMEType));
            if (fieldRow != null)
                return mTable.getValueByName(fieldRow, "icon_name");
        }

        return Content.CONTENT_TYPE_UNKNOWN;
    }

    /**
     * Parses a CSV file identified by the path/file name parameter
     * and loads it into an internally managed <i>DataTable</i>.
     *
     * @param aPathFileName Absolute file name.
     * @param aWithHeaders If <i>true</i>, then column headers will be
     *                     recognized in the CSV file.
     *
     * @throws IOException I/O related exception.
     */
    public void load(String aPathFileName, boolean aWithHeaders)
        throws IOException
    {
        Logger appLogger = mAppMgr.getLogger(this, "load");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        DataTableCSV dataTableCSV = new DataTableCSV(mTable);
        dataTableCSV.load(aPathFileName, aWithHeaders);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Parses a CSV file identified by the path/file name parameter
     * and loads it into an internally managed <i>DataTable</i>.
     *
     * @throws IOException I/O related exception.
     * @throws NSException Missing property variable.
     */
    public void load()
        throws IOException, NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "load");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        String propertyName = "doc_type_file";
        String docTypeFileName = getCfgString(propertyName, Content.CONTENT_DOCTYPE_FILE_DEFAULT);
        if (StringUtils.isEmpty(docTypeFileName))
        {
            String msgStr = String.format("Content property '%s' is undefined.",
                                           mCfgPropertyPrefix + "." + propertyName);
            throw new NSException(msgStr);
        }
        String docTypePathFileName = String.format("%s%c%s", mAppMgr.getString(mAppMgr.APP_PROPERTY_CFG_PATH),
                                                   File.separatorChar, docTypeFileName);
        load(docTypePathFileName, true);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }
}
