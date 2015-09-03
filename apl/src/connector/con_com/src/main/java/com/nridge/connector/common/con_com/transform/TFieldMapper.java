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

import com.nridge.connector.common.con_com.Connector;
import com.nridge.core.app.mgr.AppMgr;
import com.nridge.core.base.doc.Document;
import com.nridge.core.base.doc.Relationship;
import com.nridge.core.base.field.data.DataBag;
import com.nridge.core.base.field.data.DataField;
import com.nridge.core.base.std.NSException;
import com.nridge.core.base.std.StrUtl;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.io.File;
import java.util.ArrayList;

/**
 * The TFieldMapper document transformer maps the fields
 * from source document to the returned destination
 * document based on an external property file.
 *
 * Note: The TFieldCollapse class made this one obsolete.
 *
 * @since 1.0
 * @author Al Cole
 */
public class TFieldMapper implements TransformInterface
{
    private final AppMgr mAppMgr;
    private PropertiesConfiguration mCfgProperties;
    private String mCfgPropertyPrefix = Connector.CFG_PROPERTY_PREFIX;

    /**
     * Default constructor that accepts an AppMgr instance for
     * configuration and logging purposes.
     *
     * @param anAppMgr Application manager instance.
     */
    public TFieldMapper(final AppMgr anAppMgr)
    {
        mAppMgr = anAppMgr;
    }

    /**
     * Default constructor that accepts an AppMgr instance for
     * configuration and logging purposes.
     *
     * @param anAppMgr Application manager instance.
     * @param aCfgPropertyPrefix Property prefix.
     */
    public TFieldMapper(final AppMgr anAppMgr, String aCfgPropertyPrefix)
    {
        mAppMgr = anAppMgr;
        mCfgPropertyPrefix = aCfgPropertyPrefix + ".transform";
    }

    /**
     * Convenience method that returns the value of a property using
     * the concatenation of the property prefix and suffix values.
     *
     * @param aSuffix Property name suffix.
     * @return Matching property value.
     */
    private String getCfgString(String aSuffix)
    {
        String propertyName;

        if (org.apache.commons.lang.StringUtils.startsWith(aSuffix, "."))
            propertyName = mCfgPropertyPrefix + aSuffix;
        else
            propertyName = mCfgPropertyPrefix + "." + aSuffix;

        return mAppMgr.getString(propertyName);
    }

    /**
     * Convenience method that returns the value of a property using
     * the concatenation of the property prefix and suffix values.
     * If the property is not found, then the default value parameter
     * will be returned.
     *
     * @param aSuffix Property name suffix.
     * @param aDefaultValue Default value.
     *
     * @return Matching property value or the default value.
     */
    private String getCfgString(String aSuffix, String aDefaultValue)
    {
        String propertyName;

        if (org.apache.commons.lang.StringUtils.startsWith(aSuffix, "."))
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
    private int getCfgInteger(String aSuffix, int aDefaultValue)
    {
        String propertyName;

        if (org.apache.commons.lang.StringUtils.startsWith(aSuffix, "."))
            propertyName = mCfgPropertyPrefix + aSuffix;
        else
            propertyName = mCfgPropertyPrefix + "." + aSuffix;

        return mAppMgr.getInt(propertyName, aDefaultValue);
    }

    /**
     * Returns <i>true</i> if the a property value evaluates to <i>true</i>.
     *
     * @param aSuffix Property name suffix.
     *
     * @return <i>true</i> or <i>false</i>
     */
    private boolean isCfgStringTrue(String aSuffix)
    {
        String propertyValue = getCfgString(aSuffix);
        return StrUtl.stringToBoolean(propertyValue);
    }

    /**
     * Validates that the transformation feature is properly configured
     * to run as part of the parent application pipeline.
     *
     * @throws NSException Indicates a configuration issue.
     */
    public void validate()
        throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "validate");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        String propertyName = "field_mapper_file";
        String fieldMappingFileName = getCfgString(propertyName);
        if (StringUtils.isEmpty(fieldMappingFileName))
            throw new NSException(String.format("Property '%s' is undefined.", propertyName));

        String fieldMappingPathFileName = String.format("%s%c%s", mAppMgr.getString(mAppMgr.APP_PROPERTY_CFG_PATH),
                                                        File.separatorChar, fieldMappingFileName);
        File mappingFile = new File(fieldMappingPathFileName);
        if (! mappingFile.exists())
            throw new NSException(String.format("%s: Does not exist.", fieldMappingPathFileName));

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    private void transform(DataBag aDstBag, DataBag aSrcBag)
    {
        DataField dstField;
        String mapFieldName;
        Logger appLogger = mAppMgr.getLogger(this, "transform");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if ((aDstBag != null) && (aSrcBag != null))
        {
            for (DataField srcField : aSrcBag.getFields())
            {
                dstField = new DataField(srcField);
                mapFieldName = mCfgProperties.getString(srcField.getName());
                if (StringUtils.isNotEmpty(mapFieldName))
                    dstField.setName(mapFieldName);
                aDstBag.add(dstField);
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    private void transform(Document aDocument)
    {
        Logger appLogger = mAppMgr.getLogger(this, "transform");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (aDocument != null)
        {
            DataBag srcBag = aDocument.getBag();
            DataBag dstBag = new DataBag(srcBag.getName(), srcBag.getTitle());
            transform(dstBag, srcBag);
            aDocument.setBag(dstBag);
            ArrayList<Relationship> relationshipList = aDocument.getRelationships();
            for (Relationship relationship : relationshipList)
            {
                for (Document document : relationship.getDocuments())
                    transform(document);
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Applies a transformation against the source document to produce
     * the returned destination document.  If the document has a field
     * containing content, then this method may update it as part of its
     * processing logic.
     *
     * @param aSrcDoc Source document instance.
     *
     * @return New destination DataBag instance.
     *
     * @throws NSException Indicates a data validation error condition.
     */
    public Document process(Document aSrcDoc)
        throws NSException
    {
        Document dstDoc;
        Logger appLogger = mAppMgr.getLogger(this, "process");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (aSrcDoc == null)
            throw new NSException("Source document is null.");

        if (mCfgProperties == null)
        {
            String fieldMappingFileName = getCfgString("field_mapper_file");
            if (StringUtils.isNotEmpty(fieldMappingFileName))
            {
                String fieldMappingPathFileName = String.format("%s%c%s", mAppMgr.getString(mAppMgr.APP_PROPERTY_CFG_PATH),
                                                                File.separatorChar, fieldMappingFileName);
                File mappingFile = new File(fieldMappingPathFileName);
                if (mappingFile.exists())
                {
                    try
                    {
                        mCfgProperties = new PropertiesConfiguration(fieldMappingPathFileName);
                    }
                    catch (ConfigurationException e)
                    {
                        mCfgProperties = null;
                        String msgStr = String.format("%s: %s", fieldMappingPathFileName, e.getMessage());
                        throw new NSException(msgStr);
                    }
                }
            }
        }

        dstDoc = new Document(aSrcDoc);
        if (mCfgProperties != null)
            transform(dstDoc);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return dstDoc;
    }
}
