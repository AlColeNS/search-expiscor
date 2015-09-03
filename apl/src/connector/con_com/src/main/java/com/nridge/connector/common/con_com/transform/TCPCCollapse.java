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
import com.nridge.core.base.field.Field;
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
 * The TCPCCollapse document transformer collapses first-level
 * relationships into the top-level document.
 * <p>Collapses the first level relationship fields into
 * the parent document.  Everything else in the hierarchy
 * is ignored (and thus lost in the transformation).</p>
 *
 * @since 1.0
 * @author Al Cole
 */
public class TCPCCollapse implements TransformInterface
{
    private final AppMgr mAppMgr;
    private PropertiesConfiguration mCfgProperties;
    private String mFieldNamePrefix = StringUtils.EMPTY;
    private String mCfgPropertyPrefix = Connector.CFG_PROPERTY_PREFIX;

    /**
     * Default constructor that accepts an AppMgr instance for
     * configuration and logging purposes.
     *
     * @param anAppMgr Application manager instance.
     */
    public TCPCCollapse(final AppMgr anAppMgr)
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
    public TCPCCollapse(final AppMgr anAppMgr, String aCfgPropertyPrefix)
    {
        mAppMgr = anAppMgr;
        mCfgPropertyPrefix = aCfgPropertyPrefix + ".extract";
        String propertyValue = getCfgString("field_name_prefix");
        if (StringUtils.isNotEmpty(propertyValue))
            mFieldNamePrefix = propertyValue;
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

        if (StringUtils.startsWith(aSuffix, "."))
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
    @Override
    public void validate()
        throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "validate");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        String propertyName = "collapse_file";
        String compositeFileName = getCfgString(propertyName);
        if (StringUtils.isEmpty(compositeFileName))
            throw new NSException(String.format("Property '%s' is undefined.", propertyName));

        String compositePathFileName = String.format("%s%c%s", mAppMgr.getString(mAppMgr.APP_PROPERTY_CFG_PATH),
                                                     File.separatorChar, compositeFileName);
        File parentChildFile = new File(compositePathFileName);
        if (! parentChildFile.exists())
            throw new NSException(String.format("%s: Does not exist.", compositePathFileName));

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    private DataBag collapse(DataBag aParentBag, DataBag aChildBag, String aChildDocType)
    {
        String fieldName;
        DataField collapseField;
        Logger appLogger = mAppMgr.getLogger(this, "collapse");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        String idFieldName = mFieldNamePrefix + "id";
        DataBag collapseBag = new DataBag(aParentBag);
        for (DataField childField : aChildBag.getFields())
        {
            fieldName = childField.getName();
            if ((! StringUtils.startsWith(fieldName, "nsd_")) &&
                (! StringUtils.equals(fieldName, idFieldName)))
            {
                fieldName = String.format("rel_%s_%s", Field.titleToName(aChildDocType),
                                          childField.getName());
                collapseField = collapseBag.getFieldByName(fieldName);
                if (collapseField == null)
                {
                    collapseField = new DataField(childField);
                    collapseField.setName(fieldName);
                    collapseField.setMultiValueFlag(true);
                    collapseBag.add(collapseField);
                }
                else
                {
                    if (! collapseField.isMultiValue())
                        collapseField.setMultiValueFlag(true);
                    collapseField.addValue(childField.getValue());
                }
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return collapseBag;
    }

    private boolean isMatch(String[] aRelPCArray, String aType)
    {
        for (String pcString : aRelPCArray)
        {
            if (StringUtils.equals(aType, pcString))
                return true;
        }

        return false;
    }

    private void transform(Document aDocument)
    {
        String relType;
        String[] propertyValueArray;
        DataBag relationshipBag, collapseBag;
        Logger appLogger = mAppMgr.getLogger(this, "transform");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (aDocument != null)
        {
            DataBag docBag = aDocument.getBag();
            String docType = aDocument.getType();
            if ((docBag != null) && (StringUtils.isNotEmpty(docType)))
            {
                collapseBag = null;
                String propertyValue = mCfgProperties.getString(docType);
                if (StringUtils.isNotEmpty(propertyValue))
                {
                    propertyValueArray = mCfgProperties.getStringArray(docType);
                    ArrayList<Relationship> parentRelationshipList = aDocument.getRelationships();
                    for (Relationship parentRelationship : parentRelationshipList)
                    {
                        relType = parentRelationship.getType();
                        if (isMatch(propertyValueArray, relType))
                        {
                            if (collapseBag == null)
                                collapseBag = docBag;
                            relationshipBag = parentRelationship.getBag();
                            collapseBag = collapse(collapseBag, relationshipBag, relType);
                        }
                    }
                }
                if (collapseBag != null)
                {
                    aDocument.setBag(collapseBag);
                    ArrayList<Relationship> relationshipList = new ArrayList<Relationship>();
                    aDocument.setRelationships(relationshipList);
                }
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Applies a transformation against the source document to produce
     * the returned destination document.
     *
     * @param aSrcDoc Source document instance.
     * @return New destination document instance.
     *
     * @throws NSException Indicates a data validation error condition.
     */
    @Override
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
            String compositeFileName = getCfgString("collapse_file");
            if (StringUtils.isNotEmpty(compositeFileName))
            {
                String compositePathFileName = String.format("%s%c%s", mAppMgr.getString(mAppMgr.APP_PROPERTY_CFG_PATH),
                                                             File.separatorChar, compositeFileName);
                File parentChildFile = new File(compositePathFileName);
                if (parentChildFile.exists())
                {
                    try
                    {
                        mCfgProperties = new PropertiesConfiguration();
                        mCfgProperties.setFileName(compositePathFileName);
                        mCfgProperties.setIOFactory(new WhitespaceIOFactory());
                        mCfgProperties.load();
                    }
                    catch (ConfigurationException e)
                    {
                        mCfgProperties = null;
                        String msgStr = String.format("%s: %s", compositePathFileName, e.getMessage());
                        throw new NSException(msgStr);
                    }
                }
            }
        }

// Clone our source document.

        dstDoc = new Document(aSrcDoc);

// Execute the transformation.

        if (mCfgProperties != null)
            transform(dstDoc);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return dstDoc;
    }
}
